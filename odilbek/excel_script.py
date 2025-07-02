import json
import re
from uuid import uuid4

import pandas as pd
from pandas import json_normalize


def try_parse(cell):
    if isinstance(cell, (dict, list)):
        return cell
    if not isinstance(cell, str):
        return {}

    cell = cell.strip()

    # Fix common encoding issues
    encoding_fixes = {
        'â€™': "'", 'â€œ': '"', 'â€': '"', 'â€“': '-', 'â€”': '-', 'â€¦': '...',
        'â€˜': "'", 'â€': '"', 'â€\\x9d': '"', 'вЂ™': "'", 'вЂ“': '-', 'вЂ”': '-', 'вЂ‹': '',
        '“': '"', '”': '"', '’': "'", '‘': "'", '–': '-', '—': '-', '…': '...'
    }
    for k, v in encoding_fixes.items():
        cell = cell.replace(k, v)

    # Remove invalid mailto/cid artifacts
    cell = re.sub(r'<mailto:[^>]+>', '', cell)
    cell = re.sub(r'\[cid:[^\]]+\]', '', cell)

    # Fix overly escaped newlines
    cell = cell.replace('\\r\\n', '\\n').replace('\r\n', '\\n').replace('\n', '\\n').replace('\r', '\\n')

    # Fix quotes if unbalanced
    quote_count = cell.count('"')
    if quote_count % 2 != 0:
        cell += '"'

    # Final fallback
    try:
        return json.loads(cell)
    except json.JSONDecodeError:
        return {}  # or optionally return {"_error": True, "raw": cell}


# Load and parse CSV
df_raw = pd.read_csv("raw_comm_email.csv")  # Adjust file path if needed
df_raw["parsed"] = df_raw["raw_content"].apply(try_parse)

# Expand raw_content
expanded = json_normalize(df_raw["parsed"])
expanded.columns = [f"raw_content.{col}" for col in expanded.columns]
df = pd.concat([df_raw.drop(columns=["raw_content", "parsed"]), expanded], axis=1)
df.columns

# Initialize schema rows
fact_rows = []
subject_rows = []
content_rows = []
participants_rows = []

for idx, row in df.iterrows():
    comm_id = str(uuid4())
    subject = row.get("subject")
    content = row.get("raw_content.content")
    comm_type = row.get("comm_type")

    ingested_at = row.get("ingested_at")
    processed_at = row.get("processed_at")
    is_processed = row.get("is_processed")
    processing_error = row.get("processing_error")

    source_id = row.get("source_id")

    fact_rows.append({
        "comm_id": comm_id,
        "ingested_at": row.get("ingested_at"),
        "processed_at": row.get("processed_at"),
        "is_processed": row.get("is_processed"),
        "processing_error": row.get("processing_error"),
        "subject": subject,
        "content": content,
        "comm_type": comm_type,
        "source_id": row.get("source_id")
    })

    subject_rows.append({"subject": subject})
    content_rows.append({"content": content})

    # Recipients
    recipients = row.get("raw_content.recipients", [])
    if isinstance(recipients, list):
        for rec in recipients:
            if isinstance(rec, dict):
                email_data = rec.get("emailAddress", {})
                participants_rows.append({
                    "comm_id": comm_id,
                    "name": email_data.get("name"),
                    "email": email_data.get("address"),
                    "role": "recipient"
                })

    # Sender
    sender = row.get("raw_content.sender", {}).get("emailAddress", {})
    if isinstance(sender, dict) and sender.get("address"):
        participants_rows.append({
            "comm_id": comm_id,
            "name": sender.get("name"),
            "email": sender.get("address"),
            "role": "sender"
        })

# Build dimensions
dim_subject = pd.DataFrame(subject_rows).drop_duplicates().reset_index(drop=True)
dim_subject["subject_id"] = dim_subject.index + 1

dim_content = pd.DataFrame(content_rows).drop_duplicates().reset_index(drop=True)
dim_content["content_id"] = dim_content.index + 1

# Build fact table
fact_df = pd.DataFrame(fact_rows)
fact_communication = fact_df.merge(dim_subject, on="subject", how="left")
fact_communication = fact_communication.merge(dim_content, on="content", how="left")
fact_communication = fact_communication[[
    "comm_id", "ingested_at", "processed_at", "is_processed", "processing_error",
    "subject_id", "content_id", "source_id"
]]

# Build dim_user and bridge table
df_participants = pd.DataFrame(participants_rows)
dim_user = df_participants.drop(columns=["comm_id", "role"]).drop_duplicates().reset_index(drop=True)
dim_user["user_id"] = dim_user.index + 1

bridge_comm_user = df_participants.merge(dim_user, on=["name", "email"])
bridge_comm_user = bridge_comm_user[["comm_id", "user_id", "role"]]

# Export to Excel
with pd.ExcelWriter("star_schema_output.xlsx", engine="xlsxwriter") as writer:
    fact_communication.to_excel(writer, sheet_name="fact_communication", index=False)
    dim_subject.to_excel(writer, sheet_name="dim_subject", index=False)
    dim_content.to_excel(writer, sheet_name="dim_content", index=False)
    dim_user.to_excel(writer, sheet_name="dim_user", index=False)
    bridge_comm_user.to_excel(writer, sheet_name="bridge_comm_user", index=False)
