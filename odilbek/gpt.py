# --- Import libraries ---
import pandas as pd
import json
import ast
from pandas import json_normalize
from uuid import uuid4
import re

# --- Load raw data ---
df_raw = pd.read_csv("../xlsx/rows.csv")

# --- Safe JSON parse ---
def safe_parse(text):
    try:
        return json.loads(text)
    except Exception:
        return {}

# --- Parse recipients ---
def parse_recipients(record):
    emails, names = [], []
    raw_recipients = record.get("recipients", [])
    if isinstance(raw_recipients, str):
        try:
            raw_recipients = ast.literal_eval(raw_recipients)
        except Exception:
            raw_recipients = []
    if not isinstance(raw_recipients, list):
        return emails, names
    for r in raw_recipients:
        if isinstance(r, dict):
            email = r.get("emailAddress", {}).get("address")
            name = r.get("emailAddress", {}).get("name")
            if email:
                emails.append(email.strip().lower())
                names.append(name)
    return emails, names

# --- Flatten raw_content ---
df_raw["parsed"] = df_raw["raw_content"].apply(safe_parse)
expanded = json_normalize(df_raw["parsed"])
expanded.columns = [f"raw_{col}" for col in expanded.columns]
df = pd.concat([df_raw.drop(columns=["raw_content", "parsed"]), expanded], axis=1)

# --- Dimensions: comm_type, subject, calendar ---
dim_comm_type = df[["comm_type"]].drop_duplicates().copy()
dim_comm_type["comm_type_id"] = dim_comm_type.index + 1

dim_subject = df[["subject"]].drop_duplicates().copy()
dim_subject["subject_id"] = dim_subject.index + 1

dim_calendar = df[["raw_calendar_id"]].dropna().drop_duplicates().copy()
dim_calendar["calendar_id"] = dim_calendar.index + 1

# --- Normalize emails ---
def normalize_emails(emails):
    if isinstance(emails, list):
        return emails
    elif isinstance(emails, str):
        try:
            parsed = ast.literal_eval(emails)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return [e.strip() for e in emails.split(",") if e.strip()]
    return [emails] if pd.notnull(emails) else []

# --- Meeting users extraction ---
user_rows = []
for _, row in df.iterrows():
    comm_id = row["id"]
    if row["comm_type"] != "meeting":
        continue

    for a in normalize_emails(row.get("raw_meeting_attendees")):
        if isinstance(a, dict):
            for email in normalize_emails(a.get("email")):
                user_rows.append({"comm_id": comm_id, "name": a.get("name"), "email": email, "location": a.get("location"), "displayName": a.get("displayName"), "phoneNumber": a.get("phoneNumber"), "isAttendee": True, "role": "attendee"})

    for email in normalize_emails(row.get("raw_participants")):
        for p in str(email).split(","):
            user_rows.append({"comm_id": comm_id, "name": None, "email": p.strip(), "location": None, "displayName": None, "phoneNumber": None, "isParticipant": True, "role": "participant"})

    for s in normalize_emails(row.get("raw_speakers")):
        name = s.get("name") if isinstance(s, dict) else s
        user_rows.append({"comm_id": comm_id, "name": name, "email": None, "location": None, "displayName": None, "phoneNumber": None, "isSpeaker": True, "role": "speaker"})

    for email in normalize_emails(row.get("raw_organizer_email")):
        user_rows.append({"comm_id": comm_id, "name": None, "email": email, "location": None, "displayName": None, "phoneNumber": None, "isOrganiser": True, "role": "organiser"})

df_users = pd.DataFrame(user_rows)

# --- Heuristic name-email match ---
def only_letters(s):
    return re.sub(r"[^a-zA-Z]", "", s or "")

named_rows = df_users[df_users["name"].notna() & df_users["email"].isna()].copy()
email_rows = df_users[df_users["email"].notna()].copy()

for i, name_row in named_rows.iterrows():
    raw_name = name_row["name"]
    if pd.isna(raw_name) or " " not in raw_name:
        continue
    original_name = raw_name.strip()
    cleaned_parts = re.sub(r"[^a-zA-Z\s]", "", original_name).strip().split()
    if len(cleaned_parts) != 2:
        continue
    variants = [(cleaned_parts[0], cleaned_parts[1]), (cleaned_parts[1], cleaned_parts[0])]
    for first, last in variants:
        first_clean = first.lower()
        last_clean = last.lower()
        first_initial = first_clean[0]
        combined = first_clean + last_clean
        for j, email_row in email_rows.iterrows():
            email = str(email_row["email"]).lower()
            if not pd.isna(email):
                email_clean = only_letters(email)
                if (last_clean in email_clean and email_clean.startswith(first_initial)) or (combined in email_clean):
                    df_users.loc[j, "name"] = original_name

# --- User ID assignment ---
final_users = []
person_map = {}
email_priority_users = {}

for _, row in df_users.iterrows():
    name = row["name"]
    email = row["email"]
    norm_name = name.lower().strip() if pd.notnull(name) else ""
    norm_email = email.lower().strip() if pd.notnull(email) else ""
    key = f"{norm_name}|{norm_email}" if norm_email else f"{norm_name}|"
    if norm_email:
        if norm_name and norm_name not in email_priority_users:
            email_priority_users[norm_name] = str(uuid4())
        elif not norm_name:
            email_priority_users[norm_email] = str(uuid4())
        assigned_id = email_priority_users.get(norm_name) or email_priority_users.get(norm_email)
        person_map[key] = assigned_id

for _, row in df_users.iterrows():
    name = row["name"]
    email = row["email"]
    norm_name = name.lower().strip() if pd.notnull(name) else ""
    norm_email = email.lower().strip() if pd.notnull(email) else ""
    key = f"{norm_name}|{norm_email}" if norm_email else f"{norm_name}|"
    user_id = person_map.get(key) or str(uuid4())
    person_map[key] = user_id
    final_users.append({"user_id": user_id, "name": name, "email": norm_email if norm_email else email, "location": row.get("location"), "displayName": row.get("displayName"), "phoneNumber": row.get("phoneNumber")})

dim_user_full = pd.DataFrame(final_users)
df_users = df_users.merge(dim_user_full[["name", "email", "user_id"]], on=["name", "email"], how="left")

# --- Parse emails ---
def parse_email_field(record, field):
    try:
        return record.get(field, {}).get("emailAddress", {}).get("address")
    except Exception:
        return None

def parse_email_name(record, field):
    try:
        return record.get(field, {}).get("emailAddress", {}).get("name")
    except Exception:
        return None

# --- Email user extraction ---
df_email = df[df["comm_type"] == "email"].copy()
df_email["parsed"] = df_email["raw_content"].apply(safe_parse)
df_email["sender_email"] = df_email["parsed"].apply(lambda x: parse_email_field(x, "from") or parse_email_field(x, "sender"))
df_email["sender_name"] = df_email["parsed"].apply(lambda x: parse_email_name(x, "from") or parse_email_name(x, "sender"))
df_email["content"] = df_email["parsed"].apply(lambda x: x.get("content"))
df_email[["recipient_emails", "recipient_names"]] = df_email["parsed"].apply(lambda x: pd.Series(parse_recipients(x)))

dim_email = df_email[["id", "raw_id", "raw_content", "sender_email", "sender_name", "content"]].rename(columns={"id": "comm_id"})

# --- Build email user bridge rows ---
email_user_rows = []
for _, row in df_email.iterrows():
    comm_id = row["id"]
    emails = row.get("recipient_emails") or []
    names = row.get("recipient_names") or []
    for email, name in zip(emails, names):
        email_user_rows.append({"comm_id": comm_id, "email": email, "name": name, "isParticipant": True, "isAttendee": False, "isOrganiser": False, "isSpeaker": False, "role": "recipients"})
    if pd.notna(row.get("sender_email")):
        email_user_rows.append({"comm_id": comm_id, "email": row["sender_email"], "name": row["sender_name"], "isOrganiser": True, "isParticipant": False, "isAttendee": False, "isSpeaker": False, "role": "sender"})

email_df = pd.DataFrame(email_user_rows)
for col in ["name", "email"]:
    email_df[col] = email_df[col].astype(str).fillna("").str.strip().str.lower()
email_df = email_df.merge(dim_user_full[["name", "email", "user_id"]], on=["name", "email"], how="left")
email_df["user_id"] = email_df["user_id"].fillna(email_df.apply(lambda _: str(uuid4()), axis=1))

for col in ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]:
    email_df[col] = email_df.get(col, False)

df_users = pd.concat([df_users, email_df], ignore_index=True)

# --- Finalize bridge_comm_user ---
df_users["role"] = df_users["role"].fillna("")
for col in ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]:
    df_users[col] = df_users[col].fillna(False)

bridge_comm_user = (
    df_users.groupby(["comm_id", "user_id", "role"], as_index=False)[
        ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]
    ].max()
)

print("✅ Bridge_comm_user table generated with sender and recipients roles.")
