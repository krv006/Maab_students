import pandas as pd
import json
import ast
import re
from uuid import uuid4


# --- Helper Functions ---
def safe_parse(text):
    try:
        return json.loads(text) if isinstance(text, str) else {}
    except json.JSONDecodeError:
        return {}


def parse_email_field(record, field):
    return record.get(field, {}).get("emailAddress", {}).get("address", "").lower().strip()


def parse_email_name(record, field):
    """Email nomini olish"""
    return record.get(field, {}).get("emailAddress", {}).get("name")


def parse_recipients(record):
    emails, names = [], []
    recipients = record.get("recipients", [])
    if isinstance(recipients, str):
        try:
            recipients = ast.literal_eval(recipients)
        except Exception:
            recipients = []
    if isinstance(recipients, list):
        for r in recipients:
            email = r.get("emailAddress", {}).get("address", "").lower().strip()
            name = r.get("emailAddress", {}).get("name")
            if email:
                emails.append(email)
                names.append(name)
    return emails, names


def normalize_emails(value):
    """Email ro'yxatini normalizatsiya qilish"""
    if isinstance(value, list):
        return [e.lower().strip() for e in value if e]
    if isinstance(value, str):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return [e.lower().strip() for e in parsed if e]
        except Exception:
            return [e.lower().strip() for e in value.split(",") if e.strip()]
    return [value.lower().strip()] if pd.notnull(value) else []


# --- Load Data ---
df_raw = pd.read_csv("../xlsx/raw_comm_rows (6).csv")
df_raw["parsed"] = df_raw["raw_content"].apply(safe_parse)

# --- Normalize JSON Columns ---
expanded = pd.json_normalize(df_raw["parsed"])
expanded.columns = [f"raw_{col}" for col in expanded.columns]
df = pd.concat([df_raw.drop(columns=["raw_content", "parsed"]), expanded], axis=1)

# --- Dimension Tables ---
dim_comm_type = df[["comm_type"]].drop_duplicates().assign(comm_type_id=lambda x: x.index + 1)
dim_subject = df[["subject"]].drop_duplicates().assign(subject_id=lambda x: x.index + 1)
dim_calendar = df[["raw_calendar_id"]].dropna().drop_duplicates().assign(calendar_id=lambda x: x.index + 1)
dim_audio = df[["raw_audio_url"]].dropna().drop_duplicates().assign(audio_id=lambda x: x.index + 1)
dim_video = df[["raw_video_url"]].dropna().drop_duplicates().assign(video_id=lambda x: x.index + 1)
dim_transcript = df[["raw_transcript_url"]].dropna().drop_duplicates().assign(transcript_id=lambda x: x.index + 1)

# --- Email Processing ---
df_email = df[df["comm_type"] == "email"].copy()
df_email["sender_email"] = df_email["parsed"].apply(
    lambda x: parse_email_field(x, "from") or parse_email_field(x, "sender"))
df_email["sender_name"] = df_email["parsed"].apply(
    lambda x: parse_email_name(x, "from") or parse_email_name(x, "sender"))
df_email["content"] = df_email["parsed"].apply(lambda x: x.get("content"))
df_email[["recipient_emails", "recipient_names"]] = df_email["parsed"].apply(lambda x: pd.Series(parse_recipients(x)))

dim_email = df_email[["id", "raw_id", "raw_content", "sender_email", "sender_name", "content"]].rename(
    columns={"id": "comm_id"})

# --- Build Email User Bridge ---
email_user_rows = []
for _, row in df_email.iterrows():
    comm_id = row["id"]
    emails = row.get("recipient_emails", []) or []
    names = row.get("recipient_names", []) or []

    # Recipients
    for email, name in zip(emails, names):
        email_user_rows.append({
            "comm_id": comm_id,
            "email": email,
            "name": name,
            "role": "recipient",
            "isParticipant": True,
            "isOrganiser": False,
            "isAttendee": False,
            "isSpeaker": False
        })

    # Sender
    if pd.notna(row.get("sender_email")):
        email_user_rows.append({
            "comm_id": comm_id,
            "email": row["sender_email"],
            "name": row["sender_name"],
            "role": "sender",
            "isParticipant": False,
            "isOrganiser": True,
            "isAttendee": False,
            "isSpeaker": False
        })

# --- Meeting Users Processing ---
user_rows = []
for _, row in df[df["comm_type"] == "meeting"].iterrows():
    comm_id = row["id"]

    for a in normalize_emails(row.get("raw_meeting_attendees", [])):
        if isinstance(a, dict):
            for email in normalize_emails(a.get("email", "")):
                user_rows.append({
                    "comm_id": comm_id,
                    "name": a.get("name"),
                    "email": email,
                    "role": "",
                    "isAttendee": True,
                    "isParticipant": False,
                    "isSpeaker": False,
                    "isOrganiser": False
                })

    for email in normalize_emails(row.get("raw_participants", [])):
        for p in str(email).split(","):
            user_rows.append({
                "comm_id": comm_id,
                "name": None,
                "email": p.strip(),
                "role": "",
                "isParticipant": True,
                "isAttendee": False,
                "isSpeaker": False,
                "isOrganiser": False
            })

    for s in normalize_emails(row.get("raw_speakers", [])):
        name = s.get("name") if isinstance(s, dict) else s
        user_rows.append({
            "comm_id": comm_id,
            "name": name,
            "email": None,
            "role": "",
            "isSpeaker": True,
            "isAttendee": False,
            "isParticipant": False,
            "isOrganiser": False
        })

    for email in normalize_emails(row.get("raw_organizer_email", [])):
        user_rows.append({
            "comm_id": comm_id,
            "name": None,
            "email": email,
            "role": "",
            "isOrganiser": True,
            "isAttendee": False,
            "isParticipant": False,
            "isSpeaker": False
        })

# --- Combine Users ---
df_users = pd.concat([pd.DataFrame(user_rows), pd.DataFrame(email_user_rows)], ignore_index=True)
for col in ["name", "email"]:
    df_users[col] = df_users[col].astype(str).fillna("").str.strip().str.lower()

# --- Assign User IDs ---
user_lookup = df_users[["name", "email"]].drop_duplicates().assign(
    user_id=lambda x: [str(uuid4()) for _ in range(len(x))])
df_users = df_users.merge(user_lookup, on=["name", "email"], how="left")

# Heuristic name-email matching
named_rows = df_users[df_users["name"] != ""].copy()
for i, row in named_rows.iterrows():
    if pd.isna(row["email"]) or row["email"] == "":
        name = row["name"].strip()
        if " " in name:
            parts = re.sub(r"[^a-zA-Z\s]", "", name).strip().split()
            if len(parts) == 2:
                first, last = parts
                first_clean = first.lower()
                last_clean = last.lower()
                first_initial = first_clean[0]
                combined = first_clean + last_clean
                email_match = df_users[df_users["email"] != ""]["email"].apply(
                    lambda e: last_clean in e and e.startswith(first_initial) or combined in e)
                if email_match.any():
                    df_users.loc[i, "email"] = df_users[df_users["email"] != ""].loc[email_match, "email"].iloc[0]

# Update dim_user
dim_user = df_users[["name", "email", "user_id"]].drop_duplicates(subset="user_id").reset_index(drop=True)
dim_user["name"] = dim_user.groupby("user_id")["name"].transform(
    lambda x: x[x != ""].iloc[0] if any(x != "") else x.iloc[0])

# --- Bridge Table ---
for col in ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]:
    df_users[col] = df_users[col].fillna(False)

bridge_comm_user = df_users.groupby(["comm_id", "user_id", "role"], as_index=False)[
    ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]
].max()

# --- Fact and Meeting Tables ---
fact_communication = df.merge(dim_comm_type, on="comm_type", how="left") \
    .merge(dim_subject, on="subject", how="left") \
    .merge(dim_calendar, on="raw_calendar_id", how="left") \
    .merge(dim_audio, on="raw_audio_url", how="left") \
    .merge(dim_video, on="raw_video_url", how="left") \
    .merge(dim_transcript, on="raw_transcript_url", how="left") \
    [[
        "id", "raw_id", "source_id", "comm_type_id", "subject_id", "calendar_id",
        "audio_id", "video_id", "transcript_id", "raw_dateString", "ingested_at",
        "processed_at", "is_processed", "raw_title", "raw_duration"
    ]].rename(columns={"id": "comm_id", "raw_dateString": "datetime_id"})

dim_meeting = df[df["comm_type"] == "meeting"].merge(fact_communication[["comm_id", "raw_id"]], on="raw_id", how="left") \
    .merge(dim_calendar, on="raw_calendar_id", how="left") \
    .merge(dim_audio, left_on="raw_audio_url", right_on="raw_audio_url", how="left") \
    .merge(dim_video, left_on="raw_video_url", right_on="raw_video_url", how="left") \
    .merge(dim_transcript, left_on="raw_transcript_url", right_on="raw_transcript_url", how="left") \
    [[
        "comm_id", "calendar_id", "audio_id", "video_id", "transcript_id",
        "raw_dateString", "raw_duration", "raw_host_email", "raw_id", "raw_title"
    ]].rename(columns={
    "raw_dateString": "dateString",
    "raw_duration": "duration",
    "raw_host_email": "host_email",
    "raw_title": "title"
})

# --- Export ---
with pd.ExcelWriter("star_schema_optimized.xlsx", engine="xlsxwriter") as writer:
    dim_comm_type.to_excel(writer, sheet_name="dim_comm_type", index=False)
    dim_subject.to_excel(writer, sheet_name="dim_subject", index=False)
    dim_user.to_excel(writer, sheet_name="dim_user", index=False)
    dim_calendar.to_excel(writer, sheet_name="dim_calendar", index=False)
    dim_audio.to_excel(writer, sheet_name="dim_audio", index=False)
    dim_video.to_excel(writer, sheet_name="dim_video", index=False)
    dim_transcript.to_excel(writer, sheet_name="dim_transcript", index=False)
    dim_meeting.to_excel(writer, sheet_name="dim_meeting", index=False)
    dim_email.to_excel(writer, sheet_name="dim_email", index=False)
    fact_communication.to_excel(writer, sheet_name="fact_communication", index=False)
    bridge_comm_user.to_excel(writer, sheet_name="bridge_comm_user", index=False)

print("Export complete.")