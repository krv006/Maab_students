import pandas as pd
import json
import ast
from pandas import json_normalize
from uuid import uuid4
import re

# --- Load Data ---
df_raw = pd.read_csv("rows.csv")

# --- Safe JSON parsing ---
def safe_parse(text):
    try:
        return json.loads(text)
    except Exception:
        return {}

df_raw["parsed"] = df_raw["raw_content"].apply(safe_parse)
expanded = json_normalize(df_raw["parsed"])
expanded.columns = [f"raw_{col}" for col in expanded.columns]
df = pd.concat([df_raw.drop(columns=["raw_content", "parsed"]), expanded], axis=1)

# --- DIMENSION TABLES ---
dim_comm_type = df[["comm_type"]].drop_duplicates().reset_index(drop=True)
dim_comm_type["comm_type_id"] = dim_comm_type.index + 1

dim_subject = df[["subject"]].drop_duplicates().reset_index(drop=True)
dim_subject["subject_id"] = dim_subject.index + 1

dim_calendar = df[["raw_calendar_id", "raw_dateString"]].dropna().drop_duplicates().reset_index(drop=True)
dim_calendar["calendar_id"] = dim_calendar.index + 1

dim_audio = df[["raw_audio_url"]].dropna().drop_duplicates().reset_index(drop=True)
dim_audio["audio_id"] = dim_audio.index + 1

dim_video = df[["raw_video_url"]].dropna().drop_duplicates().reset_index(drop=True)
dim_video["video_id"] = dim_video.index + 1

dim_transcript = df[["raw_transcript_url"]].dropna().drop_duplicates().reset_index(drop=True)
dim_transcript["transcript_id"] = dim_transcript.index + 1

# --- Helper: Normalize emails ---
def normalize_emails(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        return val
    try:
        parsed = ast.literal_eval(val)
        if isinstance(parsed, list):
            return parsed
    except:
        return [v.strip() for v in str(val).split(",") if v.strip()]
    return []

# --- Users extraction ---
user_rows = []
for _, row in df.iterrows():
    comm_id = row["id"]
    if row["comm_type"] != "meeting": continue

    for attendee in normalize_emails(row.get("raw_meeting_attendees")):
        email = attendee.get("email") if isinstance(attendee, dict) else attendee
        user_rows.append({"comm_id": comm_id, "email": email, "role": "attendee"})

    for email in normalize_emails(row.get("raw_participants")):
        user_rows.append({"comm_id": comm_id, "email": email, "role": "participant"})

    for speaker in normalize_emails(row.get("raw_speakers")):
        name = speaker.get("name") if isinstance(speaker, dict) else speaker
        user_rows.append({"comm_id": comm_id, "name": name, "role": "speaker"})

    for email in normalize_emails(row.get("raw_organizer_email")):
        user_rows.append({"comm_id": comm_id, "email": email, "role": "organiser"})

# --- Emails extraction ---
def parse_email_field(x, field):
    try:
        return x.get(field, {}).get("emailAddress", {}).get("address")
    except:
        return None

def parse_email_name(x, field):
    try:
        return x.get(field, {}).get("emailAddress", {}).get("name")
    except:
        return None

def parse_recipients(x):
    emails, names = [], []
    for r in x.get("recipients", []):
        if isinstance(r, dict):
            email = r.get("emailAddress", {}).get("address")
            name = r.get("emailAddress", {}).get("name")
            if email:
                emails.append(email)
                names.append(name)
    return emails, names

df_email = df[df["comm_type"] == "email"].copy()
df_email["parsed"] = df_email["raw_content"].apply(safe_parse)
df_email["sender_email"] = df_email["parsed"].apply(lambda x: parse_email_field(x, "from") or parse_email_field(x, "sender"))
df_email["sender_name"] = df_email["parsed"].apply(lambda x: parse_email_name(x, "from") or parse_email_name(x, "sender"))
df_email[["recipient_emails", "recipient_names"]] = df_email["parsed"].apply(lambda x: pd.Series(parse_recipients(x)))

email_user_rows = []
for _, row in df_email.iterrows():
    comm_id = row["id"]
    for email, name in zip(row.get("recipient_emails", []), row.get("recipient_names", [])):
        email_user_rows.append({"comm_id": comm_id, "email": email, "name": name, "role": "receiver"})
    if pd.notna(row.get("sender_email")):
        email_user_rows.append({"comm_id": comm_id, "email": row["sender_email"], "name": row["sender_name"], "role": "sender"})

# --- User DataFrame and ID assignment ---
df_users = pd.DataFrame(user_rows + email_user_rows)
df_users["email"] = df_users["email"].astype(str).str.lower().str.strip()
df_users["name"] = df_users["name"].astype(str).str.strip()
user_lookup = df_users[["name", "email"]].drop_duplicates().copy()
user_lookup["user_id"] = [str(uuid4()) for _ in range(len(user_lookup))]
df_users = df_users.merge(user_lookup, on=["name", "email"], how="left")
dim_user = user_lookup.copy()

# --- Ensure role exists and clear for meetings ---
if "role" not in df_users.columns:
    df_users["role"] = ""

meeting_ids = df[df["comm_type"] == "meeting"]["id"].unique()
df_users["role"] = df_users.apply(
    lambda row: "" if row["comm_id"] in meeting_ids else row["role"], axis=1
)
df_users["role"] = df_users["role"].fillna("").astype(str).str.strip()

# --- Bridge Table ---
for col in ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]:
    if col not in df_users.columns:
        df_users[col] = False
    else:
        df_users[col] = df_users[col].fillna(False)

bridge_comm_user = (
    df_users.groupby(["comm_id", "user_id", "role"], as_index=False)[
        ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]
    ].max()
)

# --- Fact Communication ---
fact_communication = df.merge(dim_comm_type, on="comm_type", how="left") \
    .merge(dim_subject, on="subject", how="left") \
    .merge(dim_calendar, on=["raw_calendar_id", "raw_dateString"], how="left") \
    .merge(dim_audio, on="raw_audio_url", how="left") \
    .merge(dim_video, on="raw_video_url", how="left") \
    .merge(dim_transcript, on="raw_transcript_url", how="left")

fact_communication = fact_communication[[
    "id", "source_id", "comm_type_id", "subject_id", "calendar_id",
    "audio_id", "video_id", "transcript_id", "raw_dateString", "ingested_at",
    "processed_at", "is_processed", "raw_title", "raw_duration"
]].rename(columns={"id": "comm_id", "raw_dateString": "datetime_id"})

# --- Export to Excel ---
with pd.ExcelWriter("1.xlsx", engine="xlsxwriter") as writer:
    dim_comm_type.to_excel(writer, sheet_name="dim_comm_type", index=False)
    dim_subject.to_excel(writer, sheet_name="dim_subject", index=False)
    dim_user.to_excel(writer, sheet_name="dim_user", index=False)
    dim_calendar.to_excel(writer, sheet_name="dim_calendar", index=False)
    dim_audio.to_excel(writer, sheet_name="dim_audio", index=False)
    dim_video.to_excel(writer, sheet_name="dim_video", index=False)
    dim_transcript.to_excel(writer, sheet_name="dim_transcript", index=False)
    fact_communication.to_excel(writer, sheet_name="fact_communication", index=False)
    bridge_comm_user.to_excel(writer, sheet_name="bridge_comm_user", index=False)

print("✅ Export complete: star_schema_optimized.xlsx")
