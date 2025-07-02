import ast
import json
import re
from uuid import uuid4

import pandas as pd
from pandas import json_normalize


# --- Helper Functions ---
def safe_parse(text):
    """JSONni xavfsiz parslash, xato bo'lsa bo'sh dict qaytaradi"""
    try:
        return json.loads(text) if isinstance(text, str) else {}
    except json.JSONDecodeError:
        return {}


def parse_email_field(record, field):
    """Email manzilini olish"""
    try:
        return record.get(field, {}).get("emailAddress", {}).get("address", "").lower().strip()
    except Exception:
        return None


def parse_email_name(record, field):
    """Email nomini olish"""
    try:
        return record.get(field, {}).get("emailAddress", {}).get("name", "")
    except Exception:
        return None


def parse_recipients(record):
    """Qabul qiluvchilar ro'yxatini olish"""
    emails, names = [], []
    recipients = record.get("recipients", [])
    if isinstance(recipients, str):
        try:
            recipients = ast.literal_eval(recipients)
        except Exception:
            recipients = []
    if isinstance(recipients, list):
        for r in recipients:
            if isinstance(r, dict):
                email = r.get("emailAddress", {}).get("address", "").lower().strip()
                name = r.get("emailAddress", {}).get("name", "")
                if email:
                    emails.append(email)
                    names.append(name if name else None)
    return emails, names


def normalize_emails(emails):
    """Email ro'yxatini normalizatsiya qilish"""
    if isinstance(emails, list):
        return [e.lower().strip() for e in emails if isinstance(e, str) and e.strip()]
    elif isinstance(emails, str):
        try:
            parsed = ast.literal_eval(emails)
            if isinstance(parsed, list):
                return [e.lower().strip() for e in parsed if isinstance(e, str) and e.strip()]
        except Exception:
            return [e.lower().strip() for e in emails.split(",") if e.strip()]
    return [emails.lower().strip()] if isinstance(emails, str) and emails.strip() else []


# --- Load Data ---
try:
    df_raw = pd.read_csv("../xlsx/raw_comm_rows (6).csv")
except FileNotFoundError:
    print("Xato: 'raw_comm_rows (6).csv' fayli topilmadi. Fayl yo'lini tekshiring.")
    exit()

# --- Check required columns ---
required_columns = ["raw_content", "comm_type", "id"]
missing_columns = [col for col in required_columns if col not in df_raw.columns]
if missing_columns:
    print(f"Xato: Quyidagi ustunlar faylda mavjud emas: {missing_columns}")
    exit()

# --- Create parsed column ---
df_raw["parsed"] = df_raw["raw_content"].apply(safe_parse)
expanded = json_normalize(df_raw["parsed"])
expanded.columns = [f"raw_{col}" for col in expanded.columns]
df = pd.concat([df_raw.drop(columns=["raw_content", "parsed"]), expanded], axis=1)

# --- Dimension Tables ---
dim_comm_type = df[["comm_type"]].drop_duplicates().copy()
dim_comm_type["comm_type_id"] = dim_comm_type.index + 1

dim_subject = df[["subject"]].drop_duplicates().copy()
dim_subject["subject_id"] = dim_subject.index + 1

dim_calendar = df[["raw_calendar_id"]].dropna().drop_duplicates().copy()
dim_calendar["calendar_id"] = dim_calendar.index + 1

dim_audio = df[["raw_audio_url"]].dropna().drop_duplicates().copy()
dim_audio["audio_id"] = dim_audio.index + 1

dim_video = df[["raw_video_url"]].dropna().drop_duplicates().copy()
dim_video["video_id"] = dim_video.index + 1

dim_transcript = df[["raw_transcript_url"]].dropna().drop_duplicates().copy()
dim_transcript["transcript_id"] = dim_transcript.index + 1

# --- Email Processing ---
df_email = df[df["comm_type"] == "email"].copy()
df_email["parsed"] = df_email["raw_content"].apply(safe_parse)
df_email["sender_email"] = df_email["parsed"].apply(
    lambda x: parse_email_field(x, "from") or parse_email_field(x, "sender"))
df_email["sender_name"] = df_email["parsed"].apply(
    lambda x: parse_email_name(x, "from") or parse_email_name(x, "sender"))
df_email["content"] = df_email["parsed"].apply(lambda x: x.get("content"))
df_email[["recipient_emails", "recipient_names"]] = df_email["parsed"].apply(lambda x: pd.Series(parse_recipients(x)))

# --- Debugging: Print sample data to verify emails ---
print("Sample sender_email:", df_email["sender_email"].head().tolist())
print("Sample recipient_emails:", df_email["recipient_emails"].head().tolist())

dim_email = df_email[["id", "raw_id", "raw_content", "sender_email", "sender_name", "content"]].rename(
    columns={"id": "comm_id"})

# --- Build Email User Bridge Rows ---
email_user_rows = []
for _, row in df_email.iterrows():
    comm_id = row["id"]
    emails = row.get("recipient_emails", []) or []
    names = row.get("recipient_names", []) or []

    # Recipients
    for email, name in zip(emails, names):
        if email:
            email_user_rows.append({
                "comm_id": comm_id,
                "email": email,
                "name": name,
                "location": None,
                "displayName": None,
                "phoneNumber": None,
                "isParticipant": True,
                "isOrganiser": False,
                "isAttendee": False,
                "isSpeaker": False,
                "role": "recipient"
            })

    # Sender
    if pd.notna(row["sender_email"]):
        email_user_rows.append({
            "comm_id": comm_id,
            "email": row["sender_email"],
            "name": row["sender_name"] if row["sender_name"] else None,
            "location": None,
            "displayName": None,
            "phoneNumber": None,
            "isParticipant": False,
            "isOrganiser": True,
            "isAttendee": False,
            "isSpeaker": False,
            "role": "sender"
        })

email_df = pd.DataFrame(email_user_rows)

# --- Meeting Users Processing ---
user_rows = []
for _, row in df[df["comm_type"] == "meeting"].iterrows():
    comm_id = row["id"]

    # Meeting attendees
    attendees = row.get("raw_meeting_attendees", [])
    if isinstance(attendees, str):
        try:
            attendees = ast.literal_eval(attendees)
        except Exception:
            attendees = []
    if isinstance(attendees, list):
        for a in attendees:
            if isinstance(a, dict):
                emails = normalize_emails(a.get("email", ""))
                name = a.get("name", None)
                for email in emails:
                    if email:
                        user_rows.append({
                            "comm_id": comm_id,
                            "name": name,
                            "email": email,
                            "location": a.get("location"),
                            "displayName": a.get("displayName"),
                            "phoneNumber": a.get("phoneNumber"),
                            "isAttendee": True,
                            "isParticipant": False,
                            "isSpeaker": False,
                            "isOrganiser": False,
                            "role": "attendee"
                        })

    # Participants
    for email in normalize_emails(row.get("raw_participants", [])):
        for p in str(email).split(","):
            if p.strip():
                user_rows.append({
                    "comm_id": comm_id,
                    "name": None,
                    "email": p.strip(),
                    "location": None,
                    "displayName": None,
                    "phoneNumber": None,
                    "isParticipant": True,
                    "isAttendee": False,
                    "isSpeaker": False,
                    "isOrganiser": False,
                    "role": "participant"
                })

    # Speakers
    speakers = row.get("raw_speakers", [])
    if speakers is None or not isinstance(speakers, (list, str)):
        speakers = []
    elif isinstance(speakers, str):
        try:
            speakers = ast.literal_eval(speakers)
            if not isinstance(speakers, list):
                speakers = []
        except Exception:
            speakers = []
    for s in speakers:
        name = s.get("name") if isinstance(s, dict) else s
        if name and isinstance(name, str):
            user_rows.append({
                "comm_id": comm_id,
                "name": name,
                "email": None,
                "location": None,
                "displayName": None,
                "phoneNumber": None,
                "isSpeaker": True,
                "isAttendee": False,
                "isParticipant": False,
                "isOrganiser": False,
                "role": "speaker"
            })

    # Organizers
    for email in normalize_emails(row.get("raw_organizer_email", [])):
        if email:
            user_rows.append({
                "comm_id": comm_id,
                "name": None,
                "email": email,
                "location": None,
                "displayName": None,
                "phoneNumber": None,
                "isOrganiser": True,
                "isAttendee": False,
                "isParticipant": False,
                "isSpeaker": False,
                "role": "organiser"
            })

df_users = pd.DataFrame(user_rows)

# --- Combine Users ---
df_users = pd.concat([df_users, email_df], ignore_index=True)

# Fix missing booleans and normalize
for col in ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]:
    df_users[col] = df_users.get(col, False)

for col in ["name", "email", "location", "displayName", "phoneNumber"]:
    df_users[col] = df_users[col].astype(str).fillna("").str.strip().str.lower()

# --- Heuristic Name-Email Matching ---
named_rows = df_users[df_users["name"] != ""].copy()
for i, row in named_rows.iterrows():
    if not row["email"]:
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
                    lambda e: last_clean in e and e.startswith(first_initial) or combined in e
                )
                if email_match.any():
                    df_users.loc[i, "email"] = df_users[df_users["email"] != ""].loc[email_match, "email"].iloc[0]

# --- Deduplicate Users and Assign user_id ---
user_lookup = df_users[["name", "email"]].drop_duplicates().copy()
user_lookup["user_id"] = [str(uuid4()) for _ in range(len(user_lookup))]
df_users = df_users.merge(user_lookup, on=["name", "email"], how="left")

# --- Create dim_user ---
dim_user = df_users[["name", "email", "user_id", "location", "displayName", "phoneNumber"]].drop_duplicates(
    subset="user_id").reset_index(drop=True)
dim_user["name"] = dim_user.groupby("user_id")["name"].transform(
    lambda x: x[x != ""].iloc[0] if any(x != "") else x.iloc[0])

# --- Clear role for meeting-related rows ---
meeting_ids = df[df["comm_type"] == "meeting"]["id"].unique()
df_users.loc[df_users["comm_id"].isin(meeting_ids), "role"] = ""

# --- Bridge Table ---
bridge_comm_user = df_users.groupby(["comm_id", "user_id", "role"], as_index=False)[
    ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]
].max()

# --- Fact Communication Table ---
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

# --- Dim Meeting ---
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
with pd.ExcelWriter("as.xlsx", engine="xlsxwriter") as writer:
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
