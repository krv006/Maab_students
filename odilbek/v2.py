import pandas as pd
import json
import ast
from pandas import json_normalize
from uuid import uuid4
import re

# --- Load and parse ---
df_raw = pd.read_csv("../xlsx/raw_comm_rows (6).csv")

def safe_parse(text):
    try:
        return json.loads(text)
    except Exception:
        return {}

def parse_recipients(record):
    emails, names = [], []

    raw_recipients = record.get("recipients", [])

    # Handle stringified list
    if isinstance(raw_recipients, str):
        try:
            raw_recipients = ast.literal_eval(raw_recipients)
        except Exception:
            raw_recipients = []

    # Ensure it's a list
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

df_raw["parsed"] = df_raw["raw_content"].apply(safe_parse)
expanded = json_normalize(df_raw["parsed"])
expanded.columns = [f"raw_{col}" for col in expanded.columns]
df = pd.concat([df_raw.drop(columns=["raw_content", "parsed"]), expanded], axis=1)

# --- DIMENSIONS ---
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

# --- Heuristic name-email matching ---
named_rows = df_users[df_users["name"].notna() & df_users["email"].isna()].copy()
email_rows = df_users[df_users["email"].notna()].copy()

def only_letters(s):
    return re.sub(r"[^a-zA-Z]", "", s or "")

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

# --- Deduplication: Prefer email entries, then merge by name ---
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

final_users = []
for _, row in df_users.iterrows():
    name = row["name"]
    email = row["email"]
    norm_name = name.lower().strip() if pd.notnull(name) else ""
    norm_email = email.lower().strip() if pd.notnull(email) else ""
    key = f"{norm_name}|{norm_email}" if norm_email else f"{norm_name}|"
    if key in person_map:
        user_id = person_map[key]
    elif norm_name in email_priority_users:
        user_id = email_priority_users[norm_name]
    else:
        # If email is None, but name matches existing user with email → use that ID
        if not norm_email and norm_name in [n.lower().strip() for n in email_priority_users.keys()]:
            user_id = email_priority_users[norm_name]
        else:
            user_id = str(uuid4())

    person_map[key] = user_id

    final_users.append({
        "user_id": user_id,
        "name": name,
        "email": norm_email if norm_email else email,
        "location": row.get("location"),
        "displayName": row.get("displayName"),
        "phoneNumber": row.get("phoneNumber")
    })

dim_user_full = pd.DataFrame(final_users)

df_users = df_users.merge(dim_user_full[["name", "email", "user_id"]], on=["name", "email"], how="left")

latest_user_ids = dim_user_full.dropna(subset=["email"]).drop_duplicates(subset=["name", "email"])
name_only_ids = dim_user_full[dim_user_full["email"].isna() & dim_user_full["name"].notna()]

reassign_map = {}
for _, row in name_only_ids.iterrows():
    name = row["name"]
    old_id = row["user_id"]
    match = latest_user_ids[latest_user_ids["name"] == name]
    if not match.empty:
        new_id = match.iloc[0]["user_id"]
        reassign_map[old_id] = new_id

df_users["user_id"] = df_users["user_id"].replace(reassign_map)


# --- Email Decomposer ---
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

def parse_recipients(record):
    emails, names = [], []
    for r in record.get("recipients", []):
        if isinstance(r, dict):
            email = r.get("emailAddress", {}).get("address")
            name = r.get("emailAddress", {}).get("name")
            if email:
                emails.append(email)
                names.append(name)
    return emails, names

# --- Parse and tag email roles ---
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
        email_user_rows.append({"comm_id": comm_id, "email": email, "name": name, "isParticipant": True, "isAttendee": False, "isOrganiser": False, "isSpeaker": False, "role": "receiver"})
    if pd.notna(row.get("sender_email")):
        email_user_rows.append({"comm_id": comm_id, "email": row["sender_email"], "name": row["sender_name"], "isOrganiser": True, "isParticipant": False, "isAttendee": False, "isSpeaker": False, "role": "sender"})

email_df = pd.DataFrame(email_user_rows)

# --- Deduplicate users and assign user_id ---
# Fix missing booleans and normalize
for col in ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]:
    email_df[col] = email_df.get(col, False)

# Append with consistent schema
df_users = pd.concat([df_users, email_df], ignore_index=True)

for col in ["name", "email"]:
    df_users[col] = df_users[col].astype(str).fillna("").str.strip()

# Preserve role info before merging user_ids
df_users = df_users.merge(
    dim_user_full[["name", "email", "user_id"]].drop_duplicates(),
    on=["name", "email"],
    how="left"
)

user_lookup = df_users[["name", "email"]].drop_duplicates().copy()
user_lookup["user_id"] = [str(uuid4()) for _ in range(len(user_lookup))]

# Assign user_id if not already present
if "user_id" not in df_users.columns or df_users["user_id"].isna().any():
    user_lookup = df_users[["name", "email"]].drop_duplicates().copy()
    user_lookup["user_id"] = [str(uuid4()) for _ in range(len(user_lookup))]

    df_users = df_users.merge(user_lookup, on=["name", "email"], how="left")

dim_user_full = df_users[["name", "email", "user_id", "location", "displayName", "phoneNumber"]].drop_duplicates(subset="user_id").reset_index(drop=True)


# Merge user_id after normalizing

# Ensure no duplicate 'user_id' column before merge
if "user_id" in df_users.columns:
    df_users = df_users.drop(columns=["user_id"])

columns_to_add = [col for col in ["location", "displayName", "phoneNumber"] if col in dim_user_full.columns]

df_users = df_users.merge(
    dim_user_full[["name", "email", "user_id"] + columns_to_add],
    on=["name", "email"],
    how="left"
)

# --- Final clean dim_user table ---
dim_user = dim_user_full[dim_user_full["user_id"].isin(df_users["user_id"].unique())].drop_duplicates("user_id").reset_index(drop=True)

# Backfill names if missing
user_id_to_name = dim_user.dropna(subset=["name"]).drop_duplicates(subset="user_id").set_index("user_id")["name"].to_dict()
dim_user["name"] = dim_user.apply(
    lambda row: user_id_to_name.get(row["user_id"], row["name"]) if pd.isna(row["name"]) else row["name"],
    axis=1
)
# If the comm_id is from a meeting, clear the 'role' column
meeting_ids = df[df["comm_type"] == "meeting"]["id"].unique()
df_users.loc[df_users["comm_id"].isin(meeting_ids), "role"] = ""

# --- Bridge Table ---
for col in ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]:
    df_users[col] = df_users[col].fillna(False)

if "role" not in df_users.columns:
    df_users["role"] = ""

bridge_comm_user = (
    df_users.groupby(["comm_id", "user_id", "role"], as_index=False)[
        ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]
    ].max()
)

# --- Remaining dimension tables ---
dim_audio = df[["raw_audio_url"]].dropna().drop_duplicates().copy()
dim_audio["audio_id"] = dim_audio.index + 1

dim_video = df[["raw_video_url"]].dropna().drop_duplicates().copy()
dim_video["video_id"] = dim_video.index + 1

dim_transcript = df[["raw_transcript_url"]].dropna().drop_duplicates().copy()
dim_transcript["transcript_id"] = dim_transcript.index + 1

# --- Fact Communication Table ---
fact_communication = df.merge(dim_comm_type, on="comm_type", how="left") \
    .merge(dim_subject, on="subject", how="left") \
    .merge(dim_calendar, on="raw_calendar_id", how="left") \
    .merge(dim_audio, on="raw_audio_url", how="left") \
    .merge(dim_video, on="raw_video_url", how="left") \
    .merge(dim_transcript, on="raw_transcript_url", how="left")

fact_communication = fact_communication[[
    "id", "raw_id", "source_id", "comm_type_id", "subject_id", "calendar_id",
    "audio_id", "video_id", "transcript_id", "raw_dateString", "ingested_at",
    "processed_at", "is_processed", "raw_title", "raw_duration"
]].rename(columns={"id": "comm_id", "raw_dateString": "datetime_id"})

# --- Dim Meeting ---
dim_meeting = df[df["comm_type"] == "meeting"].copy()
dim_meeting = dim_meeting.merge(fact_communication[["comm_id", "raw_id"]], on="raw_id", how="left")
dim_meeting = dim_meeting[[
    "comm_id", "raw_calendar_id", "raw_audio_url", "raw_dateString", "raw_duration",
    "raw_host_email", "raw_id", "raw_title", "raw_video_url", "raw_transcript_url"
]].rename(columns={
    "raw_calendar_id": "calendar_id",
    "raw_audio_url": "audio_url",
    "raw_dateString": "dateString",
    "raw_duration": "duration",
    "raw_host_email": "host_email",
    "raw_title": "title",
    "raw_video_url": "video_url",
    "raw_transcript_url": "transcript_url"
})

# --- Merge meeting with dimension keys ---
dim_meeting["calendar_id"] = dim_meeting["calendar_id"].astype(str)
dim_calendar["calendar_id"] = dim_calendar["calendar_id"].astype(str)
dim_meeting = dim_meeting.merge(dim_calendar, on="calendar_id", how="left") \
    .merge(dim_audio, left_on="audio_url", right_on="raw_audio_url", how="left") \
    .merge(dim_video, left_on="video_url", right_on="raw_video_url", how="left") \
    .merge(dim_transcript, left_on="transcript_url", right_on="raw_transcript_url", how="left")

dim_meeting = dim_meeting[[
    "comm_id", "calendar_id", "audio_id", "video_id", "transcript_id",
    "dateString", "duration", "host_email", "raw_id", "title"
]]

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