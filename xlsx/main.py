import pandas as pd
import json

df = pd.read_excel("input_raw_content.xlsx")

# 2. Dimensional jadvallar
dim_comm_type = df[["comm_type"]].drop_duplicates().reset_index(drop=True)
dim_comm_type["comm_type_id"] = dim_comm_type.index + 1

dim_subject = df[["subject"]].drop_duplicates().reset_index(drop=True)
dim_subject["subject_id"] = dim_subject.index + 1

dim_user_records = []
dim_calendar_records = []
dim_audio_records = []
dim_video_records = []
dim_transcript_records = []
dim_meeting_records = []
dim_email_records = []
bridge_comm_user_records = []
fact_communication_records = []

# 3. JSON parsing helper
def parse_json(value):
    try:
        return json.loads(value)
    except:
        return None

# 4. Har bir qatorni qayta ishlash
for idx, row in df.iterrows():
    raw = parse_json(row["raw_content"])
    if not raw:
        continue

    comm_id = row["id"]
    calendar_id = raw.get("calendar_id")

    # dim_calendar
    if raw.get("dateString"):
        dim_calendar_records.append({
            "calendar_id": calendar_id,
            "date": raw.get("dateString")
        })

    # dim_audio
    if raw.get("audio_url"):
        dim_audio_records.append({
            "calendar_id": calendar_id,
            "audio_url": raw.get("audio_url")
        })

    # dim_video
    if raw.get("video_url"):
        dim_video_records.append({
            "calendar_id": calendar_id,
            "video_url": raw.get("video_url")
        })

    # dim_transcript
    if raw.get("transcript_url"):
        dim_transcript_records.append({
            "calendar_id": calendar_id,
            "transcript_url": raw.get("transcript_url")
        })

    # dim_meeting
    dim_meeting_records.append({
        "calendar_id": calendar_id,
        "organizer_email": raw.get("organizer_email"),
        "participants": ", ".join(raw.get("participants", [])) if isinstance(raw.get("participants"), list) else raw.get("participants")
    })

    # dim_email
    if "from" in raw and "emailAddress" in raw["from"]:
        dim_email_records.append({
            "calendar_id": calendar_id,
            "email_from": raw["from"]["emailAddress"].get("address"),
            "name": raw["from"]["emailAddress"].get("name")
        })

    # dim_user + bridge_comm_user
    for user in raw.get("meeting_attendees", []):
        email = user.get("email")
        if email:
            dim_user_records.append({"email": email})
            bridge_comm_user_records.append({
                "comm_id": comm_id,
                "user_email": email
            })

    # fact_communication
    fact_communication_records.append({
        "comm_id": comm_id,
        "comm_type": row["comm_type"],
        "subject": row["subject"],
        "calendar_id": calendar_id,
        "ingested_at": row["ingested_at"],
        "processed_at": row["processed_at"],
        "is_processed": row["is_processed"],
        "is_loaded": row["is_loaded"]
    })

# 5. DataFrame-larga aylantirish
dim_user = pd.DataFrame(dim_user_records).drop_duplicates().reset_index(drop=True)
dim_calendar = pd.DataFrame(dim_calendar_records).drop_duplicates().reset_index(drop=True)
dim_audio = pd.DataFrame(dim_audio_records).drop_duplicates().reset_index(drop=True)
dim_video = pd.DataFrame(dim_video_records).drop_duplicates().reset_index(drop=True)
dim_transcript = pd.DataFrame(dim_transcript_records).drop_duplicates().reset_index(drop=True)
dim_meeting = pd.DataFrame(dim_meeting_records).drop_duplicates().reset_index(drop=True)
dim_email = pd.DataFrame(dim_email_records).drop_duplicates().reset_index(drop=True)
bridge_comm_user = pd.DataFrame(bridge_comm_user_records).drop_duplicates().reset_index(drop=True)
fact_communication = pd.DataFrame(fact_communication_records).drop_duplicates().reset_index(drop=True)

# 6. Excel faylga yozish
with pd.ExcelWriter("parsed_communications.xlsx", engine="xlsxwriter") as writer:
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
