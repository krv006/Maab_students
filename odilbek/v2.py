import pandas as pd
import json
import ast
from pandas import json_normalize
from uuid import uuid4
import re
import html


# JSON parsing funksiyasi
def try_parse(cell):
    if isinstance(cell, (dict, list)):
        return cell
    if not isinstance(cell, str):
        return {}
    cell = cell.strip()
    encoding_fixes = {
        'â€™': "'", 'â€œ': '"', 'â€': '"', 'â€“': '-', 'â€”': '-', 'â€¦': '...',
        'â€˜': "'", 'â€\\x9d': '"', 'вЂ™': "'", 'вЂ“': '-', 'вЂ”': '-', 'вЂ‹': '',
        '“': '"', '”': '"', '’': "'", '‘': "'", '–': '-', '—': '-', '…': '...'
    }
    for k, v in encoding_fixes.items():
        cell = cell.replace(k, v)
    cell = re.sub(r'<mailto:[^>]+>|\[cid:[^\]]+\]', '', cell)
    cell = cell.replace('\\r\\n', '\\n').replace('\r\n', '\\n').replace('\n', '\\n').replace('\r', '\\n')
    if cell.count('"') % 2 != 0:
        cell += '"'
    try:
        return json.loads(cell)
    except json.JSONDecodeError:
        return {}


# Email parsing funksiyalari
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
    recipients = record.get("recipients", [])
    if isinstance(recipients, str):
        try:
            recipients = ast.literal_eval(recipients)
        except Exception:
            recipients = []
    if not isinstance(recipients, list):
        return [], []
    for r in recipients:
        if isinstance(r, dict):
            email = r.get("emailAddress", {}).get("address")
            name = r.get("emailAddress", {}).get("name")
            if email:
                emails.append(email.strip().lower())
                names.append(name)
    return emails, names


# Ma'lumotlarni yuklash
df_raw = pd.read_csv("raw_comm_rows (6).csv")  # Fayl nomini moslashtiring
df_raw["parsed"] = df_raw["raw_content"].apply(try_parse)
expanded = json_normalize(df_raw["parsed"])
expanded.columns = [f"raw_content.{col}" for col in expanded.columns]
df = pd.concat([df_raw.drop(columns=["raw_content", "parsed"]), expanded], axis=1)

# bridge_comm_user uchun ma'lumotlarni tayyorlash
participants_rows = []
for idx, row in df.iterrows():
    comm_id = str(uuid4())
    # Email ma'lumotlari
    if row.get("comm_type") == "email":
        parsed_content = row.get("parsed", {})
        sender_email = parse_email_field(parsed_content, "sender") or parse_email_field(parsed_content, "from")
        sender_name = parse_email_name(parsed_content, "sender") or parse_email_name(parsed_content, "from")
        recipient_emails, recipient_names = parse_recipients(parsed_content)

        # Sender qo'shish
        if pd.notna(sender_email):
            participants_rows.append({
                "comm_id": comm_id,
                "name": sender_name,
                "email": sender_email.strip().lower() if sender_email else None,
                "role": "sender",
                "isOrganiser": True,
                "isParticipant": False,
                "isAttendee": False,
                "isSpeaker": False
            })

        # Recipients qo'shish
        for email, name in zip(recipient_emails, recipient_names):
            participants_rows.append({
                "comm_id": comm_id,
                "name": name,
                "email": email,
                "role": "recipient",
                "isOrganiser": False,
                "isParticipant": True,
                "isAttendee": False,
                "isSpeaker": False
            })

    # Meeting ma'lumotlari
    elif row.get("comm_type") == "meeting":
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


        for a in normalize_emails(row.get("raw_content.meeting_attendees")):
            if isinstance(a, dict):
                for email in normalize_emails(a.get("email")):
                    participants_rows.append({
                        "comm_id": comm_id,
                        "name": a.get("name"),
                        "email": email,
                        "role": "attendee",
                        "isAttendee": True,
                        "isParticipant": False,
                        "isOrganiser": False,
                        "isSpeaker": False
                    })

        for email in normalize_emails(row.get("raw_content.participants")):
            for p in str(email).split(","):
                participants_rows.append({
                    "comm_id": comm_id,
                    "name": None,
                    "email": p.strip(),
                    "role": "participant",
                    "isAttendee": False,
                    "isParticipant": True,
                    "isOrganiser": False,
                    "isSpeaker": False
                })

        for s in normalize_emails(row.get("raw_content.speakers")):
            name = s.get("name") if isinstance(s, dict) else s
            participants_rows.append({
                "comm_id": comm_id,
                "name": name,
                "email": None,
                "role": "speaker",
                "isAttendee": False,
                "isParticipant": False,
                "isOrganiser": False,
                "isSpeaker": True
            })

        for email in normalize_emails(row.get("raw_content.organizer_email")):
            participants_rows.append({
                "comm_id": comm_id,
                "name": None,
                "email": email,
                "role": "organiser",
                "isAttendee": False,
                "isParticipant": False,
                "isOrganiser": True,
                "isSpeaker": False
            })

# df_users yaratish
df_users = pd.DataFrame(participants_rows)
for col in ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]:
    df_users[col] = df_users[col].fillna(False)

# Xatoni tuzatish: name va email ustunlarini alohida qayta ishlash
df_users["name"] = df_users["name"].astype(str).fillna("").str.strip().str.lower()
df_users["email"] = df_users["email"].astype(str).fillna("").str.strip().str.lower()

# dim_user yaratish va deduplikatsiya
dim_user = df_users[["name", "email"]].drop_duplicates().copy()
dim_user["user_id"] = [str(uuid4()) for _ in range(len(dim_user))]


# Name-email matching
def only_letters(s):
    return re.sub(r"[^a-zA-Z]", "", s or "")


named_rows = df_users[df_users["name"] != ""].copy()
email_rows = df_users[df_users["email"] != ""].copy()

for i, name_row in named_rows.iterrows():
    raw_name = name_row["name"]
    if not raw_name or " " not in raw_name:
        continue
    cleaned_parts = re.sub(r"[^a-zA-Z\s]", "", raw_name).strip().split()
    if len(cleaned_parts) != 2:
        continue
    first, last = cleaned_parts
    first_clean = first.lower()
    last_clean = last.lower()
    first_initial = first_clean[0]
    combined = first_clean + last_clean
    for j, email_row in email_rows.iterrows():
        email = email_row["email"]
        if email:
            email_clean = only_letters(email)
            if (last_clean in email_clean and email_clean.startswith(first_initial)) or (combined in email_clean):
                df_users.loc[j, "name"] = raw_name

# user_id tayinlash
df_users = df_users.merge(dim_user[["name", "email", "user_id"]], on=["name", "email"], how="left")
dim_user = df_users[["user_id", "name", "email"]].drop_duplicates(subset="user_id").reset_index(drop=True)

# Meetinglar uchun role tozalash
meeting_ids = df[df["comm_type"] == "meeting"]["id"].unique()
df_users.loc[df_users["comm_id"].isin(meeting_ids), "role"] = ""

# bridge_comm_user yaratish
bridge_comm_user = df_users.groupby(["comm_id", "user_id", "role"], as_index=False)[
    ["isAttendee", "isParticipant", "isSpeaker", "isOrganiser"]
].max()

# Eksport qilish
with pd.ExcelWriter("star_schema_output.xlsx", engine="xlsxwriter") as writer:
    bridge_comm_user.to_excel(writer, sheet_name="bridge_comm_user", index=False)
    dim_user.to_excel(writer, sheet_name="dim_user", index=False)

print("bridge_comm_user jadvali muvaffaqiyatli eksport qilindi!")