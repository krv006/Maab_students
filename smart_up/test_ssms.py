import pandas as pd
from sqlalchemy import create_engine
import urllib

# 1. JSON faylni o‘qish
print("📥 JSON fayl o'qilmoqda...")
df = pd.read_json("smartup_order_export.json")
print(f"✅ {len(df)} ta satr yuklandi.")

# 2. SQL Server ulanish parametrlari (SQL Authentication bilan)
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=WIN-LORQJU2719N;"
    "DATABASE=Test;"
    "TrustServerCertificate=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# 3. Ma'lumotlar bazaga yoziladi
table_name = "smartup_data"

try:
    print("⬆️ Ma’lumotlar bazaga yozilmoqda...")
    df.to_sql(table_name, con=engine, index=False, if_exists="replace")
    print(f"✅ {table_name} jadvalga muvaffaqiyatli yozildi.")
except Exception as e:
    print(f"❌ Xatolik: {e}")
