import pandas as pd
import os
import pyodbc

# SQL Serverga ulanish (o'zingizning ulanish ma'lumotlaringizni kiriting)
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      'SERVER=192.168.111.14;'
                      'DATABASE=cropagrodb;'
                      'UID=Kamron;'
                      'PWD=1')

# SQL so'rovini bajarish (faqat Area ustuni)
query = """
SELECT [Area]
FROM [dbo].[CropAgroInfo]
WHERE [HarvestName] = 'Paxta'
"""
df = pd.read_sql(query, conn)

# 'Area' ustunini raqamga aylantirish
df['Area'] = pd.to_numeric(df['Area'], errors='coerce')  # 'coerce' noto'g'ri qiymatlar uchun NaN qiladi

# NaN qiymatlarni olib tashlash (agar kerak bo'lsa)
df.dropna(subset=['Area'], inplace=True)

# Area ni hisoblash va formatlash
df['Formatted_Area'] = (df['Area'] / 1000).apply(lambda x: f"{x:.2f}")  # 2 o'nlik raqam bilan formatlash

# test_bi_python papkasini yaratish
output_dir = os.path.join(os.path.expanduser("~"), "Desktop", "test_bi_python")
os.makedirs(output_dir, exist_ok=True)

# Excel faylga saqlash yo‘li
file_path = os.path.join(output_dir, "rv.xlsx")
df.to_excel(file_path, index=False)

print(f"Fayl saqlandi: {file_path}")

# Ulanishni yopish
conn.close()
