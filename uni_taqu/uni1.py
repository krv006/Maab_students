import pandas as pd

# Excel faylni yuklash
file_path = "task.xlsx"  # Avvalgi qayta ishlangan fayl
df = pd.read_excel(file_path, engine="openpyxl")

# 21-dan 27-gacha bo'lgan qatorlarni olish
df_part = df.iloc[54:187]  # Pandas indeks 0-dan boshlanadi, shuning uchun 21-qator = 20-indeks

# Yangi faylga saqlash
output_file = "part_54_187.xlsx"
df_part.to_excel(output_file, index=False, engine="openpyxl")

print("✅ 21-27 qatorlar ajratildi va saqlandi:", output_file)
