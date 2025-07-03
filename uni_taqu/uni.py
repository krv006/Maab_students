import pandas as pd

# Excel faylni yuklash
file_path = "../simple.xlsx"
df = pd.read_excel(file_path, header=None)

# 1-3 qatorlarni 1 qatorga aylantirish
df.iloc[0] = df.iloc[0:3].fillna('').astype(str).agg(' '.join, axis=0)
df = df.iloc[3:].reset_index(drop=True)  # 4-23 qatorlarini saqlab qolish

# Yangi ustun nomlarini o‘rnatish
columns = ['id', 'Ежемесячный Фин. Отчет Университета'] + \
          list(df.iloc[0, 2:5]) + \
          ['2024 ostatok'] + \
          ['Yanvar', 'Fevral', 'Mart', 'Aprel', 'May', 'Iyun', 'Iyul', 'Avgust', 'Sentabr', 'Oktabr', 'Noyabr', 'Dekabr'] + \
          ['Grand Total 2024-2025']

df.columns = columns
df = df.iloc[1:].reset_index(drop=True)  # 1-qator sarlavha bo‘lgani uchun qirqamiz

# Yangi faylga saqlash
output_file = "task.xlsx"
df.to_excel(output_file, index=False)

print("Fayl muvaffaqiyatli yaratildi:", output_file)
