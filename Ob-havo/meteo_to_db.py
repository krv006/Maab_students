import pandas as pd
import pyodbc

try:
    # Databasega ulanish
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=192.168.111.14;"
        "DATABASE=weather;"
        "UID=sa;"
        "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
    )

    # Cursor obyekti yaratish
    cursor = conn.cursor()

    # CSV faylni o‘qish
    data = pd.read_csv('meteo_data.csv', encoding='utf-8')

    # Ma'lumotlarni minimal tozalash
    data['Region Code'] = data['Region Code'].apply(lambda x: str(x) if pd.notna(x) else None)  # Bo‘sh qiymatlar uchun None
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce').dt.strftime('%Y-%m-%d')  # Sana formatini to‘g‘rilash

    # SQL so‘rovi
    sql_query = """
        INSERT INTO meteo_data (
            date,
            region_code,
            timestamp,
            title,
            humidity_10cm,
            humidity_20cm,
            humidity_30cm,
            date_time,
            temp_10cm,
            temp_20cm,
            temp_30cm
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Ma'lumotlarni qator-qator kiritish
    inserted_rows = 0
    for index, row in data.iterrows():
        try:
            cursor.execute(sql_query, (
                row['Date'] if pd.notna(row['Date']) else None,  # DATE turi
                row['Region Code'],  # VARCHAR(10)
                str(row['Timestamp']) if pd.notna(row['Timestamp']) else None,  # NVARCHAR(100)
                str(row['Title']) if pd.notna(row['Title']) else None,  # NVARCHAR(100)
                str(row['Влажность почвы 10 см (-10)']) if pd.notna(row['Влажность почвы 10 см (-10)']) else None,  # NVARCHAR(100)
                str(row['Влажность почвы 20 см (-20)']) if pd.notna(row['Влажность почвы 20 см (-20)']) else None,  # NVARCHAR(100)
                str(row['Влажность почвы 30 см (-30)']) if pd.notna(row['Влажность почвы 30 см (-30)']) else None,  # NVARCHAR(100)
                str(row['Дата']) if pd.notna(row['Дата']) else None,  # NVARCHAR(100)
                str(row['Температура почвы 10 см (-10)']) if pd.notna(row['Температура почвы 10 см (-10)']) else None,  # NVARCHAR(100)
                str(row['Температура почвы 20 см (-20)']) if pd.notna(row['Температура почвы 20 см (-20)']) else None,  # NVARCHAR(100)
                str(row['Температура почвы 30 см (-30)']) if pd.notna(row['Температура почвы 30 см (-30)']) else None  # NVARCHAR(100)
            ))
            inserted_rows += 1
        except Exception as e:
            print(f"Qator {index} da xatolik: {e}")
            print(f"Qiymatlar: {row.to_dict()}")
            continue

    # O‘zgarishlarni saqlash
    conn.commit()
    print(f"Muvaffaqiyatli kiritilgan qatorlar soni: {inserted_rows}")

except Exception as e:
    print(f"Xatolik yuz berdi: {e}")

finally:
    # Ulanishni yopish
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()