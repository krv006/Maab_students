import pandas as pd
import json

with open("deals.json", "r", encoding="utf-8") as f:
    deals = json.load(f)

df = pd.DataFrame(deals)

print("Hamma Bitimlar (1-5 qator):")
print(df.head())

buyers_summary = df.groupby("buyer_name")["deal_cost"].agg(["count", "sum"]).reset_index()
print("\nDeal type bo'yicha tahlil:")
print(buyers_summary)

deal_type_summary = df.groupby("deal_type")["deal_cost"].agg(["count", "sum"]).reset_index()
print("\nDEal type bo'yicha tahlil:")
print(deal_type_summary)

status_summary = df.groupby("status")["deal_cost"].agg(["count", "sum"]).reset_index()
print("\nStatus bo'yicha tahlil:")
print(status_summary)

region_summary = df.groupby("refgion_name")["deal_cost"].agg(["count", "sum"]).reset_index()
print("\nViloyatlar bo'yicha tahlil:")
print(region_summary)

product_summary = df.groupby("product_name")["deal_cost"].agg(["count", "sum"]).reset_index()
print("\nMahsulotlar bo'yicha tahlil:")
print(product_summary)

df['contract_date'] = pd.to_datetime(df['contract_date'], errors='coerce')
date_summary = df.groupby(df['contract_date'].dt.to_period("M"))["deal_cost"].agg(["count", "sum"]).reset_index()
print("\nSana (oylik) bo'yicha tahllil:")
print(date_summary)

with pd.ExcelWriter("deal_tahlil_tooliq.xlsx") as writer:
    df.to_excel(writer, sheet_name="Barcha_deal", index=False)
    buyers_summary.to_excel(writer, sheet_name="Xaridor_tahlil", index=False)
    deal_type_summary.to_excel(writer, sheet_name="Deal_Type_tahlil", index=False)
    status_summary.to_excel(writer, sheet_name="Status_tahlil", index=False)
    region_summary.to_excel(writer, sheet_name="Mahsulotalr", index=False)
    product_summary.to_excel(writer, sheet_name="Mahsulotlar", index=False)
    date_summary.to_excel(writer, sheet_name="Sana_oylik", index=False)

print("\nTahlil tugadi. Fayl 'deal_tahlil_tooliq.xlsx' ismi bilan saqlandi.")
