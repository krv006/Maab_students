import requests
import pandas as pd
import base64
from requests.auth import HTTPBasicAuth

url = "https://smartup.online/#/!5phmhu7gz/trade/txs/tdeal/order$export"

username = "powerbi@epco"
password = "said_2021"

endcode_credentials = base64.b64encode(f"{username}:{password}".encode()).decode()

# Headers
headers = {
    'Authorization': f"Basic {endcode_credentials}",
    'project_code': 'trade',
    'filial_id': '6091241'
}


response = requests.get(url, headers=headers)

if response.status_code == 200:
    print("Success ✅")
    print(response.json())
else:
    print("Xatolik:", response.status_code)
    print(response.text)

json_data = response.json()