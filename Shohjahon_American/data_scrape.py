import os

import pandas as pd
import requests

API_KEY = "HI_BITCH"
PAGE_SIZE = 100
SAVE_PATH = "/lakehouse/default/Files/subapi3/"

standard_endpoints = [
    'ClientDispatch', 'Company', 'Division', 'Leftover', 'Ticket',
    'TicketDriverAddedEvent', 'TicketEventHistory', 'TicketType', 'Users', 'Role',
    'EmployeeClass', 'Truck', 'Trailer', 'TruckEventHistory', 'TruckLocationHistory',
    'TruckMessageHistory', 'TruckPointHistory', 'FuelEvent', 'Dvir', 'EldEntry',
    'Customer', 'Point', 'PointClass', 'Tag', 'TagGroup', 'SlumpProfile',
    'FunctionalityTier', 'VehicleComponent', 'VehicleMeasurement', 'VehicleTest',
]

no_params = [
    "TicketEventEventCode",
    "TicketEventStatusCode"
]

custom_endpoints = {
    "TrackableAssetStatus": "API_URL/TrackableAssetStatus?assetType=1"
}

for endpoint in standard_endpoints:
    all_data = []
    page = 1
    existing_records = 0

    file_path = f"{SAVE_PATH}{endpoint}.csv"
    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path)
            existing_records = len(existing_df)
        except:
            existing_records = 0

    while True:
        url = f"API_URL/{endpoint}?page={page}&pageSize={PAGE_SIZE}"
        headers = {
            "X-API-KEY": API_KEY,
            "accept": "application/json"
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"❌ Error {response.status_code} on {endpoint} page {page}")
            break

        json_data = response.json()
        data = json_data.get("data", [])
        all_data.extend(data)

        pagination = json_data.get("metadata", {}).get("pagination", {})
        total_pages = pagination.get("totalPageCount", 1)
        total_records = pagination.get("totalRecordCount", 0)

        if len(all_data) + existing_records >= total_records:
            break

        if page >= total_pages:
            break

        page += 1

    if all_data:
        new_df = pd.DataFrame(all_data)

        if os.path.exists(file_path):
            try:
                existing_df = pd.read_csv(file_path)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df.drop_duplicates(inplace=True)
                combined_df.to_csv(file_path, index=False)
            except:
                new_df.to_csv(file_path, index=False)
        else:
            new_df.to_csv(file_path, index=False)

        print(f" Saved (incremental): {endpoint}")
    else:
        print(f"️ No new data in: {endpoint}")

for endpoint in no_params:
    url = f"API_URL/{endpoint}"
    headers = {
        "X-API-KEY": API_KEY,
        "accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json().get("data", [])
        if data:
            df = pd.DataFrame(data)
            df.to_csv(f"{SAVE_PATH}{endpoint}.csv", index=False)
            print(f" Saved: {endpoint}")
        else:
            print(f" Empty response in: {endpoint}")
    else:
        print(f" Error {response.status_code} in: {endpoint}")

for name, url in custom_endpoints.items():
    headers = {
        "X-API-KEY": API_KEY,
        "accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json().get("data", [])
        if data:
            df = pd.DataFrame(data)
            df.to_csv(f"{SAVE_PATH}{name}.csv", index=False)
            print(f" Saved: {name}")
        else:
            print(f" Empty response in: {name}")
    else:
        print(f"Error {response.status_code} in: {name}")
