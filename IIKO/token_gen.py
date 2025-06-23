import requests
import json

# Step 1: Your API login
API_LOGIN = "e8a769994d6948cbaca35c09b1345c71"

# Step 2: Token endpoint
url = "https://api-ru.iiko.services/api/1/access_token"

# Step 3: Headers and body
headers = {
    "Content-Type": "application/json"
}

body = {
    "apiLogin": API_LOGIN
}

# Step 4: Make the request
response = requests.post(url, headers=headers, json=body)

# Step 5: Print result
print("📡 Status Code:", response.status_code)
print("📄 Response JSON:", json.dumps(response.json(), indent=2))

# Save token for later
if response.status_code == 200:
    token = response.json()["token"]
    with open("iiko_token.txt", "w") as f:
        f.write(token)
    print("✅Token saved to iiko_token.txt")
