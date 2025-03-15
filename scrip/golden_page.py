from bs4 import BeautifulSoup
import requests
import pandas as pd

url = 'https://www.goldenpages.uz/en/'
page = requests.get(url)
soup = BeautifulSoup(page.text, 'html')

titles = soup.find_all('h3')
table_titles = [title.text.strip() for title in titles][:20]

table = soup.find_all('span')[4:]
table_num = [title.text.strip() for title in table][:20]
table_num_final = [int(i) for i in table_num]

data = {'Categories' : table_titles, 'Count' : table_num_final}
df = pd.DataFrame(data)
print(df)
