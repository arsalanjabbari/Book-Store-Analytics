import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

headers = {'User-Agent':
               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
           "Accept-Language":
               "en-US,en;q=0.5",
           'encoding': 'utf-8'}
site = 'https://www.iranketab.ir'
book_links = []
i = 1
while True:
    try:
        r = requests.get(f'https://www.iranketab.ir/book?pagenumber={i}&pagesize=500', headers=headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        links = soup.find_all(class_='col-lg-6 col-md-6 col-xs-12')
        if len(links) == 0:
            break
        else:
            for j in links:
                book_links.append(site + j.find(class_='product-item-link').get('href'))
        print(i, end=' ')
        i += 1
    except:
        time.sleep(20)
pd.DataFrame(book_links).to_csv('links of all books.csv')
