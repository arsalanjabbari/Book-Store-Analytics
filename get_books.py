from bs4 import BeautifulSoup
import pandas as pd
import time
import asyncio
import aiohttp
import nest_asyncio

headers = {'User-Agent':
               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
               'Chrome/50.0.2661.102 Safari/537.36',
           "Accept-Language":
               "en-US,en;q=0.5",
           'encoding': 'utf-8'}
site = 'https://www.iranketab.ir'
df = pd.read_csv('link_of_all_books.csv')
links = df['0'].values
site = 'https://www.iranketab.ir'

dictt = {'f_title': [], 'e_title': [], 'price_broken': [],
         'price_special': [], 'discount': [], 'data_rating': [],
         'publishers': [], 'author': [], 'book_id': [], 'shbook': [],
         'ghat': [], 'npages': [], 'translator': [], 'jyear': [], 'myear': [],
         'description': [], 'typej': [], 'sprint': [], 'product_features': [], 'product_tags': [], 'persian_bar': [],
         'lbook': []}
profiles_all = {'book_id': [], 'name': [], 'role': []}
profiles_link = {'book_id': [], 'name': [], 'link': [], 'role': []}
publishers_all = {'book_id': [], 'name': []}
publishers_link = {'book_id': [], 'name': [], 'link': []}
tags = {'book_id': [], 'name': []}


async def get_books(soup):
    bs = soup.find_all(class_='product-container well clearfix')[0].find_all(class_='col-md-9 col-sm-9')
    try:
        features = []
        for f in (soup.find(class_='product-features').find_all('h4')):
            features.append(f.text.strip())
    except:
        features = []
    try:
        pb = ''
        for b in soup.find_all(class_='persian-bar'):
            pb += (b.text.strip()) + ' '
    except:
        pb = ''
    try:

        description = (soup.find_all(class_='product-description')[0].text.strip())
    except:
        description = ''
    for i in bs:
        try:
            product_table = i.find(class_='product-table').find_all('tr')
        except:
            product_table = []
        book_id = None
        try:
            for n in range(len(product_table)):
                if 'کد' in product_table[n].text:
                    book_id = int(product_table[n].text.split(':')[1])
        except:
            print('o')
        try:
            tag = []
            for t in (soup.find(class_='product-tags').find_all('a')):
                tag.append(t.text.strip())
                tags['name'].append(t.text.strip())
                tags['book_id'].append(book_id)
        except:
            tag = []
        if book_id not in dictt['book_id']:
            shbook = None
            ghat = None
            npages = None
            p = None
            jyear = None
            myear = None
            typej = None
            sprint = None
            lbook = None
            trl = []
            for n in range(len(product_table)):
                if 'کد کتاب' in product_table[n].text:
                    book_id = int(product_table[n].text.split(':')[1].strip())
                if 'شابک' in product_table[n].text:
                    shbook = (product_table[n].text.split(':')[1].strip())
                if 'قطع' in product_table[n].text:
                    ghat = (product_table[n].text.split(':')[1].strip())
                if 'صفحه' in product_table[n].text:
                    npages = (product_table[n].text.split(':')[1].strip())
                if 'مترجم' in product_table[n].text:
                    try:
                        for p in product_table[n].find_all(class_='prodoct-attribute-item'):
                            trl.append(p.text.strip())
                            profiles_all['book_id'].append(book_id)
                            profiles_all['name'].append(p.text.strip())
                            profiles_all['role'].append('translator')
                    except:
                        print('no')
                    try:
                        for p in product_table[n].find_all('a'):
                            profiles_link['book_id'].append(book_id)
                            profiles_link['name'].append(p.text.strip())
                            profiles_link['link'].append(site + p.get('href'))
                            profiles_link['role'].append('translator')
                    except:
                        print('no')
                if 'سال انتشار شمسی' in product_table[n].text:
                    jyear = (product_table[n].text.split(':')[1].strip())
                if 'سال انتشار میلادی' in product_table[n].text:
                    myear = (product_table[n].text.split(':')[1].strip())
                if 'نوع جلد' in product_table[n].text:
                    typej = (product_table[n].text.split(':')[1].strip())
                if 'سری چاپ' in product_table[n].text:
                    sprint = (product_table[n].text.split(':')[1].strip())
                if 'زبان کتاب' in product_table[n].text:
                    lbook = (product_table[n].text.split(':')[1].strip()).split('و')
            dictt['product_features'].append(features)
            dictt['product_tags'].append(tag)
            dictt['persian_bar'].append(pb)
            dictt['description'].append(description)
            pub = []
            aut = []
            for person in (i.find_all(class_='col-xs-12 prodoct-attribute-items')):
                if 'نویسنده' in person.text:
                    try:
                        for p in person.find_all('a'):
                            aut.append(p.text.strip())
                            profiles_all['book_id'].append(book_id)
                            profiles_all['name'].append(p.text.strip())
                            profiles_all['role'].append('author')
                            profiles_link['book_id'].append(book_id)
                            profiles_link['name'].append(p.text.strip())
                            profiles_link['link'].append(site + p.get('href'))
                            profiles_link['role'].append('author')
                    except:
                        print('o')
                if 'انتشارات' in person.text:
                    try:
                        for p in person.find_all('a'):
                            pub.append(p.text.strip())
                            publishers_all['book_id'].append(book_id)
                            publishers_all['name'].append(p.text.strip())
                            publishers_link['book_id'].append(book_id)
                            publishers_link['name'].append(p.text.strip())
                            publishers_link['link'].append(site + p.get('href'))
                    except:
                        None
            try:
                dictt['f_title'].append(i.find(class_='product-name').text.strip().replace('\n', ''))
            except:
                dictt['f_title'].append(None)
            try:
                dictt['e_title'].append(i.find(class_='product-name-englishname ltr').text.strip().replace('\n', ''))
            except:
                dictt['e_title'].append(None)
            try:
                dictt['price_broken'].append(
                    int(i.find(class_='price price-broken').text.strip().replace('\n', '').replace(',', '')))
            except:
                try:
                    dictt['price_broken'].append(
                        int(i.find(class_='price').text.strip().replace('\n', '').replace(',', '')))
                except:
                    dictt['price_broken'].append(None)
            try:
                dictt['price_special'].append(
                    int(i.find(class_='price price-special').text.strip().replace('\n', '').replace(',', '')))
            except:
                try:
                    dictt['price_special'].append(
                        int(i.find(class_='price').text.strip().replace('\n', '').replace(',', '')))
                except:
                    dictt['price_special'].append(None)
            try:
                dictt['discount'].append(int(i.find(
                    style='float: left;font-size: 12px;line-height: 1.375;background-color: #fb3449;color: #fff;padding: 5px 30px 3px;-webkit-border-radius: 0 16px 16px 16px;border-radius: 0 16px 16px 16px;').text.split(
                    '%')[0].strip()))
            except:
                dictt['discount'].append(None)
            try:
                dictt['data_rating'].append(float(i.find(class_='my-rating').get('data-rating')))
            except:
                dictt['data_rating'].append(None)
            try:
                dictt['publishers'].append((pub))
            except:
                dictt['publishers'].append(None)
            try:
                dictt['author'].append((aut))
            except:
                dictt['author'].append(None)
            dictt['sprint'].append(sprint)
            dictt['book_id'].append(book_id)
            dictt['shbook'].append(shbook)
            dictt['ghat'].append(ghat)
            dictt['npages'].append(npages)
            dictt['translator'].append((trl))
            dictt['jyear'].append(jyear)
            dictt['myear'].append(myear)
            dictt['typej'].append(typej)
            dictt['lbook'].append(lbook)
    return (dictt, profiles_all, profiles_link, publishers_all, publishers_link, tags)


semaphore = asyncio.Semaphore(10)


async def fetch(session, url):
    try:
        async with semaphore:
            async with session.get(url, timeout=False) as response:
                response_text = await response.text()
                print(f"Response from {url}: {len(response_text)} bytes")
                return response_text
    except aiohttp.ClientError as e:
        print(f"Aiohttp ClientError while fetching {url}: {e}")
    except Exception as e:
        print(f"Unexpected error while fetching {url}: {e}")


count = []


async def scrape_book(url):
    async with aiohttp.ClientSession() as session:
        try:
            html = await fetch(session, url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                res = await get_books(soup)
                print(len(count))
                count.append(True)
                return 1
            else:
                print(f"Empty response from {url}")
                return 0
        except Exception as e:
            print(f"Error while scraping {url}: {e}")
            return 0


urls = links
nest_asyncio.apply()
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
loop = asyncio.get_event_loop()
batch_size = 500
for j in range(0, len(urls), batch_size):
    batch = urls[j:j + batch_size]
    results = loop.run_until_complete(asyncio.gather(*(scrape_book(url) for url in batch)))
    time.sleep(5)
i = 'all'
pd.DataFrame(dictt).to_csv(f'books {i}.csv')
pd.DataFrame(profiles_all).to_csv(f'all_persons {i}.csv')
pd.DataFrame(profiles_link).to_csv(f'all_persons_links {i}.csv')
pd.DataFrame(publishers_all).to_csv(f'all_publishers {i}.csv')
pd.DataFrame(publishers_link).to_csv(f'all_publishers_links {i}.csv')
pd.DataFrame(tags).to_csv(f'tags {i}.csv')
