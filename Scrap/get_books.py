from bs4 import BeautifulSoup
import pandas as pd
import time
import asyncio
import aiohttp
import nest_asyncio

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
        if book_id not in book_details['book_id']:
            shabak = None
            ghaat = None
            npages = None
            pyear = None
            gyear = None
            typej = None
            sprint = None
            lbook = None
            trl = []
            for n in range(len(product_table)):
                if 'کد کتاب' in product_table[n].text:
                    book_id = int(product_table[n].text.split(':')[1].strip())
                if 'شابک' in product_table[n].text:
                    shabak = (product_table[n].text.split(':')[1].strip())
                if 'قطع' in product_table[n].text:
                    ghaat = (product_table[n].text.split(':')[1].strip())
                if 'صفحه' in product_table[n].text:
                    npages = (product_table[n].text.split(':')[1].strip())
                if 'مترجم' in product_table[n].text:
                    try:
                        for p in product_table[n].find_all(class_='prodoct-attribute-item'):
                            trl.append(p.text.strip())
                            profiles['book_id'].append(book_id)
                            profiles['name'].append(p.text.strip())
                            profiles['role'].append('translator')
                    except:
                        print('no')
                    try:
                        for p in product_table[n].find_all('a'):
                            profiles_links['book_id'].append(book_id)
                            profiles_links['name'].append(p.text.strip())
                            profiles_links['link'].append(website + p.get('href'))
                            profiles_links['role'].append('translator')
                    except:
                        print('no')
                if 'سال انتشار شمسی' in product_table[n].text:
                    pyear = (product_table[n].text.split(':')[1].strip())
                if 'سال انتشار میلادی' in product_table[n].text:
                    gyear = (product_table[n].text.split(':')[1].strip())
                if 'نوع جلد' in product_table[n].text:
                    typej = (product_table[n].text.split(':')[1].strip())
                if 'سری چاپ' in product_table[n].text:
                    sprint = (product_table[n].text.split(':')[1].strip())
                if 'زبان کتاب' in product_table[n].text:
                    lbook = (product_table[n].text.split(':')[1].strip()).split('و')
            book_details['product_features'].append(features)
            book_details['product_tags'].append(tag)
            book_details['persian_bar'].append(pb)
            book_details['description'].append(description)
            pub = []
            aut = []
            for person in (i.find_all(class_='col-xs-12 prodoct-attribute-items')):
                if 'نویسنده' in person.text:
                    try:
                        for p in person.find_all('a'):
                            aut.append(p.text.strip())
                            profiles['book_id'].append(book_id)
                            profiles['name'].append(p.text.strip())
                            profiles['role'].append('author')
                            profiles_links['book_id'].append(book_id)
                            profiles_links['name'].append(p.text.strip())
                            profiles_links['link'].append(website + p.get('href'))
                            profiles_links['role'].append('author')
                    except:
                        print('o')
                if 'انتشارات' in person.text:
                    try:
                        for p in person.find_all('a'):
                            pub.append(p.text.strip())
                            publishers['book_id'].append(book_id)
                            publishers['name'].append(p.text.strip())
                            publishers_links['book_id'].append(book_id)
                            publishers_links['name'].append(p.text.strip())
                            publishers_links['link'].append(website + p.get('href'))
                    except:
                        pass
            try:
                book_details['f_title'].append(i.find(class_='product-name').text.strip().replace('\n', ''))
            except:
                book_details['f_title'].append(None)
            try:
                book_details['e_title'].append(
                    i.find(class_='product-name-englishname ltr').text.strip().replace('\n', ''))
            except:
                book_details['e_title'].append(None)
            try:
                book_details['price_broken'].append(
                    int(i.find(class_='price price-broken').text.strip().replace('\n', '').replace(',', '')))
            except:
                try:
                    book_details['price_broken'].append(
                        int(i.find(class_='price').text.strip().replace('\n', '').replace(',', '')))
                except:
                    book_details['price_broken'].append(None)
            try:
                book_details['price_special'].append(
                    int(i.find(class_='price price-special').text.strip().replace('\n', '').replace(',', '')))
            except:
                try:
                    book_details['price_special'].append(
                        int(i.find(class_='price').text.strip().replace('\n', '').replace(',', '')))
                except:
                    book_details['price_special'].append(None)
            try:
                book_details['discount'].append(int(i.find(
                    style='float: left;font-size: 12px;line-height: 1.375;background-color: #fb3449;color: #fff;padding: 5px 30px 3px;-webkit-border-radius: 0 16px 16px 16px;border-radius: 0 16px 16px 16px;').text.split(
                    '%')[0].strip()))
            except:
                book_details['discount'].append(None)
            try:
                book_details['data_rating'].append(float(i.find(class_='my-rating').get('data-rating')))
            except:
                book_details['data_rating'].append(None)
            try:
                book_details['publishers'].append((pub))
            except:
                book_details['publishers'].append(None)
            try:
                book_details['author'].append((aut))
            except:
                book_details['author'].append(None)
            book_details['sprint'].append(sprint)
            book_details['book_id'].append(book_id)
            book_details['shabak'].append(shabak)
            book_details['ghaat'].append(ghaat)
            book_details['npages'].append(npages)
            book_details['translator'].append((trl))
            book_details['pyear'].append(pyear)
            book_details['gyear'].append(gyear)
            book_details['typej'].append(typej)
            book_details['lbook'].append(lbook)
    return (book_details, profiles, profiles_links, publishers, publishers_links, tags)

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

headers = {'User-Agent':
               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
               'Chrome/50.0.2661.102 Safari/537.36',
           "Accept-Language":
               "en-US,en;q=0.5",
           'encoding': 'utf-8'}
website = 'https://www.iranketab.ir'
books_links_df = pd.read_csv('/Users/arsalanjabbari/Desktop/BOOTCAMP/Book-Store-Analytics/Data/books_links.csv')
links = books_links_df['0'].values

book_details = {'f_title': [], 'e_title': [], 'price_broken': [],
                'price_special': [], 'discount': [], 'data_rating': [],
                'publishers': [], 'author': [], 'book_id': [], 'shabak': [],
                'ghaat': [], 'npages': [], 'translator': [], 'pyear': [], 'gyear': [],
                'description': [], 'typej': [], 'sprint': [], 'product_features': [], 'product_tags': [],
                'persian_bar': [],
                'lbook': []}

profiles = {'book_id': [], 'name': [], 'role': []}
profiles_links = {'book_id': [], 'name': [], 'link': [], 'role': []}
publishers = {'book_id': [], 'name': []}
publishers_links = {'book_id': [], 'name': [], 'link': []}
tags = {'book_id': [], 'name': []}

urls = links

nest_asyncio.apply()
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
loop = asyncio.get_event_loop()
batch_size = 500
for j in range(0, len(urls), batch_size):
    batch = urls[j:j + batch_size]
    results = loop.run_until_complete(asyncio.gather(*(scrape_book(url) for url in batch)))
    time.sleep(5)

pd.DataFrame(book_details).to_csv(f'books.csv')
pd.DataFrame(profiles).to_csv(f'profiles.csv')
pd.DataFrame(profiles_links).to_csv(f'profiles_links.csv')
pd.DataFrame(publishers).to_csv(f'publishers.csv')
pd.DataFrame(publishers_links).to_csv(f'publishers_links.csv')
pd.DataFrame(tags).to_csv(f'tags.csv')
