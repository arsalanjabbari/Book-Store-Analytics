from bs4 import BeautifulSoup
import pandas as pd
import time
import asyncio
import aiohttp
import nest_asyncio
from urllib.parse import unquote

books=pd.read_csv('books all.csv')
books.drop(columns=['Unnamed: 0'],inplace=True)
df_profiles_all=pd.read_csv('all_persons all.csv')
df_profiles_all.drop(columns=['Unnamed: 0'],inplace=True)
df_profiles_all_links=pd.read_csv('all_persons_links all.csv')
df_profiles_all_links.drop(columns=['Unnamed: 0'],inplace=True)
df_publishers_all=pd.read_csv('all_publishers all.csv')
df_publishers_all.drop(columns=['Unnamed: 0'],inplace=True)
df_publishers_link=pd.read_csv('all_publishers_links all.csv')
df_publishers_link.drop(columns=['Unnamed: 0'],inplace=True)
df_tags=pd.read_csv('tags all.csv')
df_tags.drop(columns=['Unnamed: 0'],inplace=True)

async def get_person(soup,url):
    name=soup.find_all('h1')[0].text.strip()
    head=soup.find_all('h5')
    linkid=unquote(url).split('/')[-1]
    try:
        desc=head[0].text.strip()
    except:
        print('no desc')
        desc=None
    try:
        like=int(soup.find(class_='btn-group').text)
    except:
        like=None
        print('no like')
    df_profiles_all.loc[df_profiles_all.name.str.fullmatch(name),'desc']=desc
    df_profiles_all.loc[df_profiles_all.name.str.fullmatch(name),'like']=like
    df_profiles_all.loc[df_profiles_all.name.str.fullmatch(name),'linkid']=linkid
    return 0

exap_url=[]

semaphore = asyncio.Semaphore(60)
async def fetch(session, url):
    try:
        async with semaphore:
            async with session.get(url) as response:
                response_text = await response.text()
                print(f"Response from {url}: {len(response_text)} bytes")
                return response_text
    except aiohttp.ClientError as e:
        exap_url.append(url)
        print(f"Aiohttp ClientError while fetching {url}: {e}")
    except Exception as e:
        exap_url.append(url)
        print(f"Unexpected error while fetching {url}: {e}")

count = []
connector = aiohttp.TCPConnector(limit=60)
timeout = aiohttp.ClientTimeout(total=30)
async def scrape_person(url):
    async with aiohttp.ClientSession() as session:
        try:
            html = await fetch(session, url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                await get_person(soup,url)
                print(len(count))
                count.append(True)
                return 1
            else:
                print(f"Empty response from {url}")
                return 0
        except Exception as e:
            print(f"Error while scraping {url}: {e}")
            return 0

df_profiles_all['desc']='No desc'
df_profiles_all['like']='No like'
df_profiles_all['linkid']='No link'

urls=(df_profiles_all_links['link'].drop_duplicates()).values
nest_asyncio.apply()
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
loop = asyncio.get_event_loop()
batch_size = 500
for j in range(0, len(urls), batch_size):
    batch = urls[j:j + batch_size]
    results = loop.run_until_complete(asyncio.gather(*(scrape_person(url) for url in batch)))
    time.sleep(5)
books['publishers']=books['publishers'].apply(lambda x : x.replace(']','').replace('[','').replace('\'',''))
df_publishers_all['linkid']=df_publishers_link['link'].apply(lambda x:unquote(x.split('/')[-1]))
df_profiles_all.to_csv('profiles_all_links.csv')
df_publishers_all.to_csv('publishers_all_links.csv')

# Retrieving lost person information

async def get_person(soup,url):
    bs=soup.find_all(class_='product-container well clearfix')[0].find_all(class_='col-md-9 col-sm-9')
    for i in bs:
        try:
            product_table=i.find(class_='product-table').find_all('tr')
        except:product_table=[]
        book_id=None
        try:
            for n in range(len(product_table)):
                if 'کد' in product_table[n].text:
                    aut=i.find_all(class_='col-xs-12 prodoct-attribute-items')[1].text.split(':')[1].strip()
                    print(aut,i.find(class_='product-name').text.strip())
        except:print('o')
    books.loc[books.book_id==url,'author']=aut
#     df_profiles_all.loc[df_profiles_all.name.str.fullmatch(name),'like']=like
#     df_profiles_all.loc[df_profiles_all.name.str.fullmatch(name),'linkid']=linkid
    return 0

exab_url=[]

semaphore = asyncio.Semaphore(60)
async def fetch(session, url):
    try:
        async with semaphore:
            async with session.get(url) as response:
                response_text = await response.text()
                print(f"Response from {url}: {len(response_text)} bytes")
                return response_text
    except aiohttp.ClientError as e:
        exab_url.append(url)
        print(f"Aiohttp ClientError while fetching {url}: {e}")
    except Exception as e:
        exab_url.append(url)
        print(f"Unexpected error while fetching {url}: {e}")

count = []
connector = aiohttp.TCPConnector(limit=60)
timeout = aiohttp.ClientTimeout(total=30)
async def scrape_person(url):
    async with aiohttp.ClientSession() as session:
        try:
            html = await fetch(session, 'https://www.iranketab.ir/book/'+str(url))
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                await get_person(soup,url)
                print(len(count))
                count.append(True)
                return 1
            else:
                print(f"Empty response from {url}")
                return 0
        except Exception as e:
            print(f"Error while scraping {url}: {e}")
            return 0

urls=(books.loc[books.author=='[]']['book_id'].values)
nest_asyncio.apply()
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
loop = asyncio.get_event_loop()
batch_size = 500
for j in range(0, len(urls), batch_size):
    batch = urls[j:j + batch_size]
    results = loop.run_until_complete(asyncio.gather(*(scrape_person(url) for url in batch)))
    time.sleep(5)

books.to_csv(f'books all.csv')

