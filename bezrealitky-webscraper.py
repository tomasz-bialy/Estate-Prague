import requests
import bs4
import time
import random
import sys
import pandas as pd
from datetime import date
from github import Github

try:
    import os
    from dotenv import load_dotenv
    load_dotenv()
    GITHUB_API_KEY = os.environ['GITHUB_API_KEY']
    print('Loaded API key from .env file.')
except:
    print('Could not load the .env file. Loading API key from environmental variable.')
    GITHUB_API_KEY = os.environ['GITHUB_API_KEY']

df = pd.DataFrame(columns=['Listing ID','Price','Latitude','Longitude','District'])

current_page = 1
max_pages = 999
wait_time = 0

while current_page <= max_pages:
    while True:
        try:  
            time.sleep(random.randrange(1,4))
            url = f'https://www.bezrealitky.com/listings/offer-rent/flat/praha?&page={current_page}'
            headers =   {
                        'Connection': 'keep-alive',
                        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-User': '?1',
                        'Sec-Fetch-Dest': 'document',
                        'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,zh;q=0.5,de;q=0.4,cs;q=0.3,ja;q=0.2'
                        }
            response = requests.get(url,headers=headers)
            soup = bs4.BeautifulSoup(response.text,"lxml")

            max_pages = int(soup.find_all("a",class_='page-link pagination__page')[-2].text)
            listings_in_page = soup.find_all(class_='product__link')
            print(f'Parsing page {current_page} of {max_pages}')
        except:
            wait_time = wait_time+120
            print(f'Error during parsing page {current_page}. Waiting {wait_time/60} minutes and will try again')
            if wait_time >= 1800:
                sys.exit("Too many trials. Arborting the script.")
            else:
                time.sleep(wait_time)
            continue
        break

    for apartment in listings_in_page:
        while True:
            try:
                time.sleep(random.randrange(1,4))
                apartment_url = apartment.get('href')
                url2 = 'https://www.bezrealitky.com' + apartment_url
                response2 = requests.get(url2,headers=headers)
                soup2 = bs4.BeautifulSoup(response2.text,"lxml")
                
                parameters = soup2.find_all('div',{"class": "col col-6 param-title"})
                values = soup2.find_all('div',{"class": "col col-6 param-value"})
                parameters = [item.text.strip() for item in parameters]
                values = [item.text.strip() for item in values]
                
                parameters2 = soup2.find_all('div',{"class": "col poi-item__name"})
                values2 = soup2.find_all('div',{"class": "poi-item__walking"})
                parameters2 = [item.text.strip()[:-1] for item in parameters2]
                values2 = [item.find('strong').text.strip()[:-2] for item in values2]
                
                parameters = parameters + parameters2
                values = values + values2

                dictionary_data = dict(zip(parameters,values))
                
                dictionary_data['Latitude'] = soup2.find_all("div", {"class": "b-map__inner"})[0]['data-lat']
                dictionary_data['Longitude'] = soup2.find_all("div", {"class": "b-map__inner"})[0]['data-lng']
                dictionary_data['Listing ID'] = soup2.find('div',{'class':'col text-right align-self-center d-none d-lg-block'}).text.strip().replace('Listing ID: ','')
                dictionary_data['District'] = soup2.find('div',{"class": "col col-12 col-md-8"}).find("h2").text.strip()

                df_single_listing = pd.DataFrame([dictionary_data])
                df = pd.concat([df,df_single_listing],axis=0,ignore_index=True)
            except:
                wait_time = wait_time+120
                print(f'Error during parsing the url: {url2} at {current_page} page. Waiting {wait_time/60} minutes and will try again')
                if wait_time >= 1800:
                    sys.exit("Too many trials. Arborting the script.")
                else:
                    time.sleep(wait_time)
                continue
            break
    current_page += 1 


g = Github(GITHUB_API_KEY)
repository = g.get_user().get_repo('Estate-rental-market-in-Prague')
filename = date.today().strftime("data-rental/Prague-%Y-%m-%d.csv")
content = df.to_csv(encoding='utf-8',index=False)
f = repository.create_file(filename, "Uploaded with PyGithub, deployed via Heroku", content)