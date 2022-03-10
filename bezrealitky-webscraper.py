import requests
import bs4
import time
import random
import numpy as np
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
    print('Could not load API key from the .env file.')

df = pd.DataFrame(columns=['Listing ID','Price','Latitude','Longitude','District'])

current_page = 1
max_pages = 999

while current_page <= max_pages:
    while True:
        try:  
            time.sleep(random.randrange(1,2))
            url = f'https://www.bezrealitky.com/listings/offer-rent/flat/praha?&page={current_page}'
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
            response = requests.get(url,headers=headers)
            soup = bs4.BeautifulSoup(response.text,"lxml")

            max_pages = int(soup.find_all("a",class_='page-link pagination__page')[-2].text)
            listings_in_page = soup.find_all(class_='product__link')
        except:
            print('Error during parsing the data. Waiting 3 minutes and will try again')
            time.sleep(180)
            continue
        break

    print(f'Scraping page {current_page} of {max_pages}')

    for apartment in listings_in_page:
        while True:
            try:
                time.sleep(random.randrange(1,2))
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
                print('Error during parsing the data. Waiting 3 minutes and will try again')
                time.sleep(180)
                continue
            break
    current_page += 1 


g = Github(GITHUB_API_KEY)
repository = g.get_user().get_repo('Estate-rental-market-in-Prague')
filename = date.today().strftime("data-rental/Prague-%Y-%m-%d.csv")
content = df.to_csv(encoding='utf-8',index=False)
f = repository.create_file(filename, "Uploaded with PyGithub, deployed via Heroku", content)