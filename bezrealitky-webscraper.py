import requests
import bs4
import time
import random
import sys
import json
import pandas as pd
from datetime import date
from github import Github
import logging
import smtplib
from email.message import EmailMessage

logging.basicConfig(level=logging.INFO, filename='debug.log', filemode='w', format='%(asctime)s: %(message)s')

try:
    import os
    from dotenv import load_dotenv
    load_dotenv()
    logging.info('Loaded environmental variables from .env file.')
except:
    logging.info('Could not load the .env file. However will try to load API key from environmental variable.')

try:
    GMAIL_PASS = os.environ['GMAIL_PASS']
    GITHUB_API_KEY = os.environ['GITHUB_API_KEY']
    g = Github(GITHUB_API_KEY)
    logging.info('Connected to GitHub via API key sucesfully.')
except:
    logging.info("Cannot connect to GitHub. Arborting.")
    sys.exit("Arborting.")
    

HEADERS = {
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

URL = "https://www.bezrealitky.com/listings/offer-rent/flat/prague?page={page_no}"

class Page:
    def __init__(self,url):
        time.sleep(random.randrange(1,4))
        self.url = url
        self.response = requests.get(self.url,headers=HEADERS)
        self.soup = bs4.BeautifulSoup(self.response.text,"lxml")
    
    def send_email_report(self):
        self.email = EmailMessage()
        self.email['from'] = 'Python'
        self.email['to'] = 't1.bialy@gmail.com'
        self.email['subject'] = 'Heroku task failure.'
        self.email.set_content("Bezrealitky webscraper failed. Debugging files attached.")
        self.email.add_attachment(self.response.text,filename='debug.html')
        with open('debug.log','r') as file:
            self.email.add_attachment(file.read(), filename='debug.log')
    
        with smtplib.SMTP(host='smtp.gmail.com', port=587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login('sesq.python@gmail.com', GMAIL_PASS)
            smtp.send_message(self.email)

class ListingPage(Page):
    def get_max_pages(self):
        self.max_pages = int(self.soup.find_all("a",class_='page-link')[-2].text)
        return self.max_pages

    def get_listing_urls(self):
        self.listings_in_page = self.soup.find_all("article",class_='PropertyCard_propertyCard__qPQRK propertyCard PropertyCard_propertyCard--landscape__7grmL')
        self.apartment_urls = [listing.find("a",href=True).get("href") for listing in self.listings_in_page]
        return self.apartment_urls     

class Apartment(Page):
    def get_parameters(self):
        self.price_table = self.soup.find('table',{"class": "PriceTable_priceTable__voQsR priceTable"})
        self.price_parameters = [instance.find("th").text.replace('Basic rent','Price').replace('Utility fees','Fees') for instance in self.price_table.find_all("tr")]
        self.price_values = [instance.find("strong").text.replace('\xa0',' ') for instance in self.price_table.find_all("tr")]

        self.characteristics_table = self.soup.find('section',{"class" : "box Section_section___TusU section mb-5 mb-lg-10"})
        self.characteristics_parameters = [instance.find("th").text for instance in self.characteristics_table.find_all("tr")]
        self.characteristics_values = [instance.find("td").text for instance in self.characteristics_table.find_all("tr")]

        self.additional_information = self.soup.find_all("div",{"class" : "ParamsTable_paramsTable__Eqwx_ paramsTable"})[1]
        self.additional_parameters = [instance.text for instance in self.additional_information.find_all("td") if instance.text.find('MHD') == -1 and instance.text.find('Front garden') == -1]

        self.gps_json = json.loads(self.soup.find("script",{"id" : "__NEXT_DATA__"}).contents[0])

        self.poi_json = json.loads(self.gps_json["props"]["pageProps"]["advert"]["poiData"])
        
        if self.poi_json is not None:
            self.poi_names = [key for key in self.poi_json]
            self.poi_values = [self.poi_json[key]['properties']['walkDistance'] for key in self.poi_names]
        else:  
            self.poi_names = []
            self.poi_values = []

        self.p = self.price_parameters + self.characteristics_parameters + self.additional_parameters + self.poi_names
        self.v = self.price_values + self.characteristics_values + [1]*len(self.additional_parameters) + self.poi_values

        self.dictionary_data = dict(zip(self.p,self.v))

        self.dictionary_data['District'] = self.soup.find("span",{"class" : "PropertyAttributes_propertyAttributesItem__kscom"}).text

        self.dictionary_data['Latitude'] = self.gps_json["props"]["pageProps"]["advert"]["gps"]["lat"]
        self.dictionary_data['Longitude'] = self.gps_json["props"]["pageProps"]["advert"]["gps"]["lng"]

        return self.dictionary_data

current_page = 1
max_pages = 999
df = pd.DataFrame()

while current_page <= max_pages:
    wait_time = 45
    while True:
        try:
            listing_page_url = URL.format(page_no=current_page)
            listing_page_ = ListingPage(url=listing_page_url)
            apartment_url_list = listing_page_.get_listing_urls()
            max_pages = listing_page_.get_max_pages()
            logging.info(f'Parsing page {current_page} of {max_pages}')
            print(f'Parsing page {current_page} of {max_pages}')
        except:
            wait_time = wait_time*2
            if wait_time >= 1200:
                listing_page_.send_email_report()
                sys.exit("Too many trials. Arborting the script.")
            else:
                logging.info(f'Error during parsing the listing page: {listing_page_url} at {current_page} page. Waiting {int(wait_time/60)} minutes and will try again')
                time.sleep(wait_time)
            continue
        break


    for apartment_url in apartment_url_list:
        while True:
            try:
                apartment_ = Apartment(url=apartment_url)
                apartment_parameters = apartment_.get_parameters()
                df_ = pd.DataFrame([apartment_parameters])
                df = pd.concat([df,df_],axis=0,ignore_index=True)
            except:
                wait_time = wait_time*2
                if wait_time >= 1200:
                    logging.exception(f'Too many unsuccessful trials during parsing the apartment: {apartment_url} at {current_page} page.')
                    apartment_.send_email_report()
                    sys.exit("Arborting the script.")
                else:
                    logging.info(f'Error during parsing the apartment: {apartment_url} at {current_page} page. Waiting {int(wait_time/60)} minutes and will try again')
                    time.sleep(wait_time)
                continue
            break

    current_page = current_page + 1

repository = g.get_user().get_repo('Estate-rental-market-in-Prague')
filename = date.today().strftime("data-rental/Prague-%Y-%m-%d.csv")
content = df.to_csv(encoding='utf-8',index=False)
f = repository.create_file(filename, "Uploaded with PyGithub, deployed via Heroku", content)
logging.info('Finished successfully.')