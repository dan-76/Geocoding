# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import json
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst
from random import shuffle
from os.path import join, abspath

try:
    import urlparse
    from urllib import urlencode
except: # For Python 3
    import urllib.parse as urlparse
    from urllib.parse import urlencode

res_per_second = 10
limit_per_day = 500
geogle_api_key = "<API KEY HERE>"
target_today = []
output_directory = './'

def folder_file_to_abs_path(*paths):
    return abspath(join('./',*paths))

def load_csv_col_to_list(folder_name,file_name,col_name):
    path_csv = folder_file_to_abs_path(folder_name,file_name)
    df = pd.DataFrame.from_csv(path_csv,sep=',',index_col=None)
    full_target_list = df[col_name].dropna().unique().tolist()
    shuffle(full_target_list)
    return full_target_list

def url_with_params(url,params={}):
    url_parts = list(urlparse.urlparse(url))
    query = dict(urlparse.parse_qsl(url_parts[4]))
    query.update(params)
    url_parts[4] = urlencode(query)
    return urlparse.urlunparse(url_parts)

def address_to_url(address, api=geogle_api_key):
    endpoint = "https://maps.googleapis.com/maps/api/geocode/json"
    parms = [('address', address.encode("utf-8")), ('sensor', 'false'), ('key', api)]
    return url_with_params(endpoint,dict(parms))


class PropertiesItem(Item):
    # Primary fields
    address = Field(output_processor=TakeFirst())

    # Calculated fields
    geocode = Field(output_processor=TakeFirst())

    # Housekeeping fields
    url = Field(output_processor=TakeFirst())
    spider = Field(output_processor=TakeFirst())
    date = Field(output_processor=TakeFirst())

class JsonWriterPipeline(object):
    def __init__(self):
        self.file = open(folder_file_to_abs_path(output_directory,'geoitems.jl'),
                         'ab')
        
    def close_spider(self, spider):
        self.file.close()
        
    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        return item

class GeocodeSpider(scrapy.Spider):
    name = "geocode"

    def start_requests(self):
        address_list = target_today
        
        urls = list(map(address_to_url,address_list))
        for url, address in zip(urls, address_list):
            request = scrapy.Request(url=url, callback=self.parse)
            request.meta['address'] = address
            yield request

    def parse(self, response):
        l = ItemLoader(item=PropertiesItem(), response=response)
        
        address = response.meta['address']
        content = json.loads(response.body_as_unicode())
        if content['status'] == 'OK':
            geo = content['results'][0]["geometry"]["location"]
        
            l.add_value('address', address)
            l.add_value('geocode', {"lat": geo["lat"], "lon": geo["lng"]})
            
            l.add_value('url', response.url)
            l.add_value('spider', self.name)
            l.add_value('date', str(datetime.datetime.now()))
            
            yield l.load_item()
            
        else:
            errmsg = 'Unexpected status="%s" for address="%s"' %(content['status'], address)
            self.logger.error(errmsg)
            print(errmsg)

def initialize_scrapy_settings():
    # default settings from scrapy
    settings = Settings()
    
    settings.set("ROBOTSTXT_OBEY", False)
    #Control by download limit per second
    settings.set("DOWNLOAD_DELAY", 1. / float(res_per_second))
    settings.set('ITEM_PIPELINES', {'__main__.JsonWriterPipeline': 100})
    return settings

def run_scrapy():
    settings = initialize_scrapy_settings()
    process = CrawlerProcess(settings)
    process.crawl(GeocodeSpider)
    process.start()

def main(input_folder,input_file,input_col_name,output_folder):
    global target_today, output_directory
    target_today = load_csv_col_to_list(input_folder,
                                        input_file,
                                        input_col_name)[:limit_per_day]
    output_directory = folder_file_to_abs_path(output_folder)
    run_scrapy()

if __name__ == "__main__":
    main(input_folder='datasets',
         input_file='address.csv',
         input_col_name='full_address', 
         output_folder='')