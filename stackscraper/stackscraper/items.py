# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose
import re

def getId(str):
    return int(re.findall(r'^/questions/(\d+)/', str)[0])

class StackscraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    text = scrapy.Field(
        # input_processor=None,
        output_processor=TakeFirst()
    )
    link = scrapy.Field(
        # input_processor=None,
        output_processor=TakeFirst()
    )
    id = scrapy.Field(
        input_processor = MapCompose(getId), 
        output_processor=TakeFirst()
    )
