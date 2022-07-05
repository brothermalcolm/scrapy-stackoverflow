# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from pyspark.sql import SparkSession
from stackscraper.models import dbConnect, dropTable, createTable, parseSpec, convertType, insertTable

class StackscraperPipeline:
    def __init__(self, ds):
        # self.tablename = tablename
        self.tablename = 'questions'
        specfile = './specs/' + self.tablename + '.csv' 
        spec = parseSpec(specfile)
        self.ds = ds
        self.file = open('./data/questions_%s.json' % (self.ds), 'w')
        self.spark = dbConnect()
        # self.spark.sql(dropTable(tablename))
        # self.spark.sql("show tables").show()
        self.spark.sql(createTable(self.tablename, spec))
        self.spark.sql("show tables").show()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            # tablename=crawler.settings.get('tablename'),
            ds=crawler.settings.get('ds')
        )

    def process_item(self, item, spider):
        # method 1: json dump
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        # method 2: hive store
        print("eeny, meeny, miney, moe... ", spider.name)
        fields = [item['text'], item['link'], item['id']]
        self.spark.sql(insertTable(self.tablename, fields, self.ds))
        return item
