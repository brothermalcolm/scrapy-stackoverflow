# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from os.path import abspath
from pyspark.sql import SparkSession

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

class StackscraperPipeline:
    # how do i pass tablename specs ds etc. into this object?
    def __init__(self, ds):
        # self.tablename = tablename
        self.ds = ds
        self.file = open('./data/questions_%s.json' % (self.ds), 'w')
        # establish db connection
        self.spark = SparkSession \
                    .builder \
                    .appName("demo webscraping spark job") \
                    .config("spark.sql.warehouse.dir", warehouse_location) \
                    .enableHiveSupport() \
                    .getOrCreate()
        set_sql = "SET hive.exec.dynamic.partition.mode=nonstrict"
        self.spark.sql(set_sql)
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        # self.dropTable()
        # self.spark.sql("show tables").show()
        self.createTable()
        self.spark.sql("show tables").show()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            # tablename=crawler.settings.get('tablename'),
            ds=crawler.settings.get('ds')
        )

    def createTable(self):
        create_sql = "CREATE TABLE IF NOT EXISTS questions (text varchar(1000),link varchar(1000),id int) PARTITIONED BY (ds DATE)" 
        print(create_sql)
        self.spark.sql(create_sql)


    def process_item(self, item, spider):
        # method 1: json dump
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        # method 2: hive store
        print("eeny, meeny, miney, moe... ", spider.name)
        insert_sql = "INSERT INTO questions PARTITION (ds = '%s') VALUES ('%s','%s',%s)" \
            % (self.ds, item['text'], item['link'], item['id'])
        self.spark.sql(insert_sql)
        return item
