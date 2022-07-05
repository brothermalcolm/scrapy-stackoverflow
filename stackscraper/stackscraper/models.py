import pandas as pd
import os
from os.path import abspath
from pyspark.sql import SparkSession
import re

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

def dbConnect():
    # establish db connection
    spark = SparkSession \
                .builder \
                .appName("demo webscraping spark job") \
                .config("spark.sql.warehouse.dir", warehouse_location) \
                .enableHiveSupport() \
                .getOrCreate()
    set_sql = "SET hive.exec.dynamic.partition.mode=nonstrict"
    spark.sql(set_sql)
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    return spark

# parse spec csv fields
def parseSpec(specfile: str) -> pd.DataFrame:
    print('parse spec csv fields...')
    print(os.path.basename(specfile))
    filename = os.path.basename(specfile)
    spec = pd.read_csv(specfile)
    return spec

def dropTable(tablename) -> str:
    drop_sql = "DROP TABLE IF EXISTS %s" % (tablename)
    print(drop_sql)
    return drop_sql

# translate to hql
def convertType(name, type, width):
    name = name.lower().strip()
    type = type.lower().strip().replace('text','varchar').replace('integer','int')
    if type in ('int','boolean'):
        width = ','
    else:
        width = '(' + str(width).strip() + '),'
    return name, type, width

def createTable(tablename, spec) -> str:
    columns = ''
    # print(spec)
    for i in range(len(spec)):
        print(spec.iloc[i,0], spec.iloc[i,2], spec.iloc[i,1])
        name, type, width = convertType(spec.iloc[i,0], spec.iloc[i,2], spec.iloc[i,1])
        columns += name + ' ' + type + width 
    # print(columns)
    create_sql = "CREATE TABLE IF NOT EXISTS %s (%s) PARTITIONED BY (ds DATE) " % (tablename, columns[:-1])
    print(create_sql)
    return create_sql

def insertTable(tablename, values: list, pkey: str) -> str:
    values = tuple(values)
    print(values)
    insert_sql = "INSERT INTO %s PARTITION (ds = '%s') VALUES %s" % (tablename, pkey, str(values))
    print(insert_sql)
    return insert_sql

def insertView(tablename, ds):
    # insert df into existing table
    insert_sql = "INSERT INTO %s PARTITION (ds = '%s') SELECT * FROM global_temp.%s" % (tablename, ds, tablename)
    print(insert_sql)
    return insert_sql

def insertOverwrite(tablename, ds):
    # insert overwrite df into existing table
    # insert_sql = "INSERT OVERWRITE %s PARTITION (ds = '%s') SELECT * FROM global_temp.%s" % (tablename, ds, tablename)
    insert_sql = "INSERT OVERWRITE %s PARTITION (ds = '%s') SELECT text, link, int(id) FROM global_temp.%s" % (tablename, ds, tablename)
    # insert_sql = "INSERT OVERWRITE %s PARTITION (ds = '%s') SELECT text, link, int(id) FROM %s" % (tablename, ds, tablename)
    print(insert_sql)
    return insert_sql

def selectTable(tablename):
    select_sql = "SELECT * FROM %s" % (tablename)
    print(select_sql)
    return select_sql 

def checkTable(tablename, ds):
    # select_sql = "SELECT COUNT(*) AS num_rows FROM %s WHERE DS = '%s'" % (tablename, ds)
    select_sql = "SELECT DS, COUNT(*) AS num_rows FROM %s GROUP BY DS" % (tablename)
    print(select_sql)
    return select_sql 
