# parse data txt file according to spec csv file
import pandas as pd
import os
from glob import glob
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import Row
import re

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

# glob all new files
specfiles = glob("./specs/*.csv") 
datafiles = glob("./data/*.json")
print(specfiles, datafiles)

# establish db connection
spark = SparkSession \
    .builder \
    .appName("demo webscraping spark job") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()
set_sql = "SET hive.exec.dynamic.partition.mode=nonstrict"
# set_sql = "SET hive.exec.dynamic.partition=True"
spark.sql(set_sql)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

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
    create_sql = "CREATE TABLE %s (%s) PARTITIONED BY (ds DATE) " % (tablename, columns[:-1])
    print(create_sql)
    return create_sql

def parseData(line: str, spec) -> list:
    row = line.rstrip()
    # parse row iteratively
    print('parse row iteratively...')
    i = 0
    fields = []
    for col in range(len(spec)):
        w = spec.iloc[col,1]
        if spec.iloc[col,2] == 'INTEGER':
            try:
                field = int(row[i:i+w].strip())
            except:
                field = None
        elif spec.iloc[col,2] == 'BOOLEAN':
            field = int(row[i:i+w].strip()) > 0
        else:
            field = row[i:i+w].strip().replace(',','')
        col += 1
        i += w
        fields.append(field)
        print(fields)
    return fields

def pandasData(datafile, spec) -> pd.DataFrame:
    print('parse fwf data txt file using pandas')
    i = 0
    colspecs = []
    for w in spec['width']:
        colspecs.append((i,i+w))
        i += w
    names = spec["column name"]
    data = pd.read_fwf(datafile, colspecs=colspecs, names=names)
    print(data.head())
    return data

def loadData(datafile, spec):
    print('load dataframe...')
    # pdf = pandasData(datafile, spec)
    # sdf = spark.createDataFrame(pdf)
    sdf = spark.read.json(datafile, mode='DROPMALFORMED')
    sdf.show()
    # print(sdf.tail(5))
    print(sdf)
    spark.catalog.dropGlobalTempView(tablename)
    sdf.createGlobalTempView(tablename)
    # sdf.createOrReplaceTempView(tablename)
    # spark.sql('select * from global_temp.%s' % tablename).show()

def sparkData(datafile, spec):
    pass

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

if __name__ == '__main__':
    # parse data txt file (new only)
    print('parse data txt file...')
    # rundates = ['2022-07-01','2022-07-02','2022-07-03','2022-07-04','2022-07-05','2022-07-06','2022-07-07']
    rundate = '2022-07-01'
    for datafile in datafiles:
        print('datafiles loop...')

        # check datafile partition (new only)
        re1 = r'(\d{4}-\d{2}-\d{2})\.json$'
        ds = re.findall(re1, datafile)[0]
        print(ds)
        if ds == rundate:
            print('new datafile found...')
        else:
            continue

        # find matching spec
        re2 = r'\/(\w+)_\d{4}-\d{2}-\d{2}\.json$'
        specfile = './specs/' + re.findall(re2, datafile)[0] + '.csv' 
        print(specfile)
        try:
            spec = parseSpec(specfile)
        except:
            print('warning: spec does not exist!')
            break

        # create table (new only)
        print('create table...')
        tablename = re.findall(re2, datafile)[0]
        spark.sql("show databases").show()
        # spark.sql(dropTable(tablename))
        # spark.sql("show tables").show()
        # spark.sql(createTable(tablename, spec))
        spark.sql("show tables").show()

        # load data file
        print('load data file...')
        loadData(datafile, spec)

        # insert data file  
        # spark.sql(insertView(tablename, ds))
        spark.sql(insertOverwrite(tablename, ds))

        # select table
        spark.sql(selectTable(tablename)).show()

        # qc table
        spark.sql(checkTable(tablename, ds)).show()    
