import unittest
# import pipeline
import main as pipeline
import pandas as pd

class testPipeline(unittest.TestCase):
    specfile = './specs/testformat1.csv'
    tablename = 'testformat1'
    spec = pd.read_csv(specfile)
    ds = '2021-07-07'

    def test_parseSpec(self):
        result = pipeline.parseSpec(self.specfile)
        self.assertEqual(result.shape, self.spec.shape)

    def test_dropTable(self):
        result = pipeline.dropTable(self.tablename)
        expect = "DROP TABLE IF EXISTS testformat1"
        self.assertEqual(result, expect)

    def test_convertType(self):
        self.assertEqual(pipeline.convertType('name', 'TEXT', 10), ('name', 'varchar', '(10),'))
        self.assertEqual(pipeline.convertType('valid', 'BOOLEAN', 1), ('valid', 'boolean', ','))
        self.assertEqual(pipeline.convertType('count', 'INTEGER', 3), ('count', 'int', ','))

    def test_createTable(self):
        result = pipeline.createTable(self.tablename, self.spec)
        expect = "CREATE EXTERNAL TABLE testformat1 (name varchar(10),valid boolean,count int) PARTITIONED BY (ds DATE) LOCATION 's3://clover-20220625/hive' "
        self.assertEqual(result, expect)

    def test_parseData(self):
        self.assertEqual(pipeline.parseData('Diabetes  1 1', self.spec), ['Diabetes', '1', '1'])
        self.assertEqual(pipeline.parseData('Asthma,   0-14', self.spec), ['Asthma', '0', '-14'])
        self.assertEqual(pipeline.parseData('  Stroke  1122', self.spec), ['Stroke', '1', '122'])

    def test_insertTable(self):
        self.assertEqual(pipeline.insertTable(self.tablename, ['Diabetes', '1', '1'], self.ds), "INSERT INTO testformat1 PARTITION (ds = '2021-07-07') VALUES ('Diabetes', '1', '1')")
        self.assertEqual(pipeline.insertTable(self.tablename, ['Asthma', '0', '-14'], self.ds), "INSERT INTO testformat1 PARTITION (ds = '2021-07-07') VALUES ('Asthma', '0', '-14')")
        self.assertEqual(pipeline.insertTable(self.tablename, ['Stroke', '1', '122'], self.ds), "INSERT INTO testformat1 PARTITION (ds = '2021-07-07') VALUES ('Stroke', '1', '122')")

if __name__ == '__main__':
    unittest.main()