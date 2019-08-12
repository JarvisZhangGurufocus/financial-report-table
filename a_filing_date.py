from mysql import MySqlHelper
from elastic import ElasticHelper

mysqlHelper = MySqlHelper()
elasticHelper = ElasticHelper()

table_ids = mysqlHelper.query('SELECT table_id FROM filing_tables WHERE filing_date is NULL')
table_ids = [ x['table_id'] for x in table_ids ]

for table_id in table_ids:
  print table_id
  table = elasticHelper.getTable(table_id, 'table_id')
  mysqlHelper.execute("UPDATE filing_tables SET filing_date = '%s' WHERE table_id = '%s'" % ( table['_source']['filing_date'] ))
