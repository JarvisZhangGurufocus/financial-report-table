from mysql import MySqlHelper
from elastic import ElasticHelper

mysqlHelper = MySqlHelper()
elasticHelper = ElasticHelper()

table_ids = mysqlHelper.query('SELECT table_id FROM filing_tables WHERE filing_date is NULL')
table_ids = [ x['table_id'] for x in table_ids ]

report_filing_date = {}

for table_id in table_ids:
  print table_id
  report_id = table_id.split(':')[0]
  if report_id not in report_filing_date.keys():
    report = elasticHelper.getReport(report_id, 'document_id')
    report_filing_date[report_id] = report['_source']['filling_date']
  filing_date = report_filing_date[report_id]
  print filing_date
  mysqlHelper.execute("UPDATE filing_tables SET filing_date = '%s' WHERE table_id = '%s'" % ( filing_date, table_id ))
