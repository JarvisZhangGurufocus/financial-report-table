from mysql import MySqlHelper
from elastic import ElasticHelper

mysqlHelper = MySqlHelper()
elasticHelper = ElasticHelper()

reportMaps = {}
tables = mysqlHelper.query('SELECT * FROM %s WHERE morn_comp_id is null and document_type is null' % mysqlHelper.table)

for table in tables:
  print table['id']
  report_id = table['table_id'].split(':')[0]
  if report_id in reportMaps.keys():
    report = reportMaps[report_id]
  else:
    report = elasticHelper.getReport(report_id, 'document_id')
    reportMaps[report_id] = report
  
  mysqlHelper.execute('''
    UPDATE {table} SET morn_comp_id = '{morn_comp_id}', document_type = '{document_type}' WHERE id = '{id}'
  '''.format(
    id = table['id'],
    table = mysqlHelper.table,
    morn_comp_id = report['_source']['morn_comp_id'],
    document_type = report['_source']['document_type']
  ))