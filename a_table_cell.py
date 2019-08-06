from elastic import ElasticHelper
from html import HtmlHelper
from mysql import MySqlHelper
from utils import Utils

elasticHelper = ElasticHelper()
htmlHelper = HtmlHelper()
mysqlHelper = MySqlHelper()
utils = Utils()

def handleStock(morn_comp_id):
  print "handleStock %s" % morn_comp_id
  ids = elasticHelper.getStockTables(morn_comp_id)
  print "  find %s tables" % len(ids)
  for id in ids:
    handleTable(id)

def handleTable(id):
  print "  get table %s" % id
  table = elasticHelper.getTable(id)
  exist = mysqlHelper.searchCell({'table_id': table['_source']['table_id']})
  if exist:
    return
  print "    get cells %s" % id
  cells = htmlHelper.getTableCells(table['_source']['content'])
  for cell in cells:
    cell['table_id'] = table['_source']['table_id']
    if not cell['date']:
      cell['date'] = table['_source']['report_period']
    mysqlHelper.saveCell(cell)

handleStock("0C00000ADA")