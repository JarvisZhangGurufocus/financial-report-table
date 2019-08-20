import re
import string
import time

from elastic import ElasticHelper
from html import HtmlHelper
from mysql import MySqlHelper
from utils import Utils

elasticHelper = ElasticHelper()
htmlHelper = HtmlHelper()
mysqlHelper = MySqlHelper()
utils = Utils()
printable = set(string.printable)

class Generator:
  def __init__(self):
    self.handled_stocks = []
    self.handled_reports = []
    self.handled_tables = []

    logs = utils.readFile('logs/generate').split('\n')
    for log in logs:
      if 'HANDLE STOCK' in log:
        self.handled_stocks.append(log.split('HANDLE STOCK')[1].strip())
      if 'HANDLE REPORT' in log:
        self.handled_reports.append(log.split('HANDLE REPORT')[1].strip())
      elif 'CELLS IN TABLE' in log:
        self.handled_tables.append(log.split('CELLS IN TABLE')[1].strip())

    if len(self.handled_stocks) > 0:
      self.handled_stocks.pop()
    if len(self.handled_reports) > 0:
      self.handled_reports.pop()
    if len(self.handled_tables) > 0:
      self.handled_tables.pop()
    
    self.handled_stocks = [x for x in set(self.handled_stocks)]
    self.handled_reports = [x for x in set(self.handled_reports)]
    self.handled_tables = [x for x in set(self.handled_tables)]

  def start(self, morn_comp_ids):
    self.log('STARTED %s' % time.ctime())
    self.log('%s STOCKS FINISHED' % str(len(self.handled_stocks)))
    self.log('%s REPORTS FINISHED' % str(len(self.handled_reports)))
    self.log('%s TABLES FINISHED' % str(len(self.handled_tables)))
    for morn_comp_id in morn_comp_ids:
      self.handleStock(morn_comp_id)

  def log(self, content):
    utils.appendFile('logs/generate', content)

  def handleStock(self, morn_comp_id):
    self.log('HANDLE STOCK %s' % morn_comp_id)
    report_ids = elasticHelper.getStockReports(morn_comp_id)
    for report_id in report_ids:
      self.handleReport(report_id)
  
  def handleReport(self, report_id):
    if report_id in self.handled_reports:
      return

    self.log('HANDLE REPORT %s' % report_id)
    resultTables = []

    report = elasticHelper.getReport(report_id)
    soup = htmlHelper.htmlSoup(report['_source']['content'])
    nodes = htmlHelper.pluckNode(soup)
    tables = [x for x in soup.find_all("table") if 'id' in x.attrs.keys()]
    tableIdxs = [ nodes.index(table) for table in tables ]

    self.log('   %s TABLE in REPORT' % str(len(tables)))
    for table in tables:
      table_id = str(report['_source']['document_id']) + ':' + table.attrs['id']
      if table_id in self.handled_tables:
        continue

      index = nodes.index(table)

      context = htmlHelper.getTableContext(nodes, index)
      context = '. '.join(context)
      context = utils.strEncode(context)
      
      section = htmlHelper.getSection(nodes, index)
      section = utils.strEncode(section)

      primary_tags = set()
      secondary_tags = set()
      other_tags = set()

      cells = htmlHelper.getTableCells(table)

      self.log('     %s CELLS IN TABLE %s' % (len(cells), table_id))
      for cell in cells:
        cell['table_id'] = table_id
        if not cell['date']:
          cell['date'] = report['_source']['report_period']
        for attr in cell['attrs']:
          if attr['type'] == 'primary':
            primary_tags.add(attr['value'])
          if attr['type'] == 'secondary':
            secondary_tags.add(attr['value'])
          if attr['type'] == 'other':
            other_tags.add(attr['value'])
        mysqlHelper.saveCell(cell)
      primary_tags = ';'.join([utils.strEncode(x) for x in primary_tags])
      secondary_tags = ';'.join([utils.strEncode(x) for x in secondary_tags])
      other_tags = ';'.join([utils.strEncode(x) for x in other_tags])

      mysqlHelper.saveTable({
        'table_id': table_id,
        'primary_tags': primary_tags, 'secondary_tags': secondary_tags, 'other_tags': other_tags, 
        'context': context, 'section': section, 'morn_comp_id': report['_source']['morn_comp_id'],
        'document_type': report['_source']['document_type'],
        'filing_date': report['_source']['filling_date']
      })

Generator().start([
  '0C000006U3','0C000009HV','0C000009L0','0C00000ADA','0C00000XW0','0C00000ZJQ','0C00001OH5','0C00008K5V'
  '0C000006SM', '0C000006SM', '0C000006SO', '0C000006SP', '0C000006SQ', '0C000006SR', '0C000006SS', '0C000006ST'
])