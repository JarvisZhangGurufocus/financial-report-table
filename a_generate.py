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
    self.handled_stocks = utils.readFile('logs/stocks').split('\n')
    self.handled_reports = utils.readFile('logs/reports').split('\n')
    self.handled_tables = utils.readFile('logs/tables').split('\n')
    self.handled_stocks = [x for x in set(self.handled_stocks) if x]
    self.handled_reports = [x for x in set(self.handled_reports) if x]
    self.handled_tables = [x for x in set(self.handled_tables) if x]

  def log(self, file, content):
    file = 'logs/%s' % file
    utils.appendFile(file, content)

  def start(self, morn_comp_ids):
    self.log('logs', 'STARTED %s' % time.ctime())
    self.log('logs', '%s STOCKS FINISHED' % str(len(self.handled_stocks)))
    self.log('logs', '%s REPORTS FINISHED' % str(len(self.handled_reports)))
    self.log('logs', '%s TABLES FINISHED' % str(len(self.handled_tables)))
    for morn_comp_id in morn_comp_ids:
      self.handleStock(morn_comp_id)
      self.log('stocks', morn_comp_id)
    self.log('logs', 'DONE')

  def handleStock(self, morn_comp_id):
    report_ids = elasticHelper.getStockReports(morn_comp_id)
    for report_id in report_ids:
      self.handleReport(report_id)
      self.log('reports', report_id)
  
  def handleReport(self, report_id):
    if report_id in self.handled_reports:
      return

    resultTables = []

    report = elasticHelper.getReport(report_id)
    soup = htmlHelper.htmlSoup(report['_source']['content'])
    nodes = htmlHelper.pluckNode(soup)
    tables = [x for x in soup.find_all("table") if 'id' in x.attrs.keys()]
    tableIdxs = [ nodes.index(table) for table in tables ]

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

      self.log('tables', table_id)

Generator().start([
  # '0C000006U3','0C000009HV','0C000009L0','0C00000ADA','0C00000XW0','0C00000ZJQ','0C00001OH5','0C00008K5V'
  # '0C000006SM', '0C000006SM', '0C000006SO', '0C000006SP', '0C000006SQ', '0C000006SR', '0C000006SS', '0C000006ST' 
    '0C000006SU', '0C000006SV', '0C000006SW', '0C000006SX', '0C000006SY', '0C000006SZ', '0C000006T0', '0C000006T1', 
    '0C000006T2', '0C000006T3', '0C000006T4', '0C000006T5', '0C000006T6', '0C000006T7', '0C000006T8', '0C000006T9', 
    '0C000006TA', '0C000006TB', '0C000006TC', '0C000006TD', '0C000006TE', '0C000006TF', '0C000006TG', '0C000006TH', 
    '0C000006TI', '0C000006TJ', '0C000006TK', '0C000006TL', '0C000006TM', '0C000006TN', '0C000006TO', '0C000006TP', 
    '0C000006TQ', '0C000006TR', '0C000006TS', '0C000006TT', '0C000006TU', '0C000006TV', '0C000006TW', '0C000006TX', 
    '0C000006TY', '0C000006TZ', '0C000006U0', '0C000006U1', '0C000006U2', '0C000006U3', '0C000006U4', '0C000006U5', 
    '0C000006U6', '0C000006U7', '0C000006U8', '0C000006U9', '0C000006UA', '0C000006UB', '0C000006UC', '0C000006UD', 
    '0C000006UE', '0C000006UF', '0C000006UG', '0C000006UH', '0C000006UI', '0C000006UJ', '0C000006UK', '0C000006UL', 
    '0C000006UM'
])