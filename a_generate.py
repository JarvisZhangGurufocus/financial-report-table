import re
import string
import time
import logging

from elastic import ElasticHelper
from htm import HtmlHelper
from mysql import MySqlHelper
from utils import Utils

printable = set(string.printable)
logging.basicConfig(level = logging.DEBUG, filename = 'logs/logs', format = '%(message)s', filemode = 'w')

class Generator:
  def __init__(self, id):
    self.workerId = id
    
    self.elasticHelper = ElasticHelper()
    self.htmlHelper = HtmlHelper()
    self.mysqlHelper = MySqlHelper()
    self.utils = Utils()

    self.handled_stocks = self.utils.readFile('logs/stocks').split('\n')
    self.handled_reports = self.utils.readFile('logs/reports').split('\n')
    self.handled_tables = self.utils.readFile('logs/tables').split('\n')
    self.handled_stocks = [x for x in set(self.handled_stocks) if x]
    self.handled_reports = [x for x in set(self.handled_reports) if x]
    self.handled_tables = [x for x in set(self.handled_tables) if x]

    self.log = self.utils.setupLogger('generator', 'logs/logs', logging.DEBUG)
    self.stockLog = self.utils.setupLogger('stocks', 'logs/stocks', logging.DEBUG)
    self.reportLog = self.utils.setupLogger('reports', 'logs/reports', logging.DEBUG)
    self.tableLog = self.utils.setupLogger('tables', 'logs/tables', logging.DEBUG)

  def start(self, morn_comp_ids):
    self.log.info('GENERATOR %s STARTED %s' % (self.workerId, time.ctime()))
    self.log.info('%s STOCKS FINISHED' % str(len(self.handled_stocks)))
    self.log.info('%s REPORTS FINISHED' % str(len(self.handled_reports)))
    self.log.info('%s TABLES FINISHED' % str(len(self.handled_tables)))
    for morn_comp_id in morn_comp_ids:
      self.handleStock(morn_comp_id)
      self.stockLog.info(morn_comp_id)
      self.log.info('Generator %s finish stock %s' % (self.workerId, morn_comp_id))
    self.log.info('DONE %s' % self.workerId)

  def handleStock(self, morn_comp_id):
    report_ids = self.elasticHelper.getStockReports(morn_comp_id)
    for report_id in report_ids:
      self.handleReport(report_id)
      self.reportLog.info(report_id)
      self.log.info('Generator %s finish report %s' % (self.workerId, report_id))
  
  def handleReport(self, report_id):
    if report_id in self.handled_reports:
      return

    resultTables = []

    report = self.elasticHelper.getReport(report_id)
    soup = self.htmlHelper.htmlSoup(report['_source']['content'])
    nodes = self.htmlHelper.pluckNode(soup)
    tables = [x for x in soup.find_all("table") if 'id' in x.attrs.keys()]
    tableIdxs = [ nodes.index(table) for table in tables if table in nodes ]

    for table in tables:
      table_id = str(report['_source']['document_id']) + ':' + table.attrs['id']
      if table_id in self.handled_tables:
        continue
      if not table in nodes:
        continue

      index = nodes.index(table)

      context = self.htmlHelper.getTableContext(nodes, index)
      context = '. '.join(context)
      context = self.utils.strEncode(context)
      
      section = self.htmlHelper.getSection(nodes, index)
      section = self.utils.strEncode(section)

      primary_tags = set()
      secondary_tags = set()
      other_tags = set()

      cells = self.htmlHelper.getTableCells(table)

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
        self.mysqlHelper.saveCell(cell)
      primary_tags = ';'.join([self.utils.strEncode(x) for x in primary_tags])
      secondary_tags = ';'.join([self.utils.strEncode(x) for x in secondary_tags])
      other_tags = ';'.join([self.utils.strEncode(x) for x in other_tags])

      self.mysqlHelper.saveTable({
        'table_id': table_id,
        'primary_tags': primary_tags, 'secondary_tags': secondary_tags, 'other_tags': other_tags, 
        'context': context, 'section': section, 'morn_comp_id': report['_source']['morn_comp_id'],
        'document_type': report['_source']['document_type'],
        'filing_date': report['_source']['filling_date']
      })

      self.tableLog.info(table_id)
      self.log.info('Generator %s finish table %s' % (self.workerId, table_id))