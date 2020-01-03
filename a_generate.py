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

    handledStocks = self.mysqlHelper.query('SELECT distinct morn_comp_id from %s' % self.mysqlHelper.table)
    self.handled_stocks = [x['morn_comp_id'] for x in handledStocks if x]
    self.handled_stocks = self.handled_stocks[:-5]

    handledTables = self.mysqlHelper.query('SELECT distinct table_id from %s' % self.mysqlHelper.table)
    self.handled_tables = [x['table_id'] for x in handledTables if x]
    
    handledReports = [x[:x.index(':')] for x in self.handled_tables if ':' in x]
    self.handled_reports = handledReports
    self.handled_reports = self.handled_reports[:-5]

    self.log = self.utils.setupLogger('generator', 'logs/logs', logging.DEBUG)

  def start(self, morn_comp_ids):
    for morn_comp_id in morn_comp_ids:
      if morn_comp_id in self.handled_stocks:
        self.log('Generator %s skip stock %s' % (self.workerId, morn_comp_id))
        continue
      self.handleStock(morn_comp_id)
      self.log.info('Generator %s handle stock %s' % (self.workerId, morn_comp_id))

  def handleStock(self, morn_comp_id):
    report_ids = self.elasticHelper.getStockReports(morn_comp_id)
    for report_id in report_ids:
      if report_id in self.handled_reports:
        self.log('Generator %s skip reprt %s' % (self.workerId, report_id))
        continue
      self.handleReport(report_id)
      self.log.info('Generator %s handle reprt %s' % (self.workerId, report_id))
  
  def handleReport(self, report_id):
    resultTables = []

    report = self.elasticHelper.getReport(report_id)
    soup = self.htmlHelper.htmlSoup(report['_source']['content'])
    nodes = self.htmlHelper.pluckNode(soup)
    tables = [x for x in soup.find_all("table") if 'id' in x.attrs.keys()]
    tableIdxs = [ nodes.index(table) for table in tables if table in nodes ]

    for table in tables:
      table_id = str(report['_source']['document_id']) + ':' + table.attrs['id']
      if table_id in self.handled_tables:
        self.log('Generator %s skip table %s' % (self.workerId, table_id))
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
      self.log.info('Generator %s handle table %s' % (self.workerId, table_id))