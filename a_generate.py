import re
import string

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
  def start(self, morn_comp_ids):
    print 'STARTED'
    for morn_comp_id in morn_comp_ids:
      self.handleStock(morn_comp_id)

  def handleStock(self, morn_comp_id):
    print 'HANDLE STOCK %s' % morn_comp_id
    report_ids = elasticHelper.getStockReports(morn_comp_id)
    for report_id in report_ids:
      self.handleReport(report_id)
  
  def handleReport(self, report_id):
    print 'HANDLE REPORT %s' % report_id
    resultTables = []

    report = elasticHelper.getReport(report_id)
    soup = htmlHelper.htmlSoup(report['_source']['content'])
    nodes = htmlHelper.pluckNode(soup)
    tables = [x for x in soup.find_all("table") if 'id' in x.attrs.keys()]
    tableIdxs = [ nodes.index(table) for table in tables ]

    print '   %s TABLE in REPORT' % str(len(tables))
    for table in tables:
      index = nodes.index(table)

      table_id = str(report['_source']['document_id']) + ':' + table.attrs['id']
      section = htmlHelper.getSection(nodes, index)
      section = ''.join([x for x in section if x in printable])
      
      context = htmlHelper.getTableContext(nodes, index)
      context = ';'.join([ utils.strEncode(x) for x in context ])

      primary_tags = set()
      secondary_tags = set()
      other_tags = set()

      cells = htmlHelper.getTableCells(table)
      print '     %s CELLS IN TABLE %s' % (len(cells), table_id)
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
      
      primary_tags = ';'.join([x for x in primary_tags])
      secondary_tags = ';'.join([x for x in secondary_tags])
      other_tags = ';'.join([x for x in other_tags])

      mysqlHelper.saveTable({
        'table_id': table_id,
        'primary_tags': primary_tags, 'secondary_tags': secondary_tags, 'other_tags': other_tags, 
        'context': context, 'section': section
      })



Generator().start(['0C000006U3','0C000009HV','0C000009L0','0C00000ADA','0C00000XW0','0C00000ZJQ','0C00001OH5','0C00008K5V'])

