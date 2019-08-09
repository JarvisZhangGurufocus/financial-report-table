import math

from mysql import MySqlHelper
from html import HtmlHelper
from utils import Utils

mysqlHelper = MySqlHelper()
htmlHelper = HtmlHelper()
utils = Utils()

def isInRange(value, compare):
  if not compare or not compare:
    return True
  value = float(value)
  compare = float(compare)
  return abs( max(value, compare) / min(value, compare) ) < 5

def getCellByTag(morn_comp_id, tag_ids):
  cell_ids = -1
  for tag_id in tag_ids:
    ids = mysqlHelper.query('''
      SELECT cell_id FROM report_cell_attrs WHERE
      attr_id in (
        SELECT attr_id
        FROM report_attr_tags
        WHERE tag_id = {tag_id}
      )
    '''.format(
      tag_id = tag_id
    ))
    ids = set([ x['cell_id'] for x in ids ])
    if cell_ids == -1:
      cell_ids = ids
    else:
      cell_ids = cell_ids.intersection(ids)

  if not cell_ids:
    return []

  cell_ids = [str(x) for x in cell_ids]
  cells = mysqlHelper.query('''
    SELECT {cell_table}.*, {table}.morn_comp_id
    FROM {cell_table} 
    JOIN {table} ON {cell_table}.table_id = {table}.table_id
    WHERE {cell_table}.id in ({ids}) AND morn_comp_id = '{morn_comp_id}'
    ORDER BY {cell_table}.date
  '''.format(
    table = mysqlHelper.table,
    cell_table = mysqlHelper.cellTable,
    ids = ','.join(cell_ids),
    morn_comp_id = morn_comp_id
  ))
  cells = [ x for x in cells if utils.isNumber(x['value']) ]
  return cells

def getTable(morn_comp_id, table):
  tableCells = []
  for row in table['rows']:
    tableCells.append(getCellByTag(morn_comp_id, row['tags']))
  
  tableDates = []
  for row in tableCells:
    for cell in row:
      tableDates.append(cell['date'])
  tableDates = list(set(tableDates))
  tableDates.sort()

  matrix = [
    [table['name']] + [ x for x in tableDates ]
  ]

  for i in range(len(table['rows'])):
    row = table['rows'][i]
    rowData = [row['name']]
    for date in tableDates:
      found = False
      for cell in tableCells[i]:
        if found:
          break
        if cell['date'] != date:
          continue
        if len(rowData) > 1 and cell['value'] == rowData[len(rowData) - 1]:
          continue
        if len(rowData) > 1 and not isInRange(cell['value'], rowData[len(rowData) - 1]):
          continue
        rowData.append(cell['value'])
        found = True
      if not found:
        rowData.append('')
    matrix.append(rowData)
  
  return matrix

def saveTable():
  HTML = ''
  for table in tables:
    array = getTable(morn_comp_id, table)
    HTML += htmlHelper.array2Table(array)
  utils.saveFile('TEST.htm', '''
    <html>
      <head>
        <style>
          table {
            margin-top: 20px;
          }
          table, th, td {
            border: 1px solid black;
          }
        </style>
      <head>
      <body>
        %s
      </body>
    </html>
  ''' % HTML)

tables = [
  # 0
  {
    'name': 'Impaired loans and troubled debt restructurings / Allowance for loan and lease losses',
    'rows': [
      { 'name': 'Consumer Real Estate', 'tags': [1895, 73, 3429] },
      { 'name': 'Credit Card And Other Consumer', 'tags': [1895, 73, 3430] },
      { 'name': 'Commercial', 'tags': [1895, 73, 1179] }
    ]
  },
  # 1
  {
    'name': 'Impaired loans and troubled debt restructurings / Carrying value',
    'rows': [
      { 'name': 'Consumer Real Estate', 'tags': [1895, 244, 3429] },
      { 'name': 'Credit Card And Other Consumer', 'tags': [1895, 244, 3430] },
      { 'name': 'Commercial', 'tags': [1895, 244, 1179] }
    ]
  },
  # 2
  {
    'name': 'Impaired loans and troubled debt restructurings / Allowance as a percentage of carrying value',
    'rows': [
      { 'name': 'Consumer Real Estate', 'tags': [1895, 1896, 3429] },
      { 'name': 'Credit Card And Other Consumer', 'tags': [1895, 1896, 3430] },
      { 'name': 'Commercial', 'tags': [1895, 1896, 1179] }
    ]
  },
  # 3
  {
    'name': 'Loans collectively evaluated for impairment / Allowance for loan and lease losses',
    'rows': [
      { 'name': 'Consumer Real Estate', 'tags': [1897, 1896, 3429] },
      { 'name': 'Credit Card And Other Consumer', 'tags': [1897, 1896, 3430] },
      { 'name': 'Commercial', 'tags': [1897, 1896, 1179] }
    ]
  },
  # 4
  {
    'name': 'Loans collectively evaluated for impairment / Carrying value',
    'rows': [
      { 'name': 'Consumer Real Estate', 'tags': [1897, 244, 3429] },
      { 'name': 'Credit Card And Other Consumer', 'tags': [1897, 244, 3430] },
      { 'name': 'Commercial', 'tags': [1897, 244, 1179] }
    ]
  },
  # 5
  {
    'name': 'Loans collectively evaluated for impairment / Allowance as a percentage of carrying value',
    'rows': [
      { 'name': 'Consumer Real Estate', 'tags': [1897, 1896, 3429] },
      { 'name': 'Credit Card And Other Consumer', 'tags': [1897, 1896, 3430] },
      { 'name': 'Commercial', 'tags': [1897, 1896, 1179] }
    ]
  },
  # 6
  {
    'name': 'VIEs',
    'rows': [
      { 'name': 'Maximum loss exposure Consolidated', 'tags': [362, 3438, 3437] },
      { 'name': 'Maximum loss exposure Unconsolidated', 'tags': [362, 3438, 3439] },
      { 'name': 'Trading account assetsConsolidated', 'tags': [11, 3438, 3437] },
      { 'name': 'Trading account assets Unconsolidated', 'tags': [11, 3438, 3439] },
      { 'name': 'Debt securities carried at fair value Consolidated', 'tags': [1772, 3438, 3437] },
      { 'name': 'Debt securities carried at fair value Unconsolidated', 'tags': [1772, 3438, 3439] },
      { 'name': 'Loans and leasesConsolidated', 'tags': [167, 3438, 3437] },
      { 'name': 'Loans and leases Unconsolidated', 'tags': [167, 3438, 3439] },
      { 'name': 'Allowance for loan and lease losses Consolidated', 'tags': [73, 3438, 3437] },
      { 'name': 'Allowance for loan and lease losses Unconsolidated', 'tags': [73, 3438, 3439] },
      { 'name': 'Long-term debt Consolidated', 'tags': [18, 3438, 3437] },
      { 'name': 'Long-term debt Unconsolidated', 'tags': [18, 3438, 3439] },
      { 'name': 'All other liabilities Consolidated', 'tags': [372, 3438, 3437] },
      { 'name': 'All other liabilities Unconsolidated', 'tags': [372, 3438, 3439] }
    ]
  },
  # 9
  {
    'name': 'Long-term Debt by Major Currency',
    'rows':[
      { 'name': 'U.s. Dollar', 'tags': [1249, 3264]},
      { 'name': 'Euros', 'tags': [1250, 3264] },
      { 'name': 'British Pound', 'tags': [1251, 3264] },
      { 'name': 'Japanese Yen', 'tags': [1252, 3264] },
      { 'name': 'Canadian Dollar', 'tags': [1253, 3264] },
      { 'name': 'Australian Dollar', 'tags': [1254, 3264] },
      { 'name': 'Total long-term debt', 'tags': [402, 3264] }
    ]
  }
]

morn_comp_id = '0C000006U3'
saveTable()