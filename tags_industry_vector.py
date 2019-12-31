import yake
import json

from mysql import MySqlHelper
from utils import Utils

kw_extractor = yake.KeywordExtractor()

tagsCount = 134665

utils = Utils()

mysqlHelper = MySqlHelper()
mysqlHelper.setConnection(db="gurufocu_main")

groupCodes = mysqlHelper.query('SELECT distinct groupcode from morn_industry')
groupCodes = [ x['groupcode'] for x in groupCodes ]

vector = []

for groupCode in groupCodes:
  row = [ 0 for x in range(0, tagsCount) ]
  tags = mysqlHelper.query('''
    SELECT  filing_tags.id, count(*) as count
    FROM gurufocu_data.filing_tables 
    JOIN stock_list ON filing_tables.morn_comp_id = stock_list.morn_comp_id
    JOIN morn_industry ON stock_list.industry = morn_industry.industrycode
    JOIN gurufocu_data.filing_cells ON filing_tables.table_id = filing_cells.table_id
    RIGHT JOIN gurufocu_data.filing_cell_attrs ON filing_cells.id = filing_cell_attrs.cell_id
    JOIN gurufocu_data.filing_attrs ON filing_attrs.id = filing_cell_attrs.attr_id
    JOIN gurufocu_data.filing_attr_tags ON filing_attr_tags.attr_id = filing_attrs.id
    JOIN gurufocu_data.filing_tags ON filing_attr_tags.tag_id = filing_tags.id
    WHERE morn_industry.groupcode = %s AND gurufocu_data.filing_tags.id < %s
    GROUP BY filing_tags.value
  ''' % (groupCode, tagsCount))
  for tag in tags:
    row[tag['id'] - 1] = tag['count']
  vector.append(row)

  utils.saveFile('industries/%s' % groupCode, json.dumps(row))

  print(groupCode)
  print(row)

utils.saveFile('industries/vector', json.dumps(vector))
  

