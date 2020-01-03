import json

from mysql import MySqlHelper
from utils import Utils

utils = Utils()

mysqlHelper = MySqlHelper()
mysqlHelper.setConnection(db="gurufocu_main")

allTags = mysqlHelper.query('SELECT id from gurufocu_data.filing_tags where stop_words = 0')
allTags = [x['id'] for x in allTags]
tagsCount = len(allTags)
utils.saveFile('logs/allTags', json.dumps(allTags))

groupCodes = mysqlHelper.query('SELECT distinct groupcode from morn_industry')
groupCodes = [ x['groupcode'] for x in groupCodes ]

for groupCode in groupCodes:
  row = [ 0 for x in range(0, tagsCount) ]
  tags = mysqlHelper.query('''
    SELECT filing_tags.id, count(*) as count
    FROM gurufocu_data.filing_tables 
    JOIN stock_list ON filing_tables.morn_comp_id = stock_list.morn_comp_id
    JOIN morn_industry ON stock_list.industry = morn_industry.industrycode
    JOIN gurufocu_data.filing_cells ON filing_tables.table_id = filing_cells.table_id
    RIGHT JOIN gurufocu_data.filing_cell_attrs ON filing_cells.id = filing_cell_attrs.cell_id
    JOIN gurufocu_data.filing_attrs ON filing_attrs.id = filing_cell_attrs.attr_id
    JOIN gurufocu_data.filing_attr_tags ON filing_attr_tags.attr_id = filing_attrs.id
    JOIN gurufocu_data.filing_tags ON filing_attr_tags.tag_id = filing_tags.id
    WHERE morn_industry.groupcode = %s 
    GROUP BY filing_tags.value
  ''' % groupCode)
  
  for idx in range(0, tagsCount):
    for tag in tags:
      if tag['id'] == allTags[idx]:
        row[idx] = tag['count']

  utils.saveFile('industries/%s' % groupCode, json.dumps(row))

  print('====')
  print(groupCode)
  print(len(row))
  

