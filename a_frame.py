import math

from mysql import MySqlHelper
from html import HtmlHelper
from utils import Utils

mysqlHelper = MySqlHelper()
htmlHelper = HtmlHelper()
utils = Utils()

def handleStock(morn_comp_id):
  tables = mysqlHelper.query('SELECT table_id from %s' % mysqlHelper.table)
  table_ids = [ x['table_id'] for x in tables ]
  for table_id in table_ids:
    handleTable(table_id)

def handleTable(table_id):
  print 'TABLE %s' % table_id
  cells = mysqlHelper.query('''
    SELECT {cell_table}.id, value, date, GROUP_CONCAT(tag_id) as tag_ids FROM {cell_table}
    JOIN {cell_attr_table} ON {cell_table}.id = {cell_attr_table}.cell_id
    JOIN {attr_tag_table} ON {cell_attr_table}.attr_id = {attr_tag_table}.attr_id
    WHERE table_id = '{table_id}'
    GROUP BY {cell_table}.id
  '''.format(
    table_id = table_id,
    cell_table = mysqlHelper.cellTable,
    cell_attr_table = mysqlHelper.cellAttrTable,
    attr_tag_table = mysqlHelper.attrTagTable
  ))

  tag_ids = []
  for cell in cells:
    cell['tag_ids'] = cell['tag_ids'].split(',')
    cell['tag_ids'].sort()
    tag_ids += cell['tag_ids']
  tag_ids = [x for x in set(tag_ids)]

  if len(tag_ids) == 0:
    return []

  tags = mysqlHelper.query('''
    SELECT * FROM {tag_table}
    WHERE id in ({ids})
  '''.format(
    tag_table=mysqlHelper.tagTable,
    ids=','.join(tag_ids)
  ))

  for cell in cells:
    cell['p_tags'] = []
    cell['s_tags'] = []
    cell['o_tags'] = []
    for tag in tags:
      if not str(tag['id']) in cell['tag_ids']:
        continue
      if tag['type'] == 'primary':
        cell['p_tags'].append({'id': tag['id'], 'value': tag['label']})
      if tag['type'] == 'secondary':
        cell['s_tags'].append({'id': tag['id'], 'value': tag['label']})
      if tag['type'] == 'other':
        cell['o_tags'].append({'id': tag['id'], 'value': tag['label']})
    cell['o_tags_key'] = '-'.join(sorted([str(x['id']) for x in cell['o_tags']]))
    cell['s_tags_key'] = '-'.join(sorted([str(x['id']) for x in cell['s_tags']]))

  for other_group in groupBy(cells, 'o_tags_key'):
    for secondary_group in groupBy(other_group, 's_tags_key'):
      frame_name = ''
      for tag in secondary_group[0]['s_tags']:
        frame_name += tag['value'] + ' '
      for tag in secondary_group[0]['o_tags']:
        frame_name += tag['value'] + ' '
      frame_name = frame_name.strip()
      rowIds = []
      for cell in secondary_group:
        row_name = ''
        for tag in cell['p_tags']:
          row_name += tag['value'] + ' '
        row = mysqlHelper.saveFrameRow({'name': row_name, 'tags': ','.join(cell['tag_ids'])})
        rowIds.append(str(row['id']))
      
      rowIds = list(set(rowIds))
      rowIds.sort()
      
      mysqlHelper.saveFrame({'name': frame_name, 'rows': ','.join(rowIds)})
      

def groupBy(arr, key):
  res = {}
  for item in arr:
    if item[key] not in res.keys():
      res[item[key]] = []
    res[item[key]].append(item)
  return res.values()

handleStock('0C000006U3')