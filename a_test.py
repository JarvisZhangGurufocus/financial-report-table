from mysql import MySqlHelper
from elastic import ElasticHelper
from html import HtmlHelper

mysqlHelper = MySqlHelper()
elasticHelper = ElasticHelper()
htmlHelper = HtmlHelper()

def getTableCells(table_id):
  cells = mysqlHelper.query('''
    SELECT report_cells.id, value, date, GROUP_CONCAT(tag_id) as tag_ids FROM report_cells
    JOIN report_cell_attrs ON report_cells.id = report_cell_attrs.cell_id
    JOIN report_attr_tags ON report_cell_attrs.attr_id = report_attr_tags.attr_id
    WHERE table_id = '{table_id}'
    GROUP BY report_cells.id
  '''.format(
    table_id = table_id
  ))

  tag_ids = []
  for cell in cells:
    cell['tag_ids'] = cell['tag_ids'].split(',')
    tag_ids += cell['tag_ids']
  tag_ids = [x for x in set(tag_ids)]

  if len(tag_ids) == 0:
    return []

  tags = mysqlHelper.query('''
    SELECT * FROM report_tags
    WHERE id in ({ids})
  '''.format(
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
        cell['p_tags'].append('%s (%s)' % (tag['label'], tag['id']))
      if tag['type'] == 'secondary':
        cell['s_tags'].append('%s (%s)' % (tag['label'], tag['id']))
      if tag['type'] == 'other':
        cell['o_tags'].append('%s (%s)' % (tag['label'], tag['id']))

  return cells

def getCellByTag(tag_ids):
  cell_ids = []
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
    ids = [ x['cell_id'] for x in ids ]
    if len(cell_ids) == 0:
      cell_ids = ids
    else:
      cell_ids = list(set(cell_ids).intersection(set(ids)))
  cell_ids = [str(x) for x in cell_ids]
  return mysqlHelper.query('SELECT * FROM report_cells WHERE id in (%s)' % ','.join(cell_ids))

# table = elasticHelper.getTable('1713062:0.1.1.1.1.1.2.3.734.2.8.0.0', 'table_id')
# cells = htmlHelper.getTableCells(table['_source']['content'])


cells = getTableCells('1745567:6.3.391.0.0')
for cell in cells:
  print cell

  # print '===='
  # print cell
  # for tag in cell['p_tags']:
  #   print tag
  # for tag in cell['s_tags']:
  #   print tag
  # for tag in cell['o_tags']:
  #   print tag
  # print ''
  # print '%s %s' % (cell['date'], cell['value'])
  # similarCells = getCellByTag(cell['tag_ids'])
  # for similarCell in similarCells:
  #   print '%s %s' % (similarCell['date'], similarCell['value'])