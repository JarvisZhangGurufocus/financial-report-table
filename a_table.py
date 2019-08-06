
import re

from mysql import MySqlHelper
from utils import Utils

utils = Utils()
mySqlHelper = MySqlHelper()

tableIds = mySqlHelper.query('SELECT DISTINCT table_id FROM %s' % mySqlHelper.cellTable)
tableIds = [ x['table_id'] for x in tableIds ]

for tableId in tableIds:
  print 'TABLE %s' % tableId
  tags = mySqlHelper.query('''
    SELECT * FROM report_tags WHERE id IN (
      SELECT tag_id FROM report_attr_tags WHERE attr_id in (
        SELECT attr_id FROM report_cell_attrs WHERE cell_id in (
          SELECT id FROM gurufocu_data.report_cells WHERE table_id = '{table_id}' 
        )
      )
    ) AND type != 'table'
  '''.format(
    table_id=tableId
  ))
  
  tags = filter(lambda x: re.sub('[^A-Za-z]+', '', x['value']).strip() and not utils.isDate(x['value']), tags)
  primary_tags = '; '.join([x['label'] for x in tags if x['type'] == 'primary'])
  secondary_tags = '; '.join([x['label'] for x in tags if x['type'] == 'secondary'])
  other_tags = '; '.join([x['label'] for x in tags if x['type'] == 'other'])

  mySqlHelper.execute('''
    UPDATE report_tables set primary_tags = '{primary_tags}', secondary_tags = '{secondary_tags}', other_tags = '{other_tags}'
    WHERE table_id = '{table_id}'
  '''.format(
    primary_tags = mySqlHelper.connection.escape_string(primary_tags),
    secondary_tags = mySqlHelper.connection.escape_string(secondary_tags),
    other_tags = mySqlHelper.connection.escape_string(other_tags),
    table_id = tableId
  ))

