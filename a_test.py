import os
import re
import hashlib
import numpy

from mysql import MySqlHelper
from elastic import ElasticHelper
from html import HtmlHelper
from utils import Utils
from env import env

env = env()
utils = Utils()
mySqlHelper = MySqlHelper()
elasticHelper = ElasticHelper()
htmlHelper = HtmlHelper()

HTML = ''
tables = mySqlHelper.query('SELECT id, table_id from report_tables')
for table in tables:
  print 'TABLE %s' % table['id']
  mTable = mySqlHelper.query("SELECT * FROM report_tables WHERE id = '%s'" % table['id'])[0]
  eTable = elasticHelper.getTable(table['table_id'], 'table_id')
  
  content = eTable['_source']['content']
  content = htmlHelper.htmlSoup(content)
  content = htmlHelper.table2Array(content)
  content = htmlHelper.array2Table(content)

  HTML += '''
    <div style="clear: both;height: 20px; background: #000; color: #FFF;">{table_id}</div>
    <div> section: {section} </div>
    <div> context: {context} </div>
    <div> primary_tags: {primary_tags} </div>
    <div> secondary_tags: {secondary_tags} </div>
    <div> other_tags: {other_tags} </div>
    {content}
  '''.format(
    table_id = table['id'],
    section = mTable['section'],
    context = mTable['context'],
    primary_tags = mTable['primary_tags'],
    secondary_tags = mTable['secondary_tags'],
    other_tags = mTable['other_tags'],
    content = content
  )

utils.saveFile('document.htm', '''
  <html>
    <header>
      table, th, td {
        border: 1px solid black;
      }
    </header>
    <body>
      %s
    </body>
  </html>
''' % HTML)