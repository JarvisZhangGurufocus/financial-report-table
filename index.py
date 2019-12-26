import threading
import time
import pymysql.cursors

from a_generate import Generator
from mysql import MySqlHelper
from env import env

class GenerarteThread (threading.Thread):
  def __init__(self, ids):
    threading.Thread.__init__(self)
    self.ids = ids
  
  def run (self):
    Generator().start(self.ids)

env = env()
connection = pymysql.connect(host=env['DB_HOST'], port=int(env['DB_PORT']), user=env['DB_USER'],password=env['DB_PWD'],db='gurufocu_main')
cur = connection.cursor(pymysql.cursors.DictCursor)
cur.execute('SELECT distinct morn_comp_id from stock_list where morn_comp_id not null;')

ids = []
for row in cur:
  ids.append(row['morn_comp_id'])
offset = int(len(ids) / 20)

while len(ids) > 0:
  fragment = ids[:offset]
  thread = GenerarteThread(fragment)
  thread.start()
  ids = ids[offset + 1:]


# truncate gurufocu_data.filing_attrs;
# truncate gurufocu_data.filing_attr_tags;
# truncate gurufocu_data.filing_cell_attrs;
# truncate gurufocu_data.filing_cells;
# truncate gurufocu_data.filing_tables;
# truncate gurufocu_data.filing_tags;