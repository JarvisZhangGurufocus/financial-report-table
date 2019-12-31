import threading
import time
import pymysql.cursors

from a_generate import Generator
from mysql import MySqlHelper
from env import env

class GenerarteThread (threading.Thread):
  def __init__(self, workerId, ids):
    threading.Thread.__init__(self)
    self.ids = ids
    self.workerId = workerId
  
  def run (self):
    Generator(self.workerId).start(self.ids)

env = env()
connection = pymysql.connect(host=env['DB_HOST'], port=int(env['DB_PORT']), user=env['DB_USER'],password=env['DB_PWD'],db='gurufocu_main')
cur = connection.cursor(pymysql.cursors.DictCursor)
cur.execute('SELECT distinct morn_comp_id from stock_list where morn_comp_id is not null;')

ids = []
for row in cur:
  ids.append(row['morn_comp_id'])
offset = int(len(ids) / 3)

worker = 1
threads = []
while len(ids) > 0:
  fragment = ids[:offset]
  thread = GenerarteThread(worker, fragment)
  threads.append(thread)
  ids = ids[offset + 1:]
  worker += 1

for thread in threads:
  thread.start()


# truncate gurufocu_data.filing_attrs;
# truncate gurufocu_data.filing_attr_tags;
# truncate gurufocu_data.filing_cell_attrs;
# truncate gurufocu_data.filing_cells;
# truncate gurufocu_data.filing_tables;
# truncate gurufocu_data.filing_tags;

# nohup python -u index.py > logs/nohup 2>&1 &
# 3555