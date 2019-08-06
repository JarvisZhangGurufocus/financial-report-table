import re
import pymysql.cursors
import json

from utils import Utils
from dateutil.parser import parse
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from env import env

env = env()
utils = Utils()
stopworddic = set(stopwords.words('english')) 
porterStemmer = PorterStemmer()

class MySqlHelper:
  def __init__(self):
    self.dbHost = env['DB_HOST'] "173.192.96.34"
    self.dbPort = env['DB_PORT']
    self.dbUser = env['DB_USER']
    self.dbPWD = env['DB_PWD']
    self.db = env['DB']

    self.cellTable = 'report_cells'
    self.attrTable = 'report_attrs'
    self.tagTable = 'report_tags'
    self.cellAttrTable = 'report_cell_attrs'
    self.attrTagTable = 'report_attr_tags'

    self.connection = pymysql.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser,password=self.dbPWD,db=self.db)

  def query(self, sql):
    # print '        %s' % sql
    cur = self.connection.cursor(pymysql.cursors.DictCursor)
    cur.execute(sql)
    res = []
    for row in cur:
      res.append(row)
    return res

  def execute(self, sql):
    # print '        %s' % sql
    cur = self.connection.cursor(pymysql.cursors.DictCursor)
    cur.execute(sql)
    id = cur.lastrowid
    self.connection.commit()
    return id

  def searchCell(self, cell):
    WHERES = '1 '
    for key in cell.keys():
      if key == 'date':
        cell[key] = parse(cell[key]).strftime("%Y-%m-%d")
      if type(cell[key]) is not list and type(cell[key]) is not dict:
        WHERES += "AND %s = '%s' " % (key, cell[key])
    SQL = 'SELECT * FROM %s WHERE %s' % (self.cellTable, WHERES)
    return self.query(SQL)
  
  def saveCell(self, cell):
    if cell is None:
      return
    saved = self.searchCell(cell)
    if saved:
      return saved[0]
    SQL = "INSERT INTO %s (value, date, table_id) VALUES ('%s','%s','%s')" % (
      self.cellTable, cell['value'], parse(cell['date']).strftime("%Y-%m-%d"), cell['table_id'])
    cellId = self.execute(SQL)
    if 'attrs' in cell.keys():
      for attr in cell['attrs']:
        if attr['value']:
          attr = self.saveAttr(attr)
          cellAttr = self.saveCellAttr({'cell_id': cellId, 'attr_id': attr['id']})
    
    cell['id'] = cellId
    return cell

  def searchAttr(self, attr):
    WHERES = '1 '
    for key in attr.keys():
      if type(attr[key]) is not list and type(attr[key]) is not dict:
        WHERES += "AND %s = '%s' " % (key, attr[key])
    SQL = 'SELECT * FROM %s WHERE %s' % (self.attrTable, WHERES)
    return self.query(SQL)

  def saveAttr(self, attr):
    if attr is None:
      return
    saved = self.searchAttr(attr)
    if saved:
      return saved[0]

    tag = {'value': attr['value'], 'type': attr['type'], 'label':attr['value']}
    tag['value'] = ' '.join([i for i in tag['value'].split() if i.lower() not in stopworddic])
    
    date = utils.isDate(attr['value'])
    if date:
      tag['value'] = date
      tag['label'] = date
    else:
      tag['value'] = re.sub('[^A-Za-z0-9]+', ' ', tag['value'])
      tag['value'] = ' '.join([porterStemmer.stem(i) for i in tag['value'].split()])
      tag['value'] = ' '.join([i for i in tag['value'].split() if len(i) > 1 ])

    SQL = "INSERT INTO %s (value, type) VALUES ('%s', '%s')" % (self.attrTable, attr['value'], attr['type'])
    attrId = self.execute(SQL)
    
    if tag['value']:
      tag = self.saveTag(tag)
      attrTag = self.saveAttrTag({'attr_id': attrId, 'tag_id': tag['id']})

    attr['id'] = attrId
    return attr

  def searchTag(self, tag):
    WHERES = '1 '
    for key in tag.keys():
      if key != 'label' and type(tag[key]) is not list and type(tag[key]) is not dict:
        WHERES += "AND %s = '%s' " % (key, tag[key])
    SQL = 'SELECT * FROM %s WHERE %s' % (self.tagTable, WHERES)
    return self.query(SQL)

  def saveTag(self, tag):
    if tag is None:
      return
    saved = self.searchTag(tag)
    if saved:
      return saved[0]
    
    SQL = "INSERT INTO %s (value, type, label) VALUES ('%s', '%s', '%s')" % (self.tagTable, tag['value'], tag['type'], tag['label'])
    tagId = self.execute(SQL)

    tag['id'] = tagId
    return tag

  def searchCellAttr(self, cellAttr):
    WHERES = '1 '
    for key in cellAttr.keys():
      if type(cellAttr[key]) is not list and type(cellAttr[key]) is not dict:
        WHERES += "AND %s = '%s' " % (key, cellAttr[key])
    SQL = 'SELECT * FROM %s WHERE %s' % (self.cellAttrTable, WHERES)
    return self.query(SQL)

  def saveCellAttr(self, cellAttr):
    if cellAttr is None:
      return
    saved = self.searchCellAttr(cellAttr)
    if saved:
      return saved[0]
    
    SQL = "INSERT INTO %s (cell_id, attr_id) VALUES ('%s', '%s')" % (self.cellAttrTable, cellAttr['cell_id'], cellAttr['attr_id'])
    cellAttrId = self.execute(SQL)

    cellAttr['id'] = cellAttrId
    return cellAttr
  
  def searchAttrTag(self, attrTag):
    WHERES = '1 '
    for key in attrTag.keys():
      if type(attrTag[key]) is not list and type(attrTag[key]) is not dict:
        WHERES += "AND %s = '%s' " % (key, attrTag[key])
    SQL = 'SELECT * FROM %s WHERE %s' % (self.attrTagTable, WHERES)
    return self.query(SQL)

  def saveAttrTag(self, attrTag):
    if attrTag is None:
      return
    saved = self.searchAttrTag(attrTag)
    if saved:
      return saved[0]
    
    SQL = "INSERT INTO %s (attr_id, tag_id) VALUES ('%s', '%s')" % (self.attrTagTable, attrTag['attr_id'], attrTag['tag_id'])
    attrTagId = self.execute(SQL)

    attrTag['id'] = attrTagId
    return attrTag