import re
import string
import hashlib

from elastic import ElasticHelper
from html import HtmlHelper
from mysql import MySqlHelper
from utils import Utils

from sklearn.externals import joblib

printable = set(string.printable)
elasticHelper = ElasticHelper()
htmlHelper = HtmlHelper()
mysqlHelper = MySqlHelper()
utils = Utils()

sectionVectorizer = joblib.load('pkls/vectorizer.section.pkl')
sectionTransformer = joblib.load('pkls/transformer.section.pkl')
sectionKnn = joblib.load('pkls/knn.section.pkl')

def handleStock(morn_comp_id):
  print "handleStock %s" % morn_comp_id
  ids = elasticHelper.getStockReports(morn_comp_id)
  print "  find %s reports" % len(ids)
  for id in ids:
    handleReport(id)

def handleReport(id):
  print "  get report %s" % id
  HTML = ''
  
  report = elasticHelper.getReport(id)
  soup = htmlHelper.htmlSoup(report['_source']['content'])
  nodes = dfsNode(soup)
  tables = [x for x in soup.find_all("table") if 'id' in x.attrs.keys()]
  tableIdxs = [ nodes.index(table) for table in tables ]
  preContext = ''
  for pos in range(len(tableIdxs)):
    table = tables[pos]
    
    index = tableIdxs[pos] - 1
    section = ''
    while index >= 0:
      node = nodes[index]
      if node.name != None and 'id' in node.attrs.keys() and node['id'] == 'temp-section':
        section = node.get_text()
        break
      index -= 1
    section = mysqlHelper.connection.escape_string(''.join([x for x in section if x in printable]))

    index = tableIdxs[pos] - 1
    context = ''
    while index >= 0:
      node = nodes[index]
      
      node_content = ''
      if node.name == 'table':
        index = -1
      elif node.name == None:
        node_content = unicode(node)
      else:
        node_content = node.get_text()

      score = 0
      if node.name == 'b':
        score += 1
      if node.name != None and 'id' in node.attrs.keys() and node['id'] == 'temp-section':
        score += 1
      if node.name != None and 'style' in node.attrs and node['style'].upper().find('FONT-WEIGHT') > -1:
        score += 1
      
      pieces = contentSplit(node_content)
      piecesIdx = len(pieces) - 1
      while piecesIdx >= 0:
        if pieces[piecesIdx].strip():
          piece_score = contentScore(pieces[piecesIdx]) + score
          if piece_score > 2:
            index = -1
            context = cleanContext(pieces[piecesIdx]) + ';' + context
          elif piece_score > 0 and len(context.split(';')) < 3:
            context = cleanContext(pieces[piecesIdx]) + ';' + context
        piecesIdx -= 1
      
      index -= 1
    
    if not context and pos > 0 and tableIdxs[pos] - tableIdxs[pos - 1] < 5:
      context = preContext
    
    preContext = context

    table_id = str(report['_source']['document_id']) + ':' + table.attrs['id']
    res = mysqlHelper.query("SELECT * FROM report_tables WHERE table_id = '%s'" % table_id)
    if len(res) == 0:
      mysqlHelper.execute("INSERT INTO report_tables (table_id, context, section) VALUES ('%s', '%s', '%s')" % (table_id, context, section))
    else:
      mysqlHelper.execute("UPDATE report_tables SET context = '%s', section = '%s' WHERE table_id='%s'" % (context, section, table_id))
    
    content = mysqlHelper.connection.escape_string(''.join([x for x in unicode(table) if x in printable]))
    utils.saveFile('tables/%s.html' % hashlib.md5(table_id).hexdigest(), content)
    
    print '   %s' % table_id
    print '       %s' % context
    print '       %s' % section

def dfsNode(node):
  if node.name == None or node.name == 'table':
    return [node]
  
  onlyFont = True
  for child in node.children:
    if node.name is None or node.name == 'span' or node.name == 'font':
      continue
    onlyFont = False
  if onlyFont and isSection(node.get_text()):
    node['id'] = 'temp-section'
    return [node]

  onlyString = True
  for child in node.children:
    if child.name is not None:
      onlyString = False
  if onlyString:
    return [node]
  
  nodes = []
  for child in node.children:
    nodes += dfsNode(child)
  return nodes

def isSection(content):
  if not re.match(r'^[ ]*ITEM(.*)', content, re.IGNORECASE) and not re.match(r'^[ ]*NOTE(.*)', content, re.IGNORECASE):
    return False
  content = content.upper().replace('ITEM', '').replace('ITEMS', '').replace('NOTE', '').replace('NOTES', '').replace(' ', '')
  content = re.sub('[^A-Za-z0-9]+', '', content)
  if content:
    return True
  return False

def cleanContext(context):
  context = filter(lambda x: x in printable, context)
  context = utils.strEncode(context)
  return context.strip()

def contentSplit(node_content):
  pieces = []
  last_index = 0
  for i in range(len(node_content)):
    if node_content[i] not in printable:
      continue
    if node_content[i] == '.' or node_content[i] == ';':
      pieces.append(node_content[last_index: i])
      last_index = i + 1
  pieces.append(node_content[last_index:])
  pieces = [x for x in pieces if x]
  return pieces

def contentScore(node_content):
  score = 0
  if node_content.upper().find('FOLLOW') > -1:
    score += 4
  if node_content.upper().find('BELOW') > -1:
    score += 2

  if node_content.upper().find('TABLE') > -1:
    score += 2

  if node_content.upper().find('PRESENT') > -1:
    score += 1
  if node_content.upper().find('PROVIDE') > -1:
    score += 1
  if node_content.upper().find('SUMMARIZE') > -1:
    score += 1
  if node_content.upper().find('SHOW') > -1:
    score += 1
  return score

handleStock("0C00000ADA")