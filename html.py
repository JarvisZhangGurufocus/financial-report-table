import json
import re
import datetime
import string

from bs4 import BeautifulSoup
from itertools import product
from utils import Utils

utils = Utils()
printable = set(string.printable)

class HtmlHelper:
  def htmlSoup(self, html):
    html = html.replace('<br/>', ' ').replace('<br>', ' ').replace('<br />', ' ').replace('\n', ' ').replace('\r', ' ')
    soup = BeautifulSoup(html, features='html.parser')
    return soup

  def htmlContent(self, html):
    html = html.replace('<br/>', ' ').replace('<br>', ' ').replace('<br />', ' ').replace('\n', ' ').replace('\r', ' ')
    soup = BeautifulSoup(html, features='html.parser')
    return soup.get_text()

  def isTitle(self, content):
    if 'In Millions' in content:
      return True
    if 'In Billions' in content:
      return True
    return False

  def isEmptyHeader(self, row):
    for content in row[1:]:
      if content != '':
        return False
    return True

  def isHeader(self, row):
    if self.isTitle(row[0]):
      return True
    
    isEmpty = True
    isDate = False
    isNumber = False
    for content in row[1:]:
      if 'Month' in content:
        isDate = True
      if 'Year' in content:
        isDate = True
      if 'Day' in content:
        isDate = True
      if utils.isNumber(content.replace('$', '').replace('%', '')):
        isNumber = True
      if content:
        isEmpty = False
    if isEmpty:
      return True
    if isDate and not isNumber:
      return True
    return False

  def getTableCells(self, html):
    if type(html) is str or type(html) is unicode:
      html = html.replace('<br/>', ' ').replace('<br>', ' ').replace('<br />', ' ').replace('\n', ' ').replace('\r', ' ')
      soup = BeautifulSoup(html, features='html.parser')
    else:
      soup = html
    
    body = self.table2Array(soup)
    header = [body.pop(0)]

    if len(body) == 0:
      return []

    while len(body) > 0 and ( body[0][0] == '' or self.isHeader(body[0])):
      header.append(body.pop(0))
    
    countEmptyHeader = 0
    while self.isEmptyHeader(header[len(header) - 1]) and countEmptyHeader < len(header):
      header = [header[len(header) - 1]] + header[0:-1]
      countEmptyHeader += 1

    cells = []

    attr = None
    for row in body:
      if row[0] == '':
        continue

      isAttr = True
      for i in range(len(row)):
        if i > 0 and row[i] != '' and row[i] != row[i-1]:
          isAttr = False
      
      if isAttr:
        attr = { 'value': row[0], 'type': 'secondary' }
        continue
      
      for i in range(len(row)):
        if i == 0:
          continue
        if row[i] == '':
          continue
        if header[len(header) - 1][i] == '':
          continue
        cell = { 'value': row[i], 'attrs':[
          { 'type': 'primary', 'value': row[0] },
          { 'type': 'primary', 'value': header[len(header) - 1][i] }
        ]}
        if attr is not None:
          cell['attrs'].append(attr)
        for h in range(len(header) - 1):
          if header[h][0] != '':
            cell['attrs'].append({'value': header[0][0], 'type': 'other'})
          cell['attrs'].append({'value': header[h][i], 'type': 'secondary'})
        
        cell = self.formatCell(cell)

        isSame = False
        for c in cells:
          if self.sameCell(cell, c):
            isSame = True
        isValide = self.valideCell(cell)

        if not isSame and isValide:
          cells.append(cell)
    
    return cells

  def formatCell(self, cell):
    cell['date'] = ''
    attrs = []

    primary_attrs = []
    secondary_attrs = []
    other_attrs = []

    cell['attrs'].sort(key= lambda x: ['primary', 'secondary', 'other'].index(x['type']))

    for attr in cell['attrs']:
      pieces = attr['value'].split()
      i = len(pieces) - 1
      while i >= 0:
        if utils.isDate(pieces[i]):
          date = utils.isDate(pieces[i])
          attr['value'] = attr['value'].replace(pieces[i], '')
          if len(date) > len(cell['date']):
            cell['date'] = date
        elif i > 1 and utils.isDate(pieces[i-2] + ' ' + pieces[i-1] + ' ' + pieces[i]):
          date = utils.isDate(pieces[i-2] + ' ' + pieces[i-1] + ' ' + pieces[i])
          attr['value'] = attr['value'].replace(pieces[i-2] + ' ' + pieces[i-1] + ' ' + pieces[i], '')
          if len(date) > len(cell['date']):
            cell['date'] = date
        elif i > 0 and utils.isDate(pieces[i-1] + ' ' + pieces[i]):
          date = utils.isDate(pieces[i-1] + ' ' + pieces[i])
          attr['value'] = attr['value'].replace(pieces[i-1] + ' ' + pieces[i], '')
          if len(date) > len(cell['date']):
            cell['date'] = date
        elif pieces[i].isdigit():
          attr['value'] = attr['value'].replace(pieces[i], '')
          if pieces[i].isdigit() and int(pieces[i]) > 1970 and int(pieces[i]) < 2050 and len(pieces[i]) > len(cell['date']):
            cell['date'] = pieces[i]
        i -= 1
      attr['value'] = re.sub(' +', ' ', attr['value'])
      if attr['type'] == 'primary' and attr['value'] in primary_attrs:
        continue
      if attr['type'] == 'secondary' and (attr['value'] in primary_attrs or attr['value'] in secondary_attrs):
        continue
      if attr['type'] == 'other' and (attr['value'] in primary_attrs or attr['value'] in secondary_attrs or attr['value'] in other_attrs):
        continue
      if attr['value'].replace(' ', ''):
        attrs.append(attr)
      if attr['type'] == 'primary':
        primary_attrs.append(attr['value'])
      if attr['type'] == 'secondary':
        secondary_attrs.append(attr['value'])
      if attr['type'] == 'other':
        other_attrs.append(attr['value'])
    
    cell['attrs'] = attrs

    return cell

  def valideCell(self, cell):
    for attr in cell['attrs']:
      if cell['value'] == attr['value']:
        return False
    return True

  def sameCell(self, cell1, cell2):
    if cell1 is None or cell2 is None:
      return False
    if re.sub('[^A-Za-z0-9]+', '', cell1['value']) != re.sub('[^0-9]+', '', cell2['value']):
      return False
    for t1 in cell1['attrs']:
      found = False
      for t2 in cell2['attrs']:
        if t1['value'] == t2['value'] and t1['type'] == t2['type']:
          found = True
      if not found:
        return False
    return True

  def array2Table(self, arr):
    HTML = ''
    for row in arr:
      rowHTML = ''
      for cell in row:
        rowHTML += '<td>%s</td>' % cell
      HTML += '<tr>%s</tr>' % rowHTML
    return '<table><tbody>%s</tbody></table>' % HTML

  def table2Array(self, soup):
    rowspans = [] 
    rows = soup.find_all('tr')

    colcount = 0
    for r, row in enumerate(rows):
      cells = row.find_all(['td', 'th'], recursive=False)
      colcount = max(
        colcount,
        sum(int(c.get('colspan', 1)) or 1 for c in cells[:-1]) + len(cells[-1:]) + len(rowspans))
      rowspans += [int(c.get('rowspan', 1)) or len(rows) - r for c in cells]
      rowspans = [s - 1 for s in rowspans if s > 1]

    table = [[''] * colcount for row in rows]

    rowspans = {}
    for row, row_elem in enumerate(rows):
      span_offset = 0
      for col, cell in enumerate(row_elem.find_all(['td', 'th'], recursive=False)):
        col += span_offset
        while rowspans.get(col, 0):
          span_offset += 1
          col += 1

        rowspan = rowspans[col] = int(cell.get('rowspan', 1)) or len(rows) - row
        colspan = int(cell.get('colspan', 1)) or colcount - col
      
        span_offset += colspan - 1
        value = utils.strEncode(cell.get_text())
        for drow, dcol in product(range(rowspan), range(colspan)):
          try:
            table[row + drow][col + dcol] = value
            rowspans[col + dcol] = rowspan
          except IndexError:
            pass

      rowspans = {c: s - 1 for c, s in rowspans.items() if s > 1}

    # merge $ % into next cell
    for y in range(len(table)):
      row = table[y]
      for x in range(len(row)):
        cell = row[x]
        if x > 0 and cell == '%':
          table[y][x-1] = table[y][x-1] + '%'
          table[y][x] = ''
        if x < len(row) - 1 and cell == '$':
          table[y][x+1] = '$' + table[y][x+1]
          table[y][x] = ''
    
    # remove empty column
    y = len(table) - 1
    while y >= 0:
      countEmpty = 0
      row = table[y]
      for i in range(len(row)):
        if row[i] == '':
          countEmpty += 1
      if countEmpty == len(row):
        table.pop(y)
      y -= 1
    
    # remove empty row
    x = len(table[0]) - 1
    while x >= 0:
      countEmpty = 0
      for y in range(len(table)):
        if table[y][x] == '':
          countEmpty += 1
      if countEmpty == len(table[0]):
        for y in range(len(table)):
          table[y].pop(x)
      x -= 1

    # remove first column if it's empty
    notPass = True
    while notPass:
      for y in range(len(table)):
        if len(table[y]) < 2:
          notPass = False
        elif table[y][0] != '' and table[y][0] != table[y][1]:
          notPass = False
      if notPass:
        for y in range(len(table)):
          table[y] = table[y][1:]

    return table

  def pluckNode(self, node):
    if node.name == None or node.name == 'table':
      return [node]
    
    onlyFont = True
    for child in node.children:
      if node.name is None or node.name == 'span' or node.name == 'font':
        continue
      onlyFont = False
    if onlyFont and self.isSection(node.get_text()):
      return [node]

    onlyString = True
    for child in node.children:
      if child.name is not None:
        onlyString = False
    if onlyString:
      return [node]
    
    nodes = []
    for child in node.children:
      nodes += self.pluckNode(child)
    return nodes
  
  def isSection(self, content):
    if not re.match(r'^[ ]*ITEM(.*)', content, re.IGNORECASE) and not re.match(r'^[ ]*NOTE(.*)', content, re.IGNORECASE):
      return False
    content = content.upper().replace('ITEM', '').replace('ITEMS', '').replace('NOTE', '').replace('NOTES', '')
    content = utils.strEncode(content)
    if content:
      return True
    return False
  
  def getSection(self, nodes, index):
    index = index - 1
    while index >= 0:
      node = nodes[index]
      if node.name != None and self.isSection(node.get_text()):
        section = node.get_text()
        section = re.sub('\s\s+', ' ', section)
        return section
      index -= 1
    return ''
  
  def getNodeContent(self, node):
    if node.name == None:
      return unicode(node)
    else:
      return node.get_text()

  def getTableContext(self, nodes, index):
    index = index - 1
    context = []
    contextEnough = False
    while index >= 0:
      if contextEnough:
        break
      node = nodes[index]
      if node.name == 'table':
        contextEnough = True
      node_content = self.getNodeContent(node)
      node_score = self.getContextNodeScore(node)
      sentences = self.breakSentence(node_content)
      sentences.reverse()
      for sentence in sentences:
        if contextEnough:
          break
        sentenceScore = self.getContextScore(sentence)
        if sentenceScore > 2:
          context.append(sentence)
          contextEnough = True
        elif sentenceScore > 0 and len(context) < 3:
          context.append(sentence)
      index -= 1
    return context
      

  def breakSentence(self, content):
    sentences = []
    last_index = 0
    for i in range(len(content)):
      if content[i] not in printable:
        continue
      if content[i] == '.' or content[i] == ';':
        sentences.append(content[last_index: i])
        last_index = i + 1
    sentences.append(content[last_index:])
    sentences = [x for x in sentences if x]
    return sentences
      
  def getContextNodeScore(self, node):
    if node.name == 'b':
      return 1
    if node.name != None and 'id' in node.attrs.keys() and node['id'] == 'temp-section':
      return 1
    if node.name != None and 'style' in node.attrs and node['style'].upper().find('FONT-WEIGHT') > -1:
      return 1
    return 0

  def getContextScore(self, content):
    score = 0
    if content.upper().find('FOLLOW') > -1:
      score += 4
    if content.upper().find('BELOW') > -1:
      score += 2

    if content.upper().find('TABLE') > -1:
      score += 2

    if content.upper().find('PRESENT') > -1:
      score += 1
    if content.upper().find('PROVIDE') > -1:
      score += 1
    if content.upper().find('SUMMARIZE') > -1:
      score += 1
    if content.upper().find('SHOW') > -1:
      score += 1
    return score



