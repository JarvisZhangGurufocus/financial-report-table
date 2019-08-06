import json
import re
import datetime

from bs4 import BeautifulSoup
from itertools import product
from utils import Utils

utils = Utils()

class HtmlHelper:
  def htmlSoup(self, html):
    html = html.replace('<br/>', ' ').replace('<br>', ' ').replace('<br />', ' ').replace('\n', ' ').replace('\r', ' ')
    soup = BeautifulSoup(html, features='html.parser')
    return soup

  def htmlContent(self, html):
    html = html.replace('<br/>', ' ').replace('<br>', ' ').replace('<br />', ' ').replace('\n', ' ').replace('\r', ' ')
    soup = BeautifulSoup(html, features='html.parser')
    return soup.get_text()

  def getTableCells(self, html):
    html = html.replace('<br/>', ' ').replace('<br>', ' ').replace('<br />', ' ').replace('\n', ' ').replace('\r', ' ')
    soup = BeautifulSoup(html, features='html.parser')
    
    body = self.table2Array(soup)
    header = [body.pop(0)]

    if len(body) == 0:
      return []

    while len(body) > 0 and body[0][0] == '':
      header.append(body.pop(0))
    
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
          cell['attrs'].append({'value': header[h][i], 'type': 'secondary'})
        if header[0][0] != '':
          cell['attrs'].append({'value': header[0][0], 'type': 'other'})
        
        cell = self.formatCell(cell)

        isSame = False
        for c in cells:
          if self.sameCell(cell, c):
            isSame = True
        if not isSame:
          cells.append(cell)
    
    return cells

  def formatCell(self, cell):
    cell['date'] = ''

    attrs = []
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
          if pieces[i] > '1970' and pieces[i] < '2050' and len(pieces[i]) > len(cell['date']):
            cell['date'] = pieces[i]
        i -= 1
      attr['value'] = re.sub(' +', ' ', attr['value'])
      if attr['value'].replace(' ', ''):
        attrs.append(attr)
    cell['attrs'] = attrs

    return cell

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