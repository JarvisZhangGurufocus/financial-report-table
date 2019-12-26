# coding: utf8
import os
import re
import time
import string
import logging
import nltk

nltk.download("stopwords")

from datetime import datetime
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import word_tokenize

porterStemmer = PorterStemmer()
lmtzr = WordNetLemmatizer()
stopworddic = set(stopwords.words('english')) 

startDate = datetime(1970, 1, 1)
endDate = datetime.now()
dateFormats = {
  # 01/26/06
  '%m/%d/%y': '%B %d %Y',
  # 01/26/2006
  '%m/%d/%Y': '%B %d %Y',
  # Feb 01 06
  '%b %d %y': '%B %d %Y',
  # Feb 01 2006 
  '%b %d %Y': '%B %d %Y', 
  # Feb 06
  '%b %y': '%B %Y',
  # Feb 2006
  '%b %Y': '%B %Y',
  # March 01 06
  '%B %d %y': '%B %d %Y',
  # March 01 2006 
  '%B %d %Y': '%B %d %Y', 
  # March 06
  '%B %y': '%B %Y',
  # March 2006
  '%B %Y': '%B %Y',
  # Feb-19
  '%b-%d': '%B %d',
  # Feb-19-07
  '%b-%d-%y': '%B %d %Y',
  # Feb-19-2007
  '%b-%d-%Y': '%B %d %Y'
}

class Utils:
  def isDate(self, string, fuzzy=False):
    for format in dateFormats.keys():
      try:
        date = datetime.strptime(string, format)
        if date >= startDate and date <= endDate:
          return date.strftime(dateFormats[format])
      except ValueError:
        pass
    return False
  
  def splitAtUpper(self, s):
    res = ''
    for w in s.split():
      if w.isupper():
        res += ' ' + w
      else:
        nw = ''
        for l in w:
          if l.isupper():
            nw += ' ' + l
          else:
            nw += l
        res += ' ' + nw
    return res.strip()
  
  def splitAtWordNumber(self, s):
    res = ''
    for w in s.split():
      for i in range(len(w)):
        if i == 0:
          res += w[i]
        elif self.isNumber(w[i-1]) and w[i] in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ':
          res += ' ' + w[i]
        elif self.isNumber(w[i]) and w[i-1] in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ':
          res += ' ' + w[i]
        else:
          res += w[i]
      res += ' '
    return res.strip()

  def cleanData(self, content):
    content = self.strEncode(content)
    content = self.removeNumber(content)
    content = re.sub('[^A-Za-z]+', ' ', content)
    content = ' '.join([i for i in content.split() if i.lower() not in stopworddic and len(i) > 1 ])
    content = ' '.join([porterStemmer.stem(i) for i in content.split()])
    return content

  def cleanSectionName(self, name,maxlen = 256):
    romes = ['i','ii','iii','iv','v','vi','vii','viii','ix','x']

    name = re.sub('[%s]*' % re.escape(string.punctuation), '', name, flags=re.I)
    name = re.sub('^item', '', name, flags=re.I)
    name = re.sub('[0-9]', '', name)
    name = re.sub('UNAUDITED', '', name, flags=re.I).strip()

    words = word_tokenize(name) 
    words = [porterStemmer.stem(lmtzr.lemmatize(word)) for word in words]
    words = [word for word in words if word not in stopworddic and word not in romes and word.isalpha() and len(word)>=2]

    name = ' '.join(words)

    if len(name)>maxlen:
      name = name[:maxlen-1]
    if name == 'execut compens':
      name = 'executcompens'
    if name == 'execut compensatio':
      name = 'executcompensatio'
    
    return name.strip().lower()

  def strEncode(self, s):
    s = s.strip()
    s = s.replace(',', '')

    s = re.sub('[^A-Za-z0-9%/.-]+', ' ', s)
    s = self.splitAtUpper(s)
    s = self.splitAtWordNumber(s)
    s = ' '.join(w[0].upper()+w[1:].lower() for w in s.split())

    if len(s) > 1:
      s = ' '.join(w for w in s.split() if len(w)>1)
    
    return s

  def removeNumber(self, s):
    res = ''
    for w in s.split(' '):
      if not self.isNumber(w):
        res += w + ' '
    return res.strip()

  def isNumber(self, s):
    s = s.replace(',', '')
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass 
    return False

  def strAcsii(self, s):
    v = 0
    s = re.sub('[^A-Za-z0-9]+', '', s)
    for c in s:
      v += ord(c)
    return v

  def saveFile(self, path, content):
    file = open(path, 'w+')
    file.write(content)
    file.close()

  def readFile(self, path):
    if not os.path.exists(path):
      return ''
    file = open(path, "r+")
    content = file.read()
    file.close()
    return content
  
  def appendFile(self, path, content):
    file = open(path, 'a+')
    file.write(content + '\n')
    file.close()

  def setupLogger(self, name, filename, level=logging.info):
    l = logging.getLogger(name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(filename, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)
    
    return logging.getLogger(name)