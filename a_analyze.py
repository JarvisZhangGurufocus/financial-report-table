import os
import re
import random

from utils import Utils
from elastic import ElasticHelper
from mysql import MySqlHelper
from html import HtmlHelper

from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from sklearn.cluster import KMeans
from sklearn.externals import joblib
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import LogisticRegression

porterStemmer = PorterStemmer()
stopworddic = set(stopwords.words('english')) 

utils = Utils()
mysqlHelper = MySqlHelper()
elasticHelper = ElasticHelper()
htmlHelper = HtmlHelper()

NUM_CLUSTERS = 200
SLOVER = 'lbfgs'

def LoadData(morn_comp_ids):
  for morn_comp_id in morn_comp_ids:
    print 'STOCK %s' % morn_comp_id
    tableIds = elasticHelper.getStockTables(morn_comp_id)
    for tableId in tableIds:
      print ' TABLE %s (%s)' % (tableId, morn_comp_id)
      if os.path.exists("tables/%s" % tableId):
        continue
      table = elasticHelper.getTable(tableId)
      content = htmlHelper.htmlContent(table['_source']['content'])
      content = utils.strEncode(content)
      utils.saveFile('tables/%s' % tableId, content)

def GetK():
  corpus = []
  i = 0
  for tableId in os.listdir('tables'):
    print '  COLLECT %s %s' % (tableId, i)
    i += 1
    content = utils.readFile('tables/%s' % tableId)
    content = cleanData(content)
    corpus.append(content)
  vectorizer = CountVectorizer(stop_words="english")
  transformer = TfidfTransformer()
  tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))
  weight = tfidf.toarray()
  SSE = []
  for i in range(1, 400):
    print '  CLUSTER %s' % i
    model = KMeans(n_clusters=i)
    model.fit(weight)
    SSE.append(model.inertia_)
  import matplotlib.pyplot as plt
  plt.xlabel('k')
  plt.ylabel('SSE')
  plt.plot(range(1, 400),SSE,'o-')
  plt.show()

def TfIdf():
  print 'TFIDF'
  corpus = []
  for tableId in os.listdir('tables'):
    print '  COLLECT %s' % tableId
    content = utils.readFile('tables/%s' % tableId)
    content = cleanData(content)
    corpus.append(content)
  
  vectorizer = CountVectorizer(stop_words="english")
  transformer = TfidfTransformer()
  tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))
  weight = tfidf.toarray()
  
  model = KMeans(n_clusters=NUM_CLUSTERS)
  model.fit(weight)

  tableClusters = {}
  for idx in range(len(model.labels_)):
    label = model.labels_[idx]
    if label not in tableClusters.keys():
      tableClusters[label] = ''
    tableClusters[label] += corpus[idx] + '\n'
  
  for label, content in tableClusters.items():
    print '   %s' % label
    utils.saveFile('clusters/%s' % label, content)
  
  knn = LogisticRegression(solver=SLOVER)
  knn.fit(weight, model.labels_)

  joblib.dump(knn, 'pkls/knn.pkl')
  joblib.dump(vectorizer, 'pkls/vectorizer.pkl')
  joblib.dump(transformer, 'pkls/transformer.pkl')

def Predict(content):
  content = cleanData(content)
  if not os.path.exists("pkls/vectorizer.pkl"):
    TfIdf()
  vectorizer = joblib.load('pkls/vectorizer.pkl')
  transformer = joblib.load('pkls/transformer.pkl')
  knn = joblib.load('pkls/knn.pkl')
  weight = transformer.transform(vectorizer.transform([content])).toarray()
  y = knn.predict(weight)
  return y

def cleanData(content):
  content = utils.strEncode(content)
  content = utils.removeNumber(content)
  content = re.sub('[^A-Za-z]+', ' ', content)
  content = ' '.join([i for i in content.split() if i.lower() not in stopworddic and len(i) > 1 ])
  content = ' '.join([porterStemmer.stem(i) for i in content.split()])
  return content

# LoadData([
#   '0C00000AS4', '0C00000AHZ', '0C00000B9B', '0C00004AHY', '0C000007XW'
# ])

# TfIdf()

# GetK()