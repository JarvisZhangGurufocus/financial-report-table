import numpy
import pandas

from utils import Utils
from mysql import MySqlHelper
from sklearn import datasets
from sklearn.svm import LinearSVC
from sklearn.externals import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer

utils = Utils()
mysqlHelper = MySqlHelper()

sectionVectorizer = joblib.load('pkls/vectorizer.section.pkl')
sectionTransformer = joblib.load('pkls/transformer.section.pkl')
sectionKnn = joblib.load('pkls/knn.section.pkl')

def getXY():
  tables = mysqlHelper.query('SELECT * FROM report_tables')
  pTags = []
  sTags = []
  oTags = []
  contexts = []
  sections = []

  for table in tables:
    if not table['primary_tags']:
      table['primary_tags'] = 'NAN'
    if not table['secondary_tags']:
      table['secondary_tags'] = 'NAN'
    if not table['other_tags']:
      table['other_tags'] = 'NAN'
    if not table['context']:
      table['context'] = 'NAN'
    if not table['section']:
      table['section'] = 'NAN'
  
    table['context'] = utils.cleanData(table['context'])

    pTags.append(table['primary_tags'].replace('|', ' '))
    sTags.append(table['secondary_tags'].replace('|', ' '))
    oTags.append(table['other_tags'].replace('|', ' '))
    contexts.append(table['context'].replace('|', ' '))

    section = utils.cleanSectionName(table['section'])
    sectionTag = sectionKnn.predict(sectionTransformer.transform(sectionVectorizer.transform([section])).toarray())[0]
    sections.append(sectionTag)

  vectorizer = TfidfVectorizer(sublinear_tf=True, min_df=5, norm='l2', encoding='latin-1', ngram_range=(1, 2), stop_words='english')
  X_pTags = vectorizer.fit_transform(pTags).toarray()
  X_sTags = vectorizer.fit_transform(sTags).toarray()
  X_oTags = vectorizer.fit_transform(oTags).toarray()
  X_contexts = vectorizer.fit_transform(contexts).toarray()
  X = numpy.hstack((X_pTags, X_sTags, X_oTags, X_contexts))

  Y = numpy.array(sections)

  return X, Y

def crossTest():
  import seaborn
  import matplotlib

  models = [
    RandomForestClassifier(n_estimators=200, max_depth=3, random_state=0),
    LinearSVC(),
    MultinomialNB(),
    LogisticRegression(random_state=0),
  ]
  X, Y = getXY()
  CV = 10
  cv_df = pandas.DataFrame(index=range(CV * len(models)))
  entries = []
  for model in models:
    model_name = model.__class__.__name__
    accuracies = cross_val_score(model, X, Y, scoring='accuracy', cv=CV)
    for fold_idx, accuracy in enumerate(accuracies):
      entries.append((model_name, fold_idx, accuracy))
  cv_df = pandas.DataFrame(entries, columns=['model_name', 'fold_idx', 'accuracy'])

  print cv_df.groupby('model_name').accuracy.mean()

  seaborn.boxplot(x='model_name', y='accuracy', data=cv_df)
  seaborn.stripplot(x='model_name', y='accuracy', data=cv_df, size=8, jitter=True, edgecolor="gray", linewidth=2)
  matplotlib.pyplot.show()

def predict(X_pred):
  model = LinearSVC()
  X, Y = getXY()
  model.fit(X, Y)
  Y_pred = model.predict(X_pred)
