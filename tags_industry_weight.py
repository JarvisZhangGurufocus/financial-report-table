import json
from operator import itemgetter
from sklearn.feature_extraction.text import TfidfTransformer
from os import listdir
from os.path import isfile, join
from utils import Utils
from mysql import MySqlHelper

utils = Utils()
mysqlHelper = MySqlHelper()

industries = [f for f in listdir('industries') if isfile(join('industries', f))]

vector = []
for industry in industries:
  content = json.loads(utils.readFile('industries/%s' % industry))
  vector.append(content)

transformer = TfidfTransformer()
tfidf = transformer.fit_transform(vector)
weights = tfidf.toarray()

for idx in range(0, len(industries)):
  weightMap = []
  for id in range (0, len(weights[0])):
    weightMap.append({'id': id + 1, 'weight': weights[idx][id]})
  weightMap = sorted(weightMap, key=itemgetter('weight'), reverse=True) 
  weightMap = weightMap[0:200]
  
  ids = ','.join([ str(x['id']) for x in weightMap ])
  tags = mysqlHelper.query('SELECT * FROM filing_tags WHERE id IN (%s)' % ids)
  
  for row in weightMap:
    for tag in tags:
      if row['id'] == tag['id']:
        row['label'] = tag['label']
        row['value'] = tag['value']

  utils.saveFile('logs/%s_weight' % industries[idx], json.dumps(weightMap, indent=4)) 