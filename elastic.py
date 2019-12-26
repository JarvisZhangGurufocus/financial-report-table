
import re
import logging

from elasticsearch import Elasticsearch
from env import env

env = env()

tracer = logging.getLogger('elasticsearch')
tracer.setLevel(logging.CRITICAL)

class ElasticHelper:
  def __init__(self):
    self.tableIndex = "financial_report_tables_v4"
    self.reportIndex = "financial_reports_v4"
    self.client = Elasticsearch([env['ELASTIC_URL']])
  
  def getReportSections(self, report):
    print(report['_source']['document_id'])
    page = self.client.search(index=self.tableIndex, scroll = '2m', size = 100, body={
      "query": {
        "bool": {
          "should":[
            { "term": { 
                "document_id": report['_source']['document_id'] 
              }
            },
          ],
          "must":[
            {
              "terms": {
                "document_type": ["10-Q", "10-K"]
              }
            }
          ]
        }
      }
    })

    sections = []
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    while (scroll_size > 0):
      page = self.client.scroll(scroll_id = sid, scroll = '2m')
      sid = page['_scroll_id']
      scroll_size = len(page['hits']['hits'])
      for record in page['hits']['hits']:
        sections.append(record)

    return sections

  def searchTables(self, table):
    page = self.client.search(index=self.tableIndex, scroll = '2m', size = 100, body={
      "query": {
        "bool": {
          "should":[
            { "term": { 
              "morn_comp_id": table['_source']['morn_comp_id'] 
            }},
          ],
          "must":[
            { "match": { 
              "striped_content": {
                "query": re.sub('[^A-Za-z0-9]+', ' ', table['_source']['striped_content']).strip(),
                "minimum_should_match": '70%'
              }
            }}
          ]
        }
      },
      "_source": ["table_id"]
    })

    ids = []
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    while (scroll_size > 0):
      page = self.client.scroll(scroll_id = sid, scroll = '2m')
      sid = page['_scroll_id']
      scroll_size = len(page['hits']['hits'])
      for record in page['hits']['hits']:
        ids.append(record['_id'])

    return ids

  def getStockTables(self, morn_comp_id):
    page = self.client.search(index=self.tableIndex, scroll = '2m', size = 1000, body={
      "query": {
        "bool": {
          "must":[
            { "term": { "morn_comp_id": morn_comp_id } }
          ]
        }
      },
      "_source": ["table_id"]
    })

    ids = []
    for record in page['hits']['hits']:
      ids.append(record['_id'])

    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    while (scroll_size > 0):
      page = self.client.scroll(scroll_id = sid, scroll = '2m')
      sid = page['_scroll_id']
      scroll_size = len(page['hits']['hits'])
      for record in page['hits']['hits']:
        ids.append(record['_id'])

    return ids

  def getStockReports(self, morn_comp_id):
    page = self.client.search(index=self.reportIndex, scroll = '2m', size = 10, body={
      "query": {
        "bool": {
          "must":[
            { "term": { "morn_comp_id": morn_comp_id } },
            { "range": { "filling_date": { "gte": "2010-01-01" } } },
            { "terms": { "document_type": ["10-Q", "10-K"] } }
          ]
        }
      },
      "_source": False
    })

    ids = []
    for record in page['hits']['hits']:
      ids.append(record['_id'])

    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    while (scroll_size > 0):
      page = self.client.scroll(scroll_id = sid, scroll = '2m')
      sid = page['_scroll_id']
      scroll_size = len(page['hits']['hits'])
      for record in page['hits']['hits']:
        ids.append(record['_id'])

    return ids

  def getTable(self, table_id, field = '_id'):
    table = self.client.search(index=self.tableIndex, size = 1, body={
      "query": {
        "bool": {
          "must":[
            { "term": { field: table_id } }
          ]
        }
      }
    })

    return table['hits']['hits'][0]
  
  def getReport(self, report_id, field = '_id'):
    report = self.client.search(index=self.reportIndex, size = 1, body={
      "query": {
        "bool": {
          "must":[
            { "term": { field: report_id } }
          ]
        }
      }
    })

    return report['hits']['hits'][0]