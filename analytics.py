import tweepy,sys,json,datetime,logging

logging.basicConfig(level=logging.INFO)

class Analytics(object):
    """docstring for lead analytics  of holmes network"""


    def __init__(self,es):
        self.es = es

    #logging.info(ing average numer analytics
    def process_avg_analytics(self, docs):
        data = []
        for item in docs["aggregations"]["leads"]["buckets"]:
            n_d = {}
            n_d['key'] = str(item["key"].encode("utf-8"))
            n_d['avg'] = str(item["aggs"]["value"])
            data.append(n_d)
            logging.info( "Average number of leads corresponding to key "+str(item["key"].encode("utf-8"))+"  is :::::::::------>>>>>"+str(item["aggs"]["value"])+"\n\n")
        return data

    #processing the other analytics
    def process_analytics(self, docs):
        data = []
        for item in docs["aggregations"]["top_tags"]["buckets"]:
            n_d={}
            n_d['key']=str(item["key"].encode("utf-8"))
            n_d['doc_count']=str(item["doc_count"])
            n_d['leads'] = []
            logging.info( "leads corresponding to key:::::::  "+str(item["key"].encode("utf-8")))
            logging.info( "doc_count corresponding to key:::::::  "+str(item["key"].encode("utf-8"))+":::::"+str(item["doc_count"])+"  \n")
            for lead in item["top_lead_hits"]["hits"]["hits"]:
                n_d['leads'].append(str(lead["_source"]["lead"].encode("utf-8")))
                logging.info( str(lead["_source"]["lead"].encode("utf-8"))+"\n")
            logging.info( "\n\n###############\n\n")
            data.append(n_d)
        return data
    #leads analysis by categories,entities,reporter respectively
    def get_category_analytics(self):
        leads_by_category = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs": {"top_tags": {"terms": {"field": "categories.raw","size": 10},
            "aggs": {"top_lead_hits": {"top_hits":{"_source": {"includes": [ "lead", "categories.raw" ]},"size" : 10}}}}}})
        logging.info( "::::::::::::Analytics Lead by categories:::::::::\n\n")
        # logging.info( len(leads_by_category["aggregations"]["top_tags"]["buckets"]))
        return self.process_analytics(leads_by_category)

    def get_entities_analytics(self):
        leads_by_entities = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs": {"top_tags": {"terms": {"field": "entities.raw","size": 10},
            "aggs": {"top_lead_hits": {"top_hits":{"_source": {"includes": [ "lead", "entities.raw" ]},"size" : 10}}}}}})
        logging.info( "::::::::::::Analytics Lead by entities:::::::::\n\n")
        return self.process_analytics(leads_by_entities)

    def get_reporter_analytics(self):
        leads_by_reporter = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs": {"top_tags": {"terms": {"field": "author.raw","size": 10},
            "aggs": {"top_lead_hits": {"top_hits":{"_source": {"includes": [ "lead", "author.raw" ]},"size" : 10}}}}}})
        logging.info( "::::::::::::Analytics Lead by reporter:::::::::\n\n")
        return self.process_analytics(leads_by_reporter)

    # Average lead analytics
    def get_avg_category_analytics(self):
        result = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs":{"leads": {"terms": {"field": "categories.raw"},
            "aggs": { "leads_per_minute": {"date_histogram": {"field": "created_at","interval": "minute"}},"aggs": { "avg_bucket": {"buckets_path": "leads_per_minute._count"
            }}}}}})
        logging.info( "::::::::::::Analytics  avg number of Lead per category :::::::::\n\n")
        return self.process_avg_analytics(result)

    def get_avg_entity_analytics(self):
        result = self.es.search(index = 'twitter_index', doc_type = "tweets",size=0, body = {"aggs":{"leads": {"terms": {"field": "entities.raw"},
            "aggs": { "leads_per_minute": {"date_histogram": {"field": "created_at","interval": "minute"}},"aggs": { "avg_bucket": {"buckets_path": "leads_per_minute._count"
            }}}}}})
        logging.info( "::::::::::::Analytics  avg number of Lead per entity :::::::::\n\n")
        return self.process_avg_analytics(result)
