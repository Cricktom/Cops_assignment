import tweepy,sys,json,datetime,logging

logging.basicConfig(level=logging.INFO)

class QueryLead(object):
    """docstring for searching the query of holmes in leads stored in elastic search"""
    def __init__(self, es):
        self.es=es

    #processing the result of search query
    def process_result(self, leads):
        count = 0
        result = []
        for lead in leads['hits']['hits']:
            n_lead = {}
            n_lead['lead'] = str(lead["_source"]["lead"].encode("utf-8"))
            n_lead['reporter'] = str((lead["_source"]["author"]).encode("utf-8"))
            n_lead['categories'] = str(lead["_source"]["categories"])
            n_lead['entities'] = str(lead["_source"]["entities"])
            n_lead['created_at'] = str(lead["_source"]["created_at"])
            result.append(n_lead)
            logging.info( "lead--->>   "+ str(lead["_source"]["lead"].encode("utf-8")))
            logging.info( "reporter--->>   "+ str((lead["_source"]["author"]).encode("utf-8")))
            logging.info( "categories--->>   "+ str(lead["_source"]["categories"]))
            logging.info( "entities--->>   "+ str(lead["_source"]["entities"]))
            logging.info( "created--->>   "+ str(lead["_source"]["created_at"]))
            # logging.info( "entities--->>   "+ str(lead[""])
            count+=1;
            logging.info( "\n\n ############################################################\n\n")
            if(count>5):
                break
        if(count==0):
            logging.error("Nothing Found")
        return result
    #making query to the elastic server
    def make_query(self,query):
        logging.info("#########################")
        logging.info(query[1])
        logging.info("#########################")
        logging.info(query[2])

        leads = self.es.search(index = 'twitter_index', body = {"query":{"bool":{"should":[{"match":{'lead':{"query":query[0],"operator": "or","fuzziness":0 }}},
        {"match":{"author":{"query":query[0],"operator": "and","fuzziness":0 }}},
        {"match":{"categories":{"query":query[0],"operator": "or","fuzziness":0 }}}
        ,{"match":{"country":{"query":query[0],"operator": "or","fuzziness":0 }}},
        {"match":{"entities":{"query":query[0],"operator": "or","fuzziness":0 }}},
        {"geo_distance":{"distance":"12km","location":query[3] if(query[3]) else "40, -70" }}],
        "must":[{"range": { "created_at": {"gte" :(query[1] if(query[1]) else "now/d") ,"lte":(query[2] if(query[2]) else "now/d")}}}]}
        }})
        # logging.info( leads)
        return self.process_result(leads)
