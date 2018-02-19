import tweepy,sys,json,datetime,logging
from tweepy import OAuthHandler,Stream
from tweepy.streaming import StreamListener

logging.basicConfig(level=logging.INFO)

class TweetRetriever(StreamListener):
    """docstr for retrivieng the tweets as lead from twitter and index them into the elastic search """
    def __init__(self,es):
        self.es = es
    # makeing index if not present and inserting lead into elastic server
    def make_index(self,tweet):
        if not self.es.indices.exists(index="twitter_index"):
            with open('mapping.json') as json_data:
                request_body = json.load(json_data)
                # logging.info( request_body)
                try:
                    res = self.es.indices.create(index = "twitter_index", body = request_body)
                except Exception as e:
                    logging.error( "error in making Index  "+str(e))
        try:
            dat = tweet["created_at"].split()
            dt = dat[-1]+"-"+dat[1]+"-"+dat[2]
            dt = datetime.datetime.strptime(dt, "%Y-%b-%d")
            dt = datetime.datetime.strftime(dt,"%Y-%m-%d")
            created = dt+"T"+dat[3]
            country = tweet["place"]
            lead = tweet["text"]
            author = tweet["user"]["name"]
            hash_t = []
            u_m=[]
            for en in tweet["entities"]["hashtags"]:
                 hash_t.append(en["text"])
            for en in tweet["entities"]["user_mentions"]:
                u_m.append(en["name"])
            request_body = {"created_at":created, "country":country, "lead":lead, "author":author, "categories":hash_t, "entities":u_m}
            self.es.index(index="twitter_index", doc_type="tweets",id=tweet["id"], body=request_body)
            logging.info( "Making index for tweets")

        except BaseException as e:
            logging.error( " In making index "+str(e))



    #on getting tweet make_index call
    def on_data(self, data):
        try:
            # logging.info( data
            self.make_index(json.loads(data));
            return True
        except BaseException as e:
            logging.error(("Error on_data: %s" % str(e)))
        return True

    #catching errors
    def on_error(self, status):
        logging.error((status))
        return True
