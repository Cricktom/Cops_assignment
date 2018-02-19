
                        ############# Required Packages ##################
import sys,tweepy,json,logging
from tweetRetriever import TweetRetriever
from queryLead import QueryLead
from analytics import Analytics
from tweepy import OAuthHandler,Stream
from tweepy.streaming import StreamListener
from elasticsearch import Elasticsearch
from flask import Flask,request,jsonify


logging.basicConfig(level=logging.INFO)
# logging.setLevel(logging.DEBUG)
app = Flask(__name__)
es_instance =  Elasticsearch(hosts=[{"host":"127.0.0.1", "port":9200}])

#method for fetching tweets
@app.route('/cops/app/tweets', methods=['POST'])
def get_tweets():
    try:
        consumer_key = 'rDNahiYsc9xnuPT1dHIurYhwN'
        consumer_secret = '9JivLmJ5kn9iSYCR9zxcChgOKgf1MIAE7Stpcz28mk0daN7RiF'
        access_token = '3900430219-vhusv7KC1iaf9eefqzF3nBYIOJ0bpDGKNADj4vi'
        access_secret = 'dfU0Zp46mNucjZd4xcgbFnsVGt3GX6ffvcohzMnqHXAZh'
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        api = tweepy.API(auth)
        twitter_stream = Stream(auth, TweetRetriever(es_instance))
        twitter_stream.filter(languages=['en'], track=["a","b", "c","d","e","f","g","h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"])
        # return True
    except Exception as e:
        logging.error("In fetching tweets"+str(e))
        # return {"sucess"}

#method for fetching leads
@app.route('/cops/app/leads', methods=['POST'])
def get_leads():

    try:
        arg =  json.dumps(request.json)
        arg = json.loads(arg)
        print type(arg)
        query_object = QueryLead(es_instance)
        return jsonify({"sucess":True, "data":query_object.make_query(arg)})

    except Exception as e:
        logging.error("Data form inapropriate please check the format"+str(e))
        return jsonify({"sucess":False})

#method for fetching analytics
@app.route('/cops/app/analytics', methods=['POST'])
def get_analytics():
    try:
        args = json.dumps(request.json)
        args = json.loads(args)
        arg = args[0]
        analytic_object = Analytics(es_instance)
        result = []
        if(arg=="category"):
            result = analytic_object.get_category_analytics()
        elif(arg=="entity"):
            result = analytic_object.get_entities_analytics()
        elif(arg=="reporter"):
            result = analytic_object.get_reporter_analytics()
        elif(arg=="avg_leads_cat"):
            result = analytic_object.get_avg_category_analytics()
        elif(arg=="avg_leads_ent"):
            result = analytic_object.get_avg_entity_analytics()
        else:
            result = ["Invalid choice"]
            logging.error( "Invalid Choice")
        return jsonify({"success":True, "data":result})

    except Exception as e:
        logging.error("In fetching analytics"+str(e))
        return jsonify({"success":False})

if __name__ == "__main__":
    logging.info("Running Holmes Network Bot")
    app.run(host='0.0.0.0',threaded=True, debug=True, port=7080)
