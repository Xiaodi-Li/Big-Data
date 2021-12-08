"""API ACCESS KEYS"""

access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""

'''
https://stackoverflow.com/questions/69338089/cant-import-streamlistener
'''
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from operator import add
import time
from pyspark.sql.functions import col, avg, size
start = time.time()
spark = SparkSession.builder.config("spark.driver.memory", "100g").getOrCreate()

producer = KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server


topic_name = "twitterdata"
topic_name = 'covid19_twitter'
topic_name = "black_life_matters"

class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        '''
        lixiaodi327@gmail.com
        LydiaLiDi@19960327
        :return:
        '''
        username = 'LydiaLi327'
        password = 'LydiaLiDi@19960327'
        consumer_key = "nni2TtoyRaFT0Mgl5irtTvh6A"
        consumer_secret = "jatz8uCWkpVyNDKmeRx3bWnlNtnYaLWIm6s1sHwH7ohGOB8Gt8"
        access_token = "1456675094260957190-0effDWNFPaRnFvvQZxY686EfLGwIwi"
        access_token_secret = "yjksIw7V3bje7s7SFfPaiA2Na1oWtRE9asbHFoxEFwy2S"
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth



class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS()
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            #stream.filter(track=["Apple"], stall_warnings=True, languages= ["en"])
            #stream.filter(track=["covid19", "corona virus"], stall_warnings=True, languages= ["en"])
            stream.filter(track=["BLM", "corona virus", "black life matters"], stall_warnings=True, languages=["en"])


class ListenerTS(StreamListener):

    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()
    # Subscribe to 1 topic
    ###.option("kafka.bootstrap.servers", "localhost:9092,host2:port2") \
    # df = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "twitter_data") \
    #     .load()

    #df.printSchema()
    #df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")