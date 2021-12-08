import json
import sys
# import json
# from pyspark import SparkContext, SparkConf
# from pyspark.shell import spark
# from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import pyspark
print(pyspark.version)
from pyspark.streaming.kafka import KafkaUtils
#from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
from textblob import TextBlob

esConn = Elasticsearch()

def getHashTag(text):
    if "trump" in text.lower():
        return "#trump"
    elif "black life" in text.lower():
        return "#black_life"
    elif "black life matters" or "BLM" in text.lower():
        return "#black_life_matters"
    else:
        return "#corona"

def getSentimentValue(text):
    sentimentAnalyser = SentimentIntensityAnalyzer()
    polarity = sentimentAnalyser.polarity_scores(text)
    if(polarity["compound"] > 0):
        return "positive"
    elif(polarity["compound"] < 0):
        return "negative"
    else:
        return "neutral"

def getSentiment(time, rdd):
    test = rdd.collect()
    for i in test:
        print('what do i look like')
        print(i)
        esConn.index(index="hash_tags_sentiment_analysis4blm",
                     doc_type="tweet-sentiment-analysis", body=i)

if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("Usage: sentimentAnalysis.py <broker_list> <topic>", file=sys.stderr)
    #     exit(-1)
    spark = SparkSession \
        .builder \
        .appName("SentimentAnalysis") \
        .config("spark.yarn.dist.jars", "</root/spark-streaming-kafka-0-8_2.11-2.4.8.jar>,</root/spark-sql-kafka-0-10_2.11-2.0.2.jar>") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 20)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(
        ssc, [topic], {"metadata.broker.list": brokers})
    tweets = kvs.map(lambda x: str(x[1].encode("ascii", "ignore"))).map(
        lambda x: (x, getSentimentValue(x), getHashTag(x))).map(lambda x: {"message": x[0], "sentiment": x[1], "hashTag": x[2]})
    tweets.foreachRDD(getSentiment)
    # consumer = KafkaConsumer("twitter")
    # for msg in consumer:
    #     dict_data = json.loads(msg.value)
    #     tweet = TextBlob(dict_data['text'])
    #     print(tweet)
    #     esConn.index(index='tweet',
    #              doc_type='test-type',
    #              body={"author": dict_data['user']['screen_name'], 'date': dict_data['created_dat'], 'message': dict_data['text']})
    #     print('\n')
    #tweets.saveAsTextFiles('/media/data1/dingcheng/workspace/Xiaodi/BigData/BigData/HW3-v0', 'sentimental_output')
    #print(tweets)
    ssc.start()
    ssc.awaitTermination()