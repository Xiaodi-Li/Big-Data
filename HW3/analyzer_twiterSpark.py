
from __future__ import print_function
import os
import sys
import ast
import json

import re
import string
import requests
import matplotlib.pyplot as plt
import threading
import queue
import time
import requests_oauthlib
#import cartopy.crs as ccrs
from sklearn.feature_extraction.text import TfidfVectorizer
from mpl_toolkits.basemap import Basemap
import matplotlib.patheffects as PathEffects
import matplotlib.pyplot as plt
from pylab import rcParams
import numpy as np
import multiprocessing
from functools import reduce

# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/1.5.2/"
# Append the python dir to PYTHONPATH so that pyspark could be found
sys.path.append('/usr/local/Cellar/apache-spark/1.5.2/python/')
# Append the python/build to PYTHONPATH so that py4j could be found
sys.path.append('/usr/local/Cellar/apache-spark/1.5.2/python/lib/py4j-0.8.2.1-src.zip')
from pyspark import SparkContext
from pyspark import SQLContext,Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml.feature import HashingTF,IDF, Tokenizer
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.feature import Word2VecModel
from json.decoder import JSONDecodeError
import seaborn as sns
from sklearn.manifold import TSNE
# Random state we define this random state to use this value in TSNE which is a randmized algo.
RS = 25111993

username = 'LydiaLi327'
password = 'LydiaLiDi@19960327'
consumer_key = "nni2TtoyRaFT0Mgl5irtTvh6A"
consumer_secret = "jatz8uCWkpVyNDKmeRx3bWnlNtnYaLWIm6s1sHwH7ohGOB8Gt8"
access_token = "1456675094260957190-0effDWNFPaRnFvvQZxY686EfLGwIwi"
access_token_secret = "yjksIw7V3bje7s7SFfPaiA2Na1oWtRE9asbHFoxEFwy2S"

auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret,access_token, access_token_secret)

BATCH_INTERVAL = 10  # How frequently to update (seconds)
clusterNum=3


def data_plotting(q):
    fig = plt.figure(figsize=(8,8))
    ax = fig.add_subplot(111)
    plt.ion() # Interactive mode

    fig.show()
    llon = -130
    ulon = 100
    llat = -30
    ulat = 60
    rcParams['figure.figsize'] = (14,10)
    my_map = Basemap(projection='merc',
                resolution = 'l', area_thresh = 1000.0,
                llcrnrlon=llon, llcrnrlat=llat, #min longitude (llcrnrlon) and latitude (llcrnrlat)
                urcrnrlon=ulon, urcrnrlat=ulat) #max longitude (urcrnrlon) and latitude (urcrnrlat)

    my_map.drawcoastlines()
    my_map.drawcountries()
    my_map.drawmapboundary()
    my_map.fillcontinents(color = 'white', alpha = 0.3)
    my_map.shadedrelief()
    plt.pause(0.0001)
    plt.show()


    colors = plt.get_cmap('jet')(np.linspace(0.0, 1.0, clusterNum))

    while True:
        if q.empty():
            time.sleep(5)

        else:
            obj=q.get()
            d=[x[0][0] for x in obj]
            c=[x[1] for x in obj]
            data = np.array(d)
            pcolor=np.array(c)
            # print("=================printing_from_data_plotting=================")
            # print(c)
            # print("=================done_printing_from_data_plotting=================")
            try:
                xs,ys = my_map(data[:, 0], data[:, 1])
                my_map.scatter(xs, ys,  marker='o', alpha = 0.5,color=colors[pcolor])
                plt.pause(0.0001)
                plt.draw()
                time.sleep(5)
            except IndexError: # Empty array
                pass


def parse(lp):
    label = float(lp[lp.find('(') + 1: lp.find(')')])
    vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))

    return LabeledPoint(label, vec)

def get_coord2(post):
    coord = tuple()
    try:
        if post['coordinates'] == None:
            coord = post['place']['bounding_box']['coordinates']
            coord = reduce(lambda agg, nxt: [agg[0] + nxt[0], agg[1] + nxt[1]], coord[0])
            coord = tuple(map(lambda t: t / 4.0, coord))
        else:
            coord = tuple(post['coordinates']['coordinates'])
    except TypeError:
        #print ('error get_coord')
        coord=(0,0)
    return coord



def get_json(myjson):
    try:
        json_object = json.loads(myjson)
    except JSONDecodeError as e:
        #except ValueError, e:
        return False
    # print("==================get_json================================")
    # print(json_object)
    # print("==================done_get_json================================")
    return json_object

# def get_json(myjson):
#   json_object = json.loads(myjson)
#   return json_object


def doc2vec(document):
    #doc_vec = np.zeros(300)
    #we should not initialize doc_vec as 0
    doc_vec = np.random.random(300)
    tot_words = 1

    for ind, word in enumerate(document):
        print(f"ind = {ind} and word={word}")
        try:
            print('lookup_bd')
            #print(lookup_bd.value)
            vec = np.array(lookup_bd.value.get(word))
            print(f'==========in_doc2vec========vec_look_up_shape_is_{vec.shape}============')
            print(vec)
            if vec.all()!= None:
                print('is doc_vec updated')
                doc_vec += vec
                print('updated doc vec')
                print(doc_vec)
                tot_words += 1
        except:
            continue

    #return(tot_words)
    # print("=======================doc_vec_after_updated===========================")
    # print(doc_vec)
    # print("=======================doc_vec_after_updated_done===========================")
    return doc_vec / float(tot_words)

from collections import Counter
def freqcount(terms_all):
    count_all = Counter()
    count_all.update(terms_all)
    return count_all.most_common(5)

remove_spl_char_regex = re.compile('[%s]' % re.escape(string.punctuation)) # regex to remove special characters
stopwords=[u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves', u'you', u'your', u'yours', u'yourself', u'yourselves', u'he', u'him', u'his', u'himself', u'she', u'her', u'hers', u'herself', u'it', u'its', u'itself', u'they', u'them', u'their', u'theirs', u'themselves', u'what', u'which', u'who', u'whom', u'this', u'that', u'these', u'those', u'am', u'is', u'are', u'was', u'were', u'be', u'been', u'being', u'have', u'has', u'had', u'having', u'do', u'does', u'did', u'doing', u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'because', u'as', u'until', u'while', u'of', u'at', u'by', u'for', u'with', u'about', u'against', u'between', u'into', u'through', u'during', u'before', u'after', u'above', u'below', u'to', u'from', u'up', u'down', u'in', u'out', u'on', u'off', u'over', u'under', u'again', u'further', u'then', u'once', u'here', u'there', u'when', u'where', u'why', u'how', u'all', u'any', u'both', u'each', u'few', u'more', u'most', u'other', u'some', u'such', u'no', u'nor', u'not', u'only', u'own', u'same', u'so', u'than', u'too', u'very', u's', u't', u'can', u'will', u'just', u'don', u'should', u'now']
def tokenize(text):
    tokens = []
    text = text.encode('ascii', 'ignore') #to decode
    text = text.decode('utf-8')
    text=re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text) # to replace url with ''
    text = remove_spl_char_regex.sub(" ",text)  # Remove special characters
    text=text.lower()
    for word in text.split():
        if word not in stopwords \
            and word not in string.punctuation \
            and len(word)>1 \
            and word != '``':
                tokens.append(word)
    # print("================tokenize==========")
    # print(tokens)
    # print("================done_tokenize==========")
    return tokens

sns.set_style('darkgrid')
sns.set_palette('muted')
sns.set_context("notebook", font_scale=1.5,
                rc={"lines.linewidth": 2.5})

#z = pd.DataFrame(model.labels_.tolist())

def scatter(x, colors):
    # We choose a color palette with seaborn.
    palette = np.array(sns.color_palette("hls", 3))

    # We create a scatter plot.
    f = plt.figure(figsize=(32, 32))
    ax = plt.subplot(aspect='equal')
    sc = ax.scatter(x[:,0], x[:,1], lw=0, s=120,
                    c=palette[colors.astype(np.int)])
    #plt.xlim(-25, 25)
    #plt.ylim(-25, 25)
    ax.axis('off')
    ax.axis('tight')

    # We add the labels for each cluster.
    txts = []
    for i in range(18):
        # Position of each label.
        xtext, ytext = np.median(x[colors == i, :], axis=0)
        txt = ax.text(xtext, ytext, str(i), fontsize=50)
        txt.set_path_effects([
            PathEffects.Stroke(linewidth=5, foreground="w"),
            PathEffects.Normal()])
        txts.append(txt)

    return f, ax, sc, txts

def streamrdd_to_df(srdd):
    sdf = sqlContext.createDataFrame(srdd)
    #print("show sdf in streamrdd_to_df")
    #sdf.show(n=2, truncate=False)
    return sdf

def get_prediction(tweet_text):
    try:
        # filter the tweets whose length is greater than 0
        tweet_text = tweet_text.filter(lambda x: len(x) > 0)
        # create a dataframe with column name 'tweet' and each row will contain the tweet
        rowRdd = tweet_text.map(lambda w: Row(tweet=w))
        # create a spark dataframe
        wordsDataFrame = spark.createDataFrame(rowRdd)
        # transform the data using the pipeline and get the predicted sentiment
        pipelineFit.transform(wordsDataFrame).select('tweet','prediction').show()
    except :
       print('No data')

if __name__ == '__main__':
    q = multiprocessing.Queue()
    job_for_another_core2 = multiprocessing.Process(target=data_plotting,args=(q,))
    job_for_another_core2.start()
    # Set up spark objects and run
    #sc  = SparkContext('local[*]', 'Social Panic Analysis')
    sc  = SparkContext('local[*]', 'Twitter Analysis')
    ssc = StreamingContext(sc, BATCH_INTERVAL)

    # trainingData = sc.textFile("/media/data1/dingcheng/workspace/spark/examples/src/main/python/mllib/data/mllib/kmeans_data.txt")\
    #     .map(lambda line: Vectors.dense([float(x) for x in line.strip().split(' ')]))
    #
    # testingData = sc.textFile("/media/data1/dingcheng/workspace/spark/examples/src/main/python/mllib/data/mllib/streaming_kmeans_data_test.txt").map(parse)
    #
    # trainingQueue = [trainingData]
    # testingQueue = [testingData]
    #
    # trainingStream = ssc.queueStream(trainingQueue)
    # testingStream = ssc.queueStream(testingQueue)
    # We create a model with random clusters and specify the number of clusters to find
    model = StreamingKMeans(k=2, decayFactor=1.0).setRandomCenters(3, 1.0, 0)

    # Now register the streams for training and testing and start the job,
    # printing the predicted cluster assignments on new data points as they arrive.
    # model.trainOn(trainingStream)

    # clust = model.predictOnValues(testingStream.map(lambda lp: (lp.label, lp.features)))
    # clust.pprint()
    print("+++++++++++++++++++++++++++++++++++++++test_simple_stream_kmeans_done")
    sqlContext=SQLContext(sc)
    # lookup = sqlContext.read.parquet("/media/data1/dingcheng/workspace/Xiaodi/BigData/BigData/HW3/word2vecModel/data").alias("lookup")
    # print("look up printSchema")
    # lookup.printSchema()
    # print("lookup show")
    # lookup.show(10)
    # lookup_bd = sc.broadcast(lookup.rdd.collectAsMap())
    #ssc.checkpoint("checkpoint")

    # Create a DStream that will connect to hostname:port, like localhost:9999
    dstream = ssc.socketTextStream("localhost", 9999)
    #dstream = ssc.socketTextStream("localhost", 2004)
    #dstream_tweets.count().pprint()
    #
    dstream_tweets=dstream.map(lambda post: get_json(post))\
         .filter(lambda post: post != False)\
         .filter(lambda post: 'created_at' in post)\
         .map(lambda post: (get_coord2(post)[0],get_coord2(post)[1],post["text"]))\
         .filter(lambda tpl: tpl[0] != 0)\
         .filter(lambda tpl: tpl[2] != '')\
         .map(lambda tpl: (tpl[0],tpl[1],tokenize(tpl[2])))\
         .map(lambda tpl:(tpl[0],tpl[1],tpl[2],doc2vec(tpl[2])))
    # print("================the_following_printing_dstream_tweets.pprints=============")
    # dstream_tweets.pprint()
    #print("================the_following_printing_traindata=============")
    trainingData=dstream_tweets.map(lambda tpl: [tpl[0],tpl[1]]+tpl[3].tolist())
    #trainingData.pprint()
    #print("================done_printing_traindata=============")
    #print("================the_following_printing_testdata=============")
    testdata=dstream_tweets.map(lambda tpl: (([tpl[0],tpl[1]],tpl[2]),[tpl[0],tpl[1]]+tpl[3].tolist()))
    #testdata.pprint()
    #print("================done_printing_testdata=============")
    #
    model = StreamingKMeans(k=clusterNum, decayFactor=0.6).setRandomCenters(302, 1.0, 3)
    #model = StreamingKMeans(k=2, decayFactor=1.0).setRandomCenters(3, 1.0, 0)
    model.trainOn(trainingData)
    clust=model.predictOnValues(testdata)
    clust.saveAsTextFiles('/media/data1/dingcheng/workspace/Xiaodi/BigData/BigData/HW3/clusts')
    #result = model.predictOnValues(testdata.map(lambda lp: (lp.label, lp.features)))
    #print(model.predictOnValues(testdata.map(lambda lp: (lp.label, lp.features))))
    # print("cluster_results")
    #print(clust)
    #clust.pprint()
    #words = lines.flatMap(lambda line: line.split(" "))
    topic=clust.map(lambda x: (x[1],x[0][1]))
    #documents = topic.foreachRDD(lambda x: x.toDF())
    #topic.pprint()
    topicAgg = topic.reduceByKey(lambda x,y: x+y)
    #tweet_text = tweet_text.filter(lambda x: len(x) > 0)
    topicAgg = topicAgg.filter(lambda x: x is None)
    documents = topicAgg.foreachRDD(lambda x: x.toDF())
    print(documents)
    #topicAgg.pprint()
    # documents = topicAgg.foreachRDD(lambda x: x.collect())
    # topicAgg.saveAsTextFiles('/media/data1/dingcheng/workspace/Xiaodi/BigData/BigData/HW3/topic_file')
    # document = []
    # with open('/media/data1/dingcheng/workspace/Xiaodi/BigData/BigData/HW3/topic_file.txt', 'r') as fout:
    #     doc = fout.readlines()
    #     for line in doc:
    #         document.append(line.strip('\n'))
    #
    # print(document)

    # documents = topicAgg.map(lambda x: x[1].tolist())
    # print(f'type of documents={type(documents)}')
    # print(documents)
    # # #wordCollect.pprint()
    # print(f'topicAgg.count()')
    # topicAgg.count().pprint()
    #topicAggPDF = topicAgg.toPandas()
    #topicAggPDF = topicAgg.foreachRDD(lambda x: x.toPandas())
    # topicAgg.foreachRDD(lambda x: x.toDF())
    # print('pandas_dataframe_converted_from_topicAgg')
    # print(type(topicAgg))
    # # RS = 25111993
    # # documents = topicAggPDF['overview'].values.astype("U")
    #
    # vectorizer = TfidfVectorizer(stop_words='english')
    # features = vectorizer.fit_transform(documents)
    # print(list(range(0, 3)))
    # sns.palplot(np.array(sns.color_palette("hls", 3)))
    # digits_proj = TSNE(random_state=RS).fit_transform(features)
    # scatter(digits_proj, model.labels_)
    # plt.savefig('digits_tsne-generated_18_cluster.png', dpi=120)
    #
    # topicAgg.map(lambda x: (x[0],freqcount(x[1]))).pprint()

    clust.foreachRDD(lambda time, rdd: q.put(rdd.collect()))
    # Run!
    ssc.start()
    ssc.awaitTermination()
