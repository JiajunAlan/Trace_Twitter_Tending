"""
        York U EECS4415 
        jiajun Zhou 215231517
        Xianzun Meng 215183858

"""

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

import sys
import requests

#initialize the ntlk sentiment
nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 30)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter",9009)


"""
check tweets belonging function
"""
def has_ferrari(line):
    line = str(line)
    track = ['ferrari','sainz', 'leclerc', 'cl16', 'cs55']
    for tag in track:
        if tag in line:
            return True
    return False

def has_mercedes(line):
    track = ['hamilton', 'mercedes', 'bottas', 'vb77', 'lh44']
    for tag in track:
        if tag in line:
            return True
    return False


def has_redbull(line):
    track = ['redbull', 'verstappen', 'perez', 'sp11', 'checoperez', 'mv33']
    for tag in track:
        if tag in line:
            return True
    return False

def has_alpine(line):
    track = ['alpine', 'renault', 'ocon', 'alonso', 'fa14', 'eo31']
    for tag in track:
        if tag in line:
            return True
    return False

def has_astonmartin(line):
    track = ['astonmartin', 'vettel', 'stroll', 'amf1', 'sv5', 'ls18']
    for tag in track:
        if tag in line:
            return True
    return False



#first lower the entire input line. and split by each tweet
tweets = dataStream.flatMap(lambda line: line.lower().split("\n"))

#filter the tewwts by topics we decide.
ferrari_tweets = tweets.filter(lambda x: has_ferrari(x))
#find the sentiment of these tweets
ferraripositive = ferrari_tweets.map(lambda word: ('ferrariPositive', 1) if sia.polarity_scores(str(word))["compound"] > 0 else ('ferrariPositive', 0))
ferrarineutral = ferrari_tweets.map(lambda word: ('ferrariNeutral', 1) if sia.polarity_scores(str(word))["compound"] == 0 else ('ferrariNeutral', 0))
ferrarinegative = ferrari_tweets.map(lambda word: ('ferrariNegative', 1) if sia.polarity_scores(str(word))["compound"] <0 else ('ferrariNegative', 0))
#union the three mapping result we get
ferrari_sentiments = ferraripositive.union(ferrarineutral)
ferrari_sentiments = ferrari_sentiments.union(ferrarinegative)

mercedes_tweets = tweets.filter(lambda x: has_mercedes(x))
mercedespositive = mercedes_tweets.map(lambda word: ('mercedesPositive', 1) if sia.polarity_scores(str(word))["compound"] > 0 else ('mercedesPositive', 0))
mercedesneutral = mercedes_tweets.map(lambda word: ('mercedesNeutral', 1) if sia.polarity_scores(str(word))["compound"] == 0 else ('mercedesNeutral', 0))
mercedesnegative = mercedes_tweets.map(lambda word: ('mercedesNegative', 1) if sia.polarity_scores(str(word))["compound"] <0 else ('mercedesNegative', 0))
mercedes_sentiments = mercedespositive.union(mercedesneutral)
mercedes_sentiments = mercedes_sentiments.union(mercedesnegative)
#union the each topic results to get the final allSentiments
allSentiments = ferrari_sentiments.union(mercedes_sentiments)

redbull_tweets = tweets.filter(lambda x: has_redbull(x))
redbullpositive = redbull_tweets.map(lambda word: ('redbullPositive', 1) if sia.polarity_scores(str(word))["compound"] > 0 else ('redbullPositive', 0))
redbullneutral = redbull_tweets.map(lambda word: ('redbullNeutral', 1) if sia.polarity_scores(str(word))["compound"] == 0 else ('redbullNeutral', 0))
redbullnegative = redbull_tweets.map(lambda word: ('redbullNegative', 1) if sia.polarity_scores(str(word))["compound"] <0 else ('redbullNegative', 0))
redbullsentiments = redbullpositive.union(redbullneutral)
redbullsentiments = redbullsentiments.union(redbullnegative)
allSentiments = allSentiments.union(redbullsentiments)


alpine_tweets = tweets.filter(lambda x: has_alpine(x))
alpinepositive = alpine_tweets.map(lambda word: ('alpinePositive', 1) if sia.polarity_scores(str(word))["compound"] > 0 else ('alpinePositive', 0))
alpineneutral = alpine_tweets.map(lambda word: ('alpineNeutral', 1) if sia.polarity_scores(str(word))["compound"] == 0 else ('alpineNeutral', 0))
alpinenegative = alpine_tweets.map(lambda word: ('alpineNegative', 1) if sia.polarity_scores(str(word))["compound"] <0 else ('alpineNegative', 0))
alpinesentiments = alpinepositive.union(alpineneutral)
alpinesentiments = alpinesentiments.union(alpinenegative)
allSentiments = allSentiments.union(alpinesentiments)

astonmartin_tweets = tweets.filter(lambda x: has_astonmartin(x))
astonmartinpositive = astonmartin_tweets.map(lambda word: ('astonmartinPositive', 1) if sia.polarity_scores(str(word))["compound"] > 0 else ('astonmartinPositive', 0))
astonmartinneutral = astonmartin_tweets.map(lambda word: ('astonmartinNeutral', 1) if sia.polarity_scores(str(word))["compound"] == 0 else ('astonmartinNeutral', 0))
astonmartinnegative = astonmartin_tweets.map(lambda word: ('astonmartinNegative', 1) if sia.polarity_scores(str(word))["compound"] <0 else ('astonmartinNegative', 0))
astonmartinsentiments = astonmartinpositive.union(astonmartinneutral)
astonmartinsentiments = astonmartinsentiments.union(astonmartinnegative)
allSentiments = allSentiments.union(astonmartinsentiments)


#reduce the allSentiments
sentimentCounts = allSentiments.reduceByKey(lambda x,y: x+y)


# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = sentimentCounts.updateStateByKey(aggregate_tags_count)

# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # sort by key aplhabet. and output to terminal and a file for plot
        sorted_rdd = rdd.sortBy(lambda x:x[0], False)
        top50 = sorted_rdd.take(50)

        #open a file
        f = open("bresult.txt", "w")

        # print it nicely
        for tag in top50:
            print('{:<40} {}'.format(tag[0], tag[1]))
            #send to a file
            result = str(tag[0]) + '\t' + str(tag[1]) + '\n'

            #write our result to file
            f.write(result)

        #close our file
        f.close()

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# do this for every single interval
hashtag_totals.foreachRDD(process_interval)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
