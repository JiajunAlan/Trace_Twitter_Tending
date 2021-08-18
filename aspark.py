"""
        York U EECS4415
        jiajun Zhou 215231517
        Xianzun Meng 215183858

"""

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 5)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter",9009)
#add your tracks here
#track = ['#italia','#covid', '#frenchgp' , '#nintendo', '#fathersday']


# split each tweet into words
#first lower the entire input line.
words = dataStream.flatMap(lambda line: line.lower().split(" "))

# filter the words to get 5 hashtags we want to count.
hashtags = words.filter(lambda w: w in track)

# map each hashtag to be a pair of (hashtag,1)
hashtag_counts = hashtags.map(lambda x: (x, 1))

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)

# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # sort counts (desc) in this time instance and take top 10
        sorted_rdd = rdd.sortBy(lambda x:x[1], False)
        top5 = sorted_rdd.take(5)

        #open a file
        f = open("aresult.txt", "w")

        # print it nicely
        for tag in top5:
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
