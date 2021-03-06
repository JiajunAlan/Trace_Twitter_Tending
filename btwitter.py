"""
    York U EECS4415
    jiajun Zhou 215231517
    Xianzun Meng 215183858

"""

# from __future__ import absolute_import, print_function

import socket
import sys
import json


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream

# Replace the values below with yours
consumer_key="#########################"
consumer_secret="##################################################"
access_token="##################################################"
access_token_secret="##################################################"


class TweetListener(StreamListener):
    """ A listener that handles tweets received from the Twitter stream.

        This listener prints tweets and then forwards them to a local port
        for processing in the spark app.
    """

    def on_data(self, data):
        """When a tweet is received, forward it"""
        try:

            global conn

            # load the tweet JSON, get pure text
            full_tweet = json.loads(data)
            tweet_text = full_tweet['text']

            # print the tweet plus a separator
            print ("------------------------------------------")
            print(tweet_text + '\n')

            # send it to spark
            conn.send(str.encode(tweet_text + '\n'))
        except:

            # handle errors
            e = sys.exc_info()[0]
            print("Error: %s" % e)


        return True

    def on_error(self, status):
        print(status)



# ==== setup local connection ====

# IP and port of local machine or Docker
TCP_IP = socket.gethostbyname(socket.gethostname()) # returns local IP
TCP_PORT = 9009

# setup local connection, expose socket, listen for spark app
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")

# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting getting tweets.")


# ==== setup twitter connection ====
listener = TweetListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, listener)

# setup search terms
# track = ['#ferrari', '#essereferrari', '#scuderiaferrari' , '#sainz', '#carlossainz', '#leclerc', '#charlesleclerc', '#ferrarif1', '#cs55', '#cl16',
#         '#hamilton', '#lewishamilton', '#mercedesf1', '#mercedes', '#mercedesamg', '#bottas', '#valtteribottas', '#vb77', '#lh44', '#mercedes-amg',
#         '#redbullracing', '#redbullf1', '#redbullracingf1', '#maxverstappen', '#verstappen', '#sergioperez', '#perezf1', '#sp11', '#checoperez', '#mv33',
#         '#alpine', '#alpinef1', '#alpineracing', '#renaultf1', '#ocon', '#alonso', '#fernandoalonso', '#estebanocon', '#fa14', '#eo31',
#         '#astonmartinracing', '#astonmartinf1', '#astonmartin', '#sebastianvettel', '#vettel', '#lancestroll', '#amf1', '#sv5', '#ls18', '#f1stroll']
# language = ['en']
#locations = [-130,-20,100,50]

# get filtered tweets, forward them to spark until interrupted
try:
    stream.filter(track=track, languages=language)#, locations=locations)
except KeyboardInterrupt:
    s.shutdown(socket.SHUT_RD)
