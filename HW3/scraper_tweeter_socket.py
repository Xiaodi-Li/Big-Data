import socket
import sys, os
from threading import *
from _thread import *
import requests
import requests_oauthlib
import tweepy
from tweepy import OAuthHandler
import json
#import oauth2 as oauth
from datetime import datetime
#Variables that contains the user credentials to access Twitter API
# Replace it with your credentials
# access_token = "582342005-QGM3VSdAL1cjAPzL6jxxxxxxxxxx"
# access_token_secret = "keEVSlaNz5fegUq8ytMrTXq62pxxxxxxxxxx"
# consumer_key = "PjlYiBasD06wnMOxxxxxxx"
# consumer_secret = "EXVZnDVb3wLA6KhwOfp9weBSngJxxxxcx"
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
auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret,access_token, access_token_secret)


HOST = ''   # Symbolic name meaning all available interfaces
PORT = 9999 # Arbitrary non-privileged port

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print ('Socket created')

#Bind socket to local host and port
try:
    s.bind((HOST, PORT))
except socket.error as msg:
    print ('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
    sys.exit()

print ('Socket bind complete')
import time
#Start listening on socket
s.listen(10)
print ('Socket now listening')

#Function for handling connections. This will be used to create threads
def clientthread(conn):
    url='https://stream.twitter.com/1.1/statuses/filter.json'
    #
    data      = [('language', 'en'), ('locations', '-130,-20,100,50')]
    #,('track','christmas')
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in data])
    response  = requests.get(query_url, auth=auth, stream=True)
    print(query_url, response) # 200 <OK>
    count = 0
    for line in response.iter_lines():  # Iterate over streaming tweets
        try:
            if count > 10000000:
                break
            post= json.loads(line.decode('utf-8'))
            #print(f'line={line}')
            contents = [post['text'], post['coordinates'], post['place']]
            #print(f'contents={contents}')
            count+= 1
            line = line.decode('utf-8')
            conn.send(str.encode(line+'\n'))
            #time.sleep(1)
            print (str(datetime.now())+' '+'count:'+str(count))
            # flush output here to force SIGPIPE to be triggered
            # while inside this try block.
            sys.stdout.flush()
        except (BrokenBarrierError, IOError):
            # Python flushes standard streams on exit; redirect remaining output
            # to devnull to avoid another BrokenPipeError at shutdown
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
            sys.exit(1)  # Python exits with error code 1 on EPIPE
            # print('BrokenPipeError caught', file=sys.stderr)
            # #print(f'line={line}')
            # e = sys.exc_info()[0]
            # print( "Error: %s" % e )

    print('done', file=sys.stderr)
    sys.stderr.close()
    conn.close()


#now keep talking with the clients
loop = 0
while 1:
    if loop>1:
        break
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    print ('Connected with ' + addr[0] + ':' + str(addr[1]))

    #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread ,(conn,))
    loop += 1
s.close()

