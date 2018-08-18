import tweepy
from TwitterConfig import TwitterConfig as cfg
import socket

"""
TwitterSocketStreamListener will listen for a topic, and if we have a socket it will send
the text of the tweet on the socket.  This will allow another process to connect to the socket
to get the tweet text.

In this repo, the example is this TwitterSocketStreamListener will send tweet text to a 
Spark streaming listener.

"""
class TwitterSocketStreamListener(tweepy.StreamListener):

    def __init__(self, csocket=None):
        super(TwitterSocketStreamListener, self).__init__()
        self.client_socket = csocket

    def _socket_send(self, data):
        if self.client_socket is None:
            return

        try:
            t = data.encode('utf-8')
            # print(t)
            self.client_socket.send(t)
        except Exception as exc:
            print(exc)

        return True

    def on_status(self, status):
        try:
            raw_tweet = status._json
            tweet_text = raw_tweet['text']
            user = raw_tweet['user']
            screen_name = user['screen_name']

            # print(screen_name)
            self._socket_send(tweet_text)

        except Exception as exc:
            print(exc)

    def on_error(self, status):
        #print(status)
        return True

if __name__ == '__main__':
    access_token = cfg.access_token
    access_token_secret = cfg.access_token_secret
    consumer_key = cfg.consumer_key
    consumer_secret = cfg.consumer_secret

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    filter_words = ['maga']

    # create socket:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        host = 'localhost'
        port = 9999
        s.bind((host, port))

        s.listen(200)
        print("Waiting to accept a connection....")
        c,addr = s.accept()
        with c:
            print('Connected by', addr)
            l = TwitterSocketStreamListener(csocket=c)

            stream = tweepy.Stream(auth=auth, listener=l)

            # filters twitter streams to capture data by keywords
            stream.filter(track=filter_words)

    print("Ending Twitter Listener....")