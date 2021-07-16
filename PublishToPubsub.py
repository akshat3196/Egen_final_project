import os
import tweepy
import json
import logging
from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future
import socket

#Getting twitter auth credentials
f = open('twitter_api_cred.json',)
twitter_cred = json.load(f)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "secret.json"


class PublishToPubsub(tweepy.StreamListener):
    def __init__(self):
        super().__init__()
        self.project_id = 'sound-yew-317214'
        self.topic_id = 'twitter-stream'
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []



    def get_callback(self, publish_future: Future ,data:str) -> callable:
        def callback(publish_future):
            try:
                logging.info(publish_future.result(timeout = 60))
            except futures.TimeoutError:
                logging.error(f"Publishing {data} timed out.")
        return callback
    
    def PublishToTopic(self, message: str):
        publish_future = self.publisher_client.publish(self.topic_path,json.dumps(message).encode("utf-8"))
        publish_future.add_done_callback(self.get_callback(publish_future,message))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures, return_when = futures.ALL_COMPLETED)
        
    def on_status(self, message):
        self.PublishToTopic(message._json)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            print("Twitter maximum api hit rate reached")
            return False

def send_tweets():
    print("Entered send_tweets")
    auth = tweepy.OAuthHandler(twitter_cred['Api_key'], twitter_cred['Api_secret'])
    auth.set_access_token(twitter_cred['Access_token'], twitter_cred['Access_token'])
        
    stream = tweepy.Stream(auth=auth, listener=PublishToPubsub())
    stream.filter(track=["covid delta","covid-19 delta", "covid 19 delta"], languages=['en'])

if __name__ == "__main__":
    
    
    
    new_skt = socket.socket()         # initiate a socket object
    host = "0.0.0.0"     # local machine address
    port = 8080                 # specific port for my appication
    new_skt.bind((host, port))        # Binding host and port

    print("Now listening on port: %s" % str(port))
    
    send_tweets()

