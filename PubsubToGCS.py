import base64
import json
import numpy as np
import pandas as pd
import logging
from google.cloud import storage

class PubsubToGCS:
    def __init__(self):
        self.bucket_name = 'akshat_egen_bucket1'

    def extract_attributes(self, data):
        transform_tweet = {
            "Tweet_id": data['id'], 
            "Tweet_time":data['created_at'], 
            "Tweet_source":data['source'], 
            "User_id":data['user']['id'], 
            "User_name":data['user']['name'].replace(',' , ''),
            "Location":data['user']['location'].replace(',' , '') if data['user']['location'] else None,
            "Place": data["place"]["country_code"] if data["place"] else None,
            "User_follower_count":data['user']['followers_count'], 
            "User_friend_count":data['user']['friends_count']}
        
        if 'retweeted_status' in data:
            try:
                transform_tweet['text'] = data['retweeted_status']['extended_tweet']['full_text']
            except KeyError:
                transform_tweet['text'] = data['retweeted_status']['text']
        else:
            try:
                transform_tweet['text'] = data['extended_tweet']['full_text']
            except KeyError:
                transform_tweet['text'] = data['text']
        return transform_tweet


    
    def structure_payload(self,message) -> pd.DataFrame:
        try:
            df=pd.DataFrame(message, index=[1])
            if not df.empty:
                logging.info("DataFrame created")
            else:
                logging.info("Empty DataFrame created")
            return df
        except Exception as e:
            logging.error(f"Error creating DataFrame {str(e)}")
            raise

    def upload_to_storage(self, df, filename):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(f'twitter_message/{filename}.csv')
        blob.upload_from_string(data=df.to_csv(index=False), content_type='text/csv')
        logging.info('Sucessfully written file to Cloud storage.')

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_dict = json.loads(pubsub_message)
    pubsub_to_gcs = PubsubToGCS()
    filtered_data = pubsub_to_gcs.extract_attributes(message_dict)
    data_frame = pubsub_to_gcs.structure_payload(filtered_data)
    tweet_id = filtered_data['Tweet_id']
    pubsub_to_gcs.upload_to_storage(data_frame, "Twitter_data_" + str(tweet_id))