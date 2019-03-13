import boto3
import json
import joblib
import numpy as np
import pandas as pd
import sklearn as sk
from pandas.io.json import json_normalize


class room_pred:
    def __init__(self,reload=True):
        #load 
        self.__get_model(reload)
        
    def __get_s3files(self,bucket_name,path,files):
      
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)

        for file in files:
            print bucket_name+path+file
            obj = bucket.Object(path+file)
            with open('./res/'+file, 'wb') as data:
                obj.download_fileobj(data)

    def __get_model(self,reload=True):

        if reload:
            self.__get_s3files('intra-home-data','homelab_dss/dss_models/room_pred/',['room_pred.pkl.gzip','target_map.json','columns_list.json'])
        
        self.clf = joblib.load('./res/room_pred.pkl.gzip')
        with open('./res/target_map.json') as f:
            target_map = json.load(f)
        self.target_map = {int(k):v for k,v in target_map.items()}

        with open('./res/columns_list.json') as f:
            self.columns = json.load(f)

    def score_room(self,data):
        df = pd.DataFrame(columns=self.columns)
        df = df.append(json_normalize(data),ignore_index=True)
        df = df.filter(regex=r'\.distance|\.rssi')
        df[df.filter(like='.distance').columns] = df[df.filter(like='.distance').columns].fillna(100.0)
        df[df.filter(like='.rssi').columns] = df[df.filter(like='.rssi').columns].fillna(-100.0)
        df = df[self.columns]

        _predictions = self.clf.predict(df)
        predictions = pd.Series(data=_predictions, index=df.index, name='predicted').map(self.target_map)
        
        df = df.join(predictions, how='left')
        return predictions[0]