from flask import Flask
from flask import request
from flask import jsonify
import json
import pickle
import numpy as np
from sklearn.metrics import confusion_matrix

import boto3, botocore
import os
import io
import zipfile

s3 = boto3.resource('s3')
download_file_to_ec2 = True

app = Flask(__name__)

bucket_name = 'storage-733'
key = 'svm_model.pickle'
env_local = bool(int(os.environ.get('ENV_LOCAL', True)))

print('Use local model = ', env_local)

def download_model():
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print('Bucket doesn\'t exist' )
        raise

    try:
        body = s3.Object(bucket_name, key).get()['Body'].read()
        f = io.BytesIO(body)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        raise
    return f

if env_local is True:
    pickle_file = open('svm_model.pickle', 'rb')
else:    
    pickle_file = download_model()

loaded_model = pickle.load( pickle_file )

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    print(data)
    # print(json.dumps(data["features"]))
    y_predict = loaded_model.predict(data["features"])
    print(y_predict, type(y_predict))
    #tn, fp, fn, tp = confusion_matrix(y_test_binary, y_predict).ravel()
    #specificity = tn / (tn+fp)
    #sensivity =  tp / (tp+fn)
    return jsonify(dict(prediction=y_predict.tolist()))
