import requests
import json
import kagglehub

import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

import os
import threading
import json
from flask import Flask, Response

app = Flask(__name__)
port = "5000"

dasgroup_rba_dataset_path = kagglehub.dataset_download('dasgroup/rba-dataset')

print(f'Data source import complete: {dasgroup_rba_dataset_path}')
chunk_index = 0

for dirname, _, filenames in os.walk(dasgroup_rba_dataset_path):
    for filename in filenames:
        print(os.path.join(dirname, filename))


@app.route('/', methods=['GET'])
def hello():
    return "Hello, Flask on Google Colab!"

@app.route('/data', methods=['GET'])
def get_data():
    global chunk_index
    chunksize=1000
    
    chunk_generator = pd.read_csv(dasgroup_rba_dataset_path+"\\rba-dataset.csv", chunksize=chunksize)
    for _ in range(chunk_index):
        next(chunk_generator)

    try:
        # Get the next chunk
        chunk = next(chunk_generator)
        chunk_index += 1  
        
        return Response(
            json.dumps(chunk.to_dict(orient='records')),
            mimetype='application/json'
        )
    except StopIteration:
        chunk_index = 0
        return Response(
            json.dumps({"message": "No more data available"}),
            mimetype='application/json'
        )
if __name__=="__main__":
    threading.Thread(target=app.run).start()