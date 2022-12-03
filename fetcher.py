#!/bin/python3

import os
#import datetime
import time
import json
import requests

def read_token(filename):
    fd = open(filename)
    line = fd.readline()
    # Remove the 'newline' character if found
    if line[len(line)-1] == '\n':
        line = line[:-1]
    fd.close()
    return line

def last_n_hrs(n):
    curr = int(round(time.time() * 1000))
    curr_minus_n = curr - n * 3600 *1000
    return curr_minus_n, curr

def main():
    # Get base directory to use relative paths
    base_dir = os.path.dirname(__file__)
    if base_dir != "":
        base_dir+="/"

    # Where to get the authorization token
    file_token=base_dir+"token"

    # Read the authorization token
    token = read_token(file_token)
    headers = {'Authorization': 'Bearer '+token, 'Content-Type': 'application/json'}

    # Get data from the last 24hrs
    min_date, max_date = last_n_hrs(24)

    query = "data.site_name:T2_US_Caltech AND ( data.file_lfn:\/store\/mc\/* OR data.file_lfn:\/store\/data\/* )"

    fields= [ 'data.file_lfn', 'data.read_bytes', 'data.file_size']

    # Number of records to retreive
    num_records = 10000

    request_data = {
            "size":num_records,
            "query":{
                "bool":{
                    "filter":[
                        {"range":{"metadata.timestamp":{"gte":min_date,"lte":max_date,"format":"epoch_millis"}}},
                        {"query_string":{
                            "analyze_wildcard":"true",
                            "query":query
                            }
                        }
                    ]
                }
            },
            "_source":fields
    }

    request = json.dumps(request_data)

    # Send the query (It takes few seconds)
    response = requests.get('https://monit-grafana.cern.ch/api/datasources/proxy/9751/_search', headers=headers, data=request)

    if response.status_code != 200:
        print("response: "+str(response))
        print("response: "+str(response.status.code))
        sys.exit(1)

    # Get the data from the response
    d = response.json()
    json_formatted_str = json.dumps(d, indent=2)
    print(json_formatted_str)

    bytes=0

    for hit in d['hits']['hits']:
      bytes+=hit['_source']['data']['read_bytes']
      
    print (bytes/1024/1024/1024)

if __name__ == "__main__":
    main()

