#!/bin/python3

import os
import sys
#import datetime
import time
import json
import requests
import re

from datetime import datetime, timezone, timedelta


def read_token(filename):
    fd = open(filename)
    line = fd.readline()
    # Remove the 'newline' character if found
    if line[len(line)-1] == '\n':
        line = line[:-1]
    fd.close()
    return line

def last_n_hrs(n):

    curr = int(round( (datetime.now(timezone.utc).timestamp() - 24*3600) * 1000))
    curr_minus_n = curr - n * 3600 *1000
    return curr_minus_n, curr

def last_n_day(n=0):

    # n = 0 yesterday full day

    current_day_start = datetime( *datetime.now(timezone.utc).timetuple()[:3] )

    day_start = current_day_start - timedelta(days=n+1)
    day_end   = current_day_start - timedelta(days=n)

#    print ("%d %d" % (day_start.timestamp()*1000, day_end.timestamp()*1000))
    return int(day_start.timestamp() * 1000), int(day_end.timestamp() * 1000)

def last_n_hour(n=0):

    # n = 0 yesterday full day

    current_hour_start = datetime( *datetime.now(timezone.utc).timetuple()[:4] )

    hour_start = current_hour_start - timedelta(hours=n+1)
    hour_end   = current_hour_start - timedelta(hours=n)

    return int(hour_start.timestamp() * 1000), int(hour_end.timestamp() * 1000)

def msts_to_human(n=0):

    ts = n / 1000
    #print (datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))
    #return ( datetime.utcfromtimestamp(ts).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    return ( datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

def msts_to_filename(n=0):
    ts = n / 1000
    return ( datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d-%H'))

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


    query = "data.site_name:T2_US_Caltech AND ( data.file_lfn:\/store\/mc\/* OR data.file_lfn:\/store\/data\/* OR data.file_lfn:\/store\/himc\/* OR data.file_lfn:\/store\/hidata\/*)"

    #query = "( data.file_lfn:\/store\/mc\/* OR data.file_lfn:\/store\/data\/* OR data.file_lfn:\/store\/himc\/* OR data.file_lfn:\/store\/hidata\/*)"

    fields= [ 'data.file_lfn', 'data.read_bytes', 'data.file_size', 'data.start_time', 'data.end_time']

    # Number of records to retreive
    num_records = 10000

    for x in range(9000,0,-1):
      min_date, max_date = last_n_hour(x)

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
      #print(json_formatted_str)


      #datetime.fromtimestamp(min_date/1000).strftime("%b %d %Y %H:%M:%S")
      #print (min_date)
      #print ( msts_to_filename(min_date) )     
      
      open( 'data/'+msts_to_filename(min_date)+'.json', 'w').write(json_formatted_str)

      total_read_bytes=0
      total_file_size=0
      for hit in d['hits']['hits']:
          data = hit['_source']['data']
          total_read_bytes+=int(data['read_bytes'])
          total_file_size+=int(data['file_size'])

      print ("%s\t%10d GiB\t%10d GiB\t%10d records" % ( msts_to_human(min_date), total_read_bytes/1024/1024/1024, total_file_size/1024/1024/1024, len(d['hits']['hits']) ) )


if __name__ == "__main__":
    main()


