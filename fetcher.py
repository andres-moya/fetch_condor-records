#!/bin/python3

import os
#import datetime
import time
import json
import requests
import re

from datetime import datetime, timezone


def read_token(filename):
    fd = open(filename)
    line = fd.readline()
    # Remove the 'newline' character if found
    if line[len(line)-1] == '\n':
        line = line[:-1]
    fd.close()
    return line

def last_n_hrs(n):

    curr = int(round(datetime.now(timezone.utc).timestamp() * 1000))
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

    fields= [ 'data.file_lfn', 'data.read_bytes', 'data.file_size', 'data.start_time', 'data.end_time']

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
#    json_formatted_str = json.dumps(d, indent=2)
#    print(json_formatted_str)

    bytes=0
    ds_bytes_2={}

    breaks1={}
    breaks2={}
    breaks3={}
    breaks4={}
    breaks5={}
    breaks6={}

    for hit in d['hits']['hits']:
      data = hit['_source']['data']

      m = re.match("^\/store\/(.*?)\/(.*?)\/(.*?)\/(.*?)\/(.*?)\/", data['file_lfn'])
      if m:
#        print(m.groups())

        cms_lfn_part0=m.group(1)
        cms_lfn_part1=m.group(2)
        cms_lfn_part2=m.group(3)
        cms_lfn_part3=m.group(4)
        cms_lfn_part4=m.group(5)

        cms_ds_part1=cms_lfn_part2
        cms_ds_part2=cms_lfn_part1
        cms_ds_part3=cms_lfn_part3
        cms_ds_part4=cms_lfn_part4

        cms_dataset="/%s/%s-%s/%s" % ( cms_ds_part1, cms_ds_part2, cms_ds_part3, cms_ds_part4)
        tstamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data['end_time']) )
        print ("%s: %s" % (tstamp, cms_dataset))
      else:
        print (data['file_lfn'])

      bytes+=int(data['read_bytes'])

      # we need to count lfn parts:
      #    part0 part1 part2 part3 part4
      # 1  *     part1 *     *     *
      # 2  *     part1 *     *     part4
      # 3  *     *     *     *     part4
      # 4  part0 part1 *     *     *
      # 5  part0 part1 *     *     part4
      # 6  part0 *     *     *     part4 

      if (cms_lfn_part1) not in breaks1:
        breaks1[(cms_lfn_part1)]=0

      if (cms_lfn_part1, cms_lfn_part4) not in breaks2:
        breaks2[(cms_lfn_part1, cms_lfn_part4)]=0

      if (cms_lfn_part4) not in breaks3:
        breaks3[(cms_lfn_part4,)]=0

      if (cms_lfn_part0, cms_lfn_part1) not in breaks4:
        breaks4[(cms_lfn_part0, cms_lfn_part1)]=0

      if (cms_lfn_part0, cms_lfn_part1, cms_lfn_part4) not in breaks5:
        breaks5[(cms_lfn_part0, cms_lfn_part1, cms_lfn_part4)]=0

      if (cms_lfn_part0, cms_lfn_part4) not in breaks6:
        breaks6[(cms_lfn_part0, cms_lfn_part4)]=0

      breaks1[(cms_lfn_part1)]+=int(data['read_bytes'])
      breaks2[(cms_lfn_part1, cms_lfn_part4)]+=int(data['read_bytes'])
      breaks3[(cms_lfn_part4,)]+=int(data['read_bytes'])
      breaks4[(cms_lfn_part0, cms_lfn_part1)]+=int(data['read_bytes'])
      breaks5[(cms_lfn_part0, cms_lfn_part1, cms_lfn_part4)]+=int(data['read_bytes'])
      breaks6[(cms_lfn_part0, cms_lfn_part4)]+=int(data['read_bytes'])

    for i in dict(sorted(breaks1.items(), key=lambda item: item[1])):
      print ("%10d GiB\t /store/*/%s/*/*/*" % (breaks1[i]/1024/1024/1024, i ))

    for i in dict(sorted(breaks2.items(), key=lambda item: item[1])):
      print ("%10d GiB\t /store/*/%s/*/*/%s" % ((breaks2[i]/1024/1024/1024,) + i) )

    for i in dict(sorted(breaks3.items(), key=lambda item: item[1])):
      print ("%10d GiB\t /store/*/*/*/*/%s" % ((breaks3[i]/1024/1024/1024,) + i) )

    for i in dict(sorted(breaks4.items(), key=lambda item: item[1])):
      print ("%10d GiB\t /store/%s/%s/*/*/*" % ((breaks4[i]/1024/1024/1024,) + i) )

    for i in dict(sorted(breaks5.items(), key=lambda item: item[1])):
      print ("%10d GiB\t /store/%s/%s/*/*/%s" % ((breaks5[i]/1024/1024/1024,) + i) )

    for i in dict(sorted(breaks6.items(), key=lambda item: item[1])):
      print ("%10d GiB\t /store/%s/*/*/*/%s" % ((breaks6[i]/1024/1024/1024,) + i) )


if __name__ == "__main__":
    main()

