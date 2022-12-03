#!/bin/python3

import requests
import json
import csv
import pandas as pd
from datetime import datetime as dt
from dateutil import parser
import time
import subprocess
import logging
import os
import sys
import pdb
import re

# Show all columns when printing a pandas DataFrame
pd.options.display.max_columns = None
pd.set_option('display.max_colwidth', None)

# Craetes a list of the fileds with a file as input
def read_fields(filename):
    fields = []
    fd = open(filename)
    lines = fd.readlines()
    for line in lines:
        fields.append("data."+line[:-1])
    fd.close()
    return fields

def remove_dot_data(fields):
    fields_no_data=[]
    for field in fields:
        field_no_data = field.replace("data.","")
        fields_no_data.append(field_no_data)
    return fields_no_data

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

    # logging format and level
    #logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(levelname)s - %(message)s', datefmt='%Y%m%d %H:%M:%S')
    hdlr = logging.FileHandler(base_dir+"out.log")
    formatter = logging.Formatter('%(asctime)s  %(levelname)s - %(message)s', datefmt='%Y%m%d %H:%M:%S')
    hdlr.setFormatter(formatter)
    log.addHandler(hdlr)
    log.setLevel(logging.INFO)


    log.info("================================================================================")
    log.info("=========                      Starting                           ==============")
    log.info("================================================================================")
    # Where to get the authorization token
    file_token=base_dir+"token"

    # Read the authorization token
    token = read_token(file_token)
    headers = {'Authorization': 'Bearer '+token, 'Content-Type': 'application/json'}

    # Get data from the last 24hrs
    min_date, max_date = last_n_hrs(24)

    # The query on lucene syntaxis
    query = "data.Type:analysis AND data.Status:Completed AND data.ExitCode:0 AND data.CMSSite:T2_US_Caltech AND data.InputData:Offsite"

    # Read the list of fields to retrieve from a file
    #fields= ['data.DESIRED_CMSDataLocations', 'data.CpuEff', 'data.CMSPrimaryProcessedDataset', 'data.BytesRecvd', 'data.ChirpCMSSWReadBytes']
    fields= [ 'data.BytesRecvd', 'data.CpuEff', 'data.CRAB_DataBlock']

    fields_no_data = remove_dot_data(fields)

    cache_regs=[
      "RunIISummer19UL16.*\/MINIAODSIM#",
      "RunIISummer19UL17.*\/MINIAODSIM#",
      "RunIISummer19UL18.*\/MINIAODSIM#",
      "RunIISummer20UL16.*\/MINIAODSIM#",
      "RunIISummer20UL17.*\/MINIAODSIM#",
      "RunIISummer20UL18.*\/MINIAODSIM#",
      "Run2016.*UL2016.*\/MINIAOD#",
      "Run2017.*UL2017.*\/MINIAOD#",
      "Run2018.*UL2018.*\/MINIAOD#"
    ]

    cache_regs_combined = '(?:% s)' % '|'.join(cache_regs)

    # Number of records to retreive
    num_records = 10000

    log.debug("Query: "+query)

    data = {
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

    data_string = json.dumps(data)

    # Send the query (It takes few seconds)
    response = requests.get('https://monit-grafana.cern.ch/api/datasources/proxy/8787/_search', headers=headers, data=data_string)

    if response.status_code != 200:
        print("response: "+str(response.status.code))
        sys.exit(1)

    # Get the data from the response
    d = response.json()
    json_formatted_str = json.dumps(d, indent=2)
    print(json_formatted_str)


    data_file = open('data_file.csv', 'w')
    csv_writer = csv.writer(data_file,delimiter=';')
    
    print (cache_regs_combined)

    count = 0
 
    for hit in d['hits']['hits']:
      if count == 0:
 
        # Writing headers of CSV file
        header = hit['_source']['data'].keys()
        csv_writer.writerow(header)
        count += 1

      # Writing data of CSV file
      print (hit['_source']['data']['CRAB_DataBlock'])
      if re.match(cache_regs_combined, hit['_source']['data']['CRAB_DataBlock']):
          hit['_source']['data']['match']=1
      else:
          hit['_source']['data']['match']=0
      csv_writer.writerow(hit['_source']['data'].values())

      #print (x['_source']['data'])

    #pdb.set_trace()

    # Create a pandas DataFrame with the data retreived
    #clean_records=[]
    #no_data_count=0
    #for record in d['hits']['hits']:
    #    try:
    #        clean_record= record['_source']['data']
    #        clean_records.append(clean_record)
    #    except:
    #        no_data_count = no_data_count +1

    #df = pd.DataFrame(clean_records)
    #print(df)

log = logging.getLogger()
if __name__ == "__main__":
    main()
