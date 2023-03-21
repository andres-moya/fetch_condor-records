#!/bin/python3

import os
import glob
import sys
import time
import json
import re
import pprint
import collections
import pdb
from datetime import datetime
import copy

#import pdb; pdb.set_trace()


# 1. count file accessed times and record its size
# 2.       # we need to count lfn parts:
#           #    part0 part1 part2 part3 part4
#           # 1  *     part1 *     *     *
#           # 2  *     part1 *     *     part4
#           # 3  *     *     *     *     part4
#           # 4  part0 part1 *     *     *
#           # 5  part0 part1 *     *     part4
#           # 6  part0 *     *     *     part4
# 
#

#file=/store/mc/RunIISummer20UL16RECO/DoubleMuonGun_Pt3To150/GEN-SIM-RECO/NoPU_106X_mcRun2_asymptotic_v13-v2/230001/00CAF73B-59C5-0642-9F9A-19ACD8541A91.root
#Dataset: /DoubleMuonGun_Pt3To150/RunIISummer20UL16RECO-NoPU_106X_mcRun2_asymptotic_v13-v2/GEN-SIM-RECO
#Dataset: /<PART1_______________>/<PART2>          	-<PART3>                       	/<PART4>
#/store/PART0{mc,data,himc,hidata}/<PART2>/<PART1>/<PART3>/<PART4>/<ID_INCR_we_dont_care>/<FILENAME_we_dont_care>
#PART0 - mc,data,himc,hidata

#           "start_time": 1679094157,
#            "end_time": 1679097625,
#            "file_lfn": "/store/mc/RunIISummer20UL18MiniAODv2/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/MINIAODSIM/106X_upgrade2018_realistic_v16_L1v1-v2/270005/A2F29F36-F9DA-5743-BCB9-FC39E4753E63.root",
#            "read_bytes": 1123358519,
#            "file_size": 6502578
#

class counter():
    def __init__(self):
        self.filecounter={}
        self.filecounter_year={}
        self.filecounter_month={}
        self.filecounter_day={}
        self.filecounter_hour={}
        self.exclude_paths=['/store/mc/HC']
        self.default_counters={'hits': 0, 'file_size': 0}

    def exluded_lfn_path(self, lfn):
        for i in self.exclude_paths:
            if lfn.startswith(i):
                return True
        return False

    def record_entry(self, data):
        #print(data)


        #datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        if self.exluded_lfn_path(data['file_lfn']): return

        #pdb.set_trace()
        start_time=datetime.utcfromtimestamp(data['start_time'])

        self.filecounter.setdefault(data['file_lfn'], copy.deepcopy(self.default_counters))

        self.filecounter[data['file_lfn']]['hits']+=1
        self.filecounter[data['file_lfn']]['file_size']=data['file_size']



        dict_key=f"{start_time.year}"
        self.filecounter_year.setdefault(dict_key, {})
        self.filecounter_year[dict_key].setdefault(data['file_lfn'], copy.deepcopy(self.default_counters))
        self.filecounter_year[dict_key][data['file_lfn']]['hits']+=1
        self.filecounter_year[dict_key][data['file_lfn']]['file_size']=data['file_size']

        dict_key=f"{start_time.year}-{start_time.month}"
        self.filecounter_month.setdefault(dict_key, {})
        self.filecounter_month[dict_key].setdefault(data['file_lfn'], copy.deepcopy(self.default_counters))
        self.filecounter_month[dict_key][data['file_lfn']]['hits']+=1
        self.filecounter_month[dict_key][data['file_lfn']]['file_size']=data['file_size']

        dict_key=f"{start_time.year}-{start_time.month}-{start_time.day}"
        self.filecounter_day.setdefault(dict_key, {})
        self.filecounter_day[dict_key].setdefault(data['file_lfn'], copy.deepcopy(self.default_counters))
        self.filecounter_day[dict_key][data['file_lfn']]['hits']+=1
        self.filecounter_day[dict_key][data['file_lfn']]['file_size']=data['file_size']

        dict_key=f"{start_time.year}-{start_time.month}-{start_time.day}-{start_time.hour}"
        self.filecounter_hour.setdefault(dict_key, {})
        self.filecounter_hour[dict_key].setdefault(data['file_lfn'], copy.deepcopy(self.default_counters))
        self.filecounter_hour[dict_key][data['file_lfn']]['hits']+=1
        self.filecounter_hour[dict_key][data['file_lfn']]['file_size']=data['file_size']


    def print(self, filecounter):
        #pprint.pprint(self.filecounter)
        #max(self.filecounter.iterkeys(), key=lambda k: self.filecounter[k])
        #s=collections.Counter(self.filecounter)
        #pprint.pprint(s.most_common(50))

        c = collections.Counter()
        total_file_size=0
        for file_lfn, values in filecounter.items():
            total_file_size+=values['file_size']
            c[file_lfn] += values['hits']
            
        #pprint.pprint(c.most_common(10))
        pprint.pprint(total_file_size/1000000000)

mycounter=counter()
def main():

    datapath = 'data'

    files=[]

    for filename in glob.glob(os.path.join(datapath, '*.json')):
        #print(filename)

        with open(os.path.join(os.getcwd(), filename), 'r') as f:
            data = json.load(f)
            #data_formatted_str = json.dumps(data['hits']['hits'], indent=2)
            #print(data_formatted_str)
            for item in data['hits']['hits']:
                #print( "%s %s %s " % ( item['_source']['data'][ 'file_lfn'], item['_source']['data'][ 'read_bytes'], item['_source']['data'][ 'file_size']))
                mycounter.record_entry( item['_source']['data'] ) 

    #print(mycounter.filecounter_hour.keys())
    #mycounter.print()
    for i in sorted(mycounter.filecounter_day.keys()):
    
        print(i)
        mycounter.print(mycounter.filecounter_day[i])

if __name__ == "__main__":
    main()

