#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This is a patch to the buggy json2anchor python script (now fixed) that forgets
to put a tab between the timestamp and the source ID

Output anchor in format (separated by TAB)
[timestamp] [source ID] [revision ID] [previous revision ID] [anchor text] [target title]
"""

from mrjob.job import MRJob
from mrjob.protocol import RawProtocol
from mrjob.protocol import RawValueProtocol

# Enable boto logging for EMR debug
import logging
logging.basicConfig(filename='boto.log',level=logging.INFO)

# Settings for running at the cluster
import os
os.environ['HADOOP_HOME']='/opt/cloudera/parcels/CDH'
os.environ['HADOOP_MAPRED_HOME']='/opt/cloudera/parcels/CDH-4.6.0-1.cdh4.6.0.p0.26/lib/hadoop-0.20-mapreduce'


class MRAnchorFix(MRJob):

    def isnumber(self,txt):
        try:
            long(txt)
            return True
        except ValueError:
            return False
        
    INPUT_PROTOCOL = RawProtocol
    INTERNAL_PROTOCOL = RawProtocol
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def mapper(self,pid,line):
        # Patch fix: the first 13 characters in pid is timestamp
        ts = pid[:13]

        # Skip mal-formed anchors
        if self.isnumber(ts):
            pageid = pid[13:]
            line = pageid + '\t' + line      
            yield (ts,line)
            

    def reducer(self,tsid,lines):
        for line in lines:
            line = tsid + '\t' + line
            yield (None,line)

if __name__ == "__main__":
    MRAnchorFix.run()
