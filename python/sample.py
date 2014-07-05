#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Sample revisions within the year 2011
"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

# Enable boto logging for EMR debug
import logging
logging.basicConfig(filename='boto.log',level=logging.INFO)

# Settings for running at the cluster
import os
os.environ['HADOOP_HOME']='/opt/cloudera/parcels/CDH'
os.environ['HADOOP_MAPRED_HOME']='/opt/cloudera/parcels/CDH-4.6.0-1.cdh4.6.0.p0.26/lib/hadoop-0.20-mapreduce'

import json

# Set up begin and end time
begin = 1293840001000
end = 1373673599000

class MRSampleRevisions(MRJob):

    INTERNAL_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, pid, line):
        obj = json.loads(line)
        timestamp = long(obj['timestamp'])
        if timestamp >= begin and timestamp < end:
            yield (None,line)

    def reducer(self, pid, lines):
        for line in lines:
            yield (None,line)

if __name__ == '__main__':
    MRSampleRevision.run()
