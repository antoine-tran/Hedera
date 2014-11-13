#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2014 Anh Tuan Tran - ttran@L3S.de

Python scripts to extract various information from Wikipedia revision history. Project details: http://antoine-tran.github.io/hedera

=================================================
THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.


Experiments for WWW2015: Sample revisions within one month and 
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

# 1 Jan 2011, 00:00:01
begin = 1293840001000

# 31 Dec 2011, 23:59:59
end = 1373673599000

class MRSampleEntityRevisions(MRJob):

    INTERNAL_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(MRSampleEntityRevisions, self).configure_options()
        self.add_file_option('--seed-file',dest='seed_file',help='files of seed entities',action='append')

    # Read the seed file into one shared set
    def load_options(self,args):
        super(MRSampleEntityRevisions, self).load_options(args)
        # seedfile = self.options.seed_file
        # self.seeds = set()
        # with open(seedfile[0],'r') as f:
        #    for line in f:
        #        self.seeds.add(long(line.rstrip()))
        
    def mapper(self, pid, line):
        obj = json.loads(line)
        timestamp = long(obj['timestamp'])
        if timestamp >= begin and timestamp < end:
            pageid = long(obj['page_id'])
            # if pageid in self.seeds:
            yield (None,line)

    def reducer(self, pid, lines):
        for line in lines:
            yield (None,line)

if __name__ == '__main__':
    MRSampleEntityRevisions.run()
