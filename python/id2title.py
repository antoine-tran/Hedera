#!/usr/bin/env python
"""
Copyright (c) 2014 Anh Tuan Tran - ttran@L3S.de

Python scripts to extract various information from Wikipedia revision history. Project details: http://antoine-tran.github.io/hedera

=================================================
THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

Extract the mapping of wikipedia page id - title from the revision dumps
"""
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

import re

import logging
logging.basicConfig(filename='boto.log',level=logging.DEBUG)


# set up classpath
import os
os.environ['HADOOP_HOME']='/opt/cloudera/parcels/CDH'
os.environ['HADOOP_MAPRED_HOME']='/opt/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce'

import json

class MRTitle2Id(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    JOBCONF = {
        'mapreduce.job.name':'Build id-title mapping',
        'mapred.child.java.opts':'-Xmx5100m',
        'mapreduce.map.memory.mb':'5100',
        'mapreduce.reduce.memory.mb':'5100',
        'mapreduce.map.java.opts':'-Xmx5100m',
        'mapreduce.reduce.java.opts':'-Xmx5120m'
    }
    
    def mapper(self, pid, line):
        obj = json.loads(line)
        pid = int(obj['page_id'])
        title = obj['page_title']
        yield (pid,title)

    def combiner(self, pid, title):
        d = set()
        for t in title:
            if not t in d:
                d.add(t)
                yield (pid,t)

    def reducer(self, pid, title):
        d = set()
        for t in title:
            if not t in d:
                d.add(t)
                yield (pid,t)


if __name__ == '__main__':
     MRTitle2Id.run()
