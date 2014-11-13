#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Copyright (c) 2014 Anh Tuan Tran - ttran@L3S.de

Python scripts to extract various information from Wikipedia revision history. Project details: http://antoine-tran.github.io/hedera

=================================================
THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.


Experimental for WWW2015: Sample revisions within the year 2011
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
# 2011-01-01
begin = 1293840001000

# 2013-07-12
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
    MRSampleRevisions.run()
