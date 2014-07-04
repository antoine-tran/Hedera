#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Extract temporal anchor text from
JSON dumps of Wikipedia revisions

Output anchor in format (separated by TAB)
[timestamp] [source ID] [revision ID] [previous revision ID] [source title] [anchor text] [target title]
"""

from mrjob.job import MRJob

# Enable boto logging for EMR debug
import logging
logging.basicConfig(filename='boto.log',level=logging.INFO)

# Settings for running at the cluster
import os
os.environ['HADOOP_HOME']='/opt/cloudera/parcels/CDH'
os.environ['HADOOP_MAPRED_HOME']='/opt/cloudera/parcels/CDH-4.6.0-1.cdh4.6.0.p0.26/lib/hadoop-0.20-mapreduce'

import json

class MRAnchorText(MRJob):

    def get_links(text):
        start = 0
        res = list()
        while True:
            start = text.find('[[',start)
            if start < 0:
                break
            end = text.find(']]', start+2)
            if end < 0:
                break
            content = text[start+2:end]
            anchor = None
            
            if not content or len(content) == 0:
                start = end + 1
                continue
            if content.find(':') >= 0:
                start = end + 1
                continue
            i = content.find('|')
            if i >= 0:
                anchor = text[i+1:]
                content = content[:i]

            i = content.find('#')
            if i >= 0:
                content = content[:i]

            if len(content) == 0:
                start = end + 1
                continue

            if not anchor:
                anchor = content

            res.add((anchor,content))

            start = end + 1

        return res
        
    def mapper(self, pid, line):
        obj = json.loads(line)
        pid = long(obj['page_id'])
        title = obj['page_title']
        timestamp = long(obj['timestamp'])
        revid = long(obj['rev_id'])
        if 'parent_id' in obj:
            parid = long(obj['parent_id'])
        else:
            parid = 0
        text = obj['text']
        anchors = self.get_links(text)
        for (a,t) in anchors:
            str = '%d\t%d\t%d\t%d\t%s\t%s\t%s'
            yield (timestamp,str)

    def reducer(self, pid, lines):
        for line in lines:
            yield line

if __name__ == '__main__':
    MRAnchorText.run()