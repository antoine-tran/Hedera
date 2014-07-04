#!/usr/bin/env python
"""
Extract the mapping of wikipedia page id - title from the revision dumps
"""
from mrjob.job import MRJob
import re

import logging
logging.basicConfig(filename='boto.log',level=logging.DEBUG)


# set up classpath
import os

WORD_RE = re.compile(r"[\w']+")

import json

class MRTitle2Id(MRJob):

    def mapper(self, pid, line):
        obj = json.loads(line)
        pid = int(obj['page_id'])
        title = obj['page_title']
        # yield (pid, line)
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