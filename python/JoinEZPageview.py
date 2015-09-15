#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Join dictionary and the time series

import sys,codecs
from collections import defaultdict

def join_ts_dict(tsfile,dictfile,outfile):
    
    # Hash-based join
    t2id = defaultdict(int)
    with codecs.open(dictfile,'r','utf-8') as reader:
        for line in reader:
            try:
                i = line.find(' ')
                t2id[line[:i]] = int(line.rstrip()[i+1:])
            except:
                continue

    with codecs.open(tsfile,'r','utf-8') as reader:
        o = codecs.open(outfile,'w','utf-8')
        for line in reader:
            i = line.find(' ')
            ent = line[:i]
            if ent in t2id:
                o.write(str(t2id[ent]))
                o.write(line[i+1:])
        o.close()

if __name__ == "__main__":
    join_ts_dict(sys.argv[1],sys.argv[2],sys.argv[3])