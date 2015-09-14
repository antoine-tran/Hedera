#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Python version of the Java class AggregateEZPageview
Sep 14, 2015
"""

from collections import defaultdict
import sys,os,codecs

# m is the month size
def aggr(infile,outfile,m):

    # We load everything into the memory
    ts = defaultdict(list)
    print infile
    with codecs.open(infile,'r','utf-8') as reader:
        for line in reader:
            i = line.find(' ')
            ent = line[:i]
            print ent
            if not ent in ts:
                ts[ent] = [0]*(m+1)
            cnt = 0

            # skip month
            i = line.find(' ',i+1)

            while cnt < m+1:
                j = line.find(' ',i+1)
                ts[ent][cnt] += int(line[i+1:j])
                cnt += 1
                i = j

    with codecs.open(outfile,'w','utf-8') as o:
        for k,v in ts.iteritems():
            o.write(k)
            [o.write(' d' % i) for i in v]
            o.write('\n')

if __name__ == "__main__":
    aggr(sys.argv[1],sys.argv[2],int(sys.argv[3]))