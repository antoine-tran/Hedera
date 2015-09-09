#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Quick resolving redirects
"""

import sys,codecs
from collections import defaultdict

# Input 1: Dictionary, input 2: redirect
def redirect_chain(simpleredirectfile,dictfile,outfile):
    t2id = defaultdict(int)
    with codecs.open(dictfile,'r','utf-8') as reader:
        for line in reader:
            i = line.rfind('\t')
            t2id[line[:i]] = int(line.rstrip()[i+1:])
    redir = defaultdict(int)
    with codecs.open(simpleredirectfile,'r','utf-8') as reader:
        for line in reader:
            i = line.rfind('\t')
            cs = int(line.rstrip()[i+1:])
            if cs == 1:
                j = line.find('\t')
                if line[j+1:i] in t2id:
                    redir[int(line[:j])] = t2id[line[j+1:i]]

    
    with codecs.open(outfile,'w','utf-8') as o:
        for t in t2id:
            pid = t2id[t]
            if not pid in redir:
                o.write('%s %d\n' % (t.replace(' ','_'),pid))
            else:
                o.write('%s\t%d\n' % (t.replace(' ','_'),redir[pid]))

        
if __name__ == '__main__':
    redirect_chain(sys.argv[1],sys.argv[2],sys.argv[3])