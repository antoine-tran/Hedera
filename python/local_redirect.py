#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Quick resolving redirects
"""

import sys,codecs
from collections import defaultdict

# Input 1: Dictionary, input 2: redirect
def redirect_chain(simpleredirectfile,outfile):
    t2id = defaultdict(int)
    with codecs.open(simpleredirectfile,'r','utf-8') as reader:
        for line in reader:
            i = line.rfind('\t')
            cs = int(line.rstrip()[i+1:])
            if cs == -1:
                j = line.find('\t')
                t2id[line[j+1:i]] = int(line[:j])
    with codecs.open(simpleredirectfile,'r','utf-8') as reader:
        o = codecs.open(outfile,'w','utf-8')
        for line in reader:
            i = line.rfind('\t')
            cs = int(line.rstrip()[i+1:])
            if cs == 1:
                j = line.find('\t')
                if line[j+1:i] in t2id:
                    o.write('%d\t%d\n' % (int(line[:j]),t2id[line[j+1:i]]))
        o.close()

# Change dictionary using the redirect mapping
def resolve_redirect(dictfile,redirfile,outfile):
    redir = defaultdict(int)
    with codecs.open(redirfile,'r','utf-8') as reader:
        for line in reader:
            i = line.rfind('\t')
            redir[int(line[:i])] = int(line.rstrip()[i+1:])
    with codecs.open(dictfile,'r','utf-8') as reader:
        for line in reader:
            
            
        
if __name__ == '__main__':
    redirect_chain(sys.argv[1],sys.argv[2])