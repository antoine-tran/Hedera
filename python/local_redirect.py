#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Quick resolving redirects
"""

import sys,codecs
from collections import defaultdict

# Input 1: Dictionary, input 2: redirect

# Step 1: Build the chain of redirects
def redirect_chain(simpleredirectfile,outfile):
    rleft = 0
    redirbysrc = defaultdict(str)
    redirbydest = defaultdict(int)
    with codecs.open()

with codecs.open(sys.argv[1],'r','utf-8') as reader:
    for line in reader:
        i = line.find('\t')
        