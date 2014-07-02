#!/usr/bin/env python

def fistn(n):
    num = 0
    a = set()
    while num < n:
        if not num in a:
            yield num
            a.add(num)
            num -= 1
            a.add(num)
        else:
            num += 2


iter = fistn(10)
for i in iter:
    print i