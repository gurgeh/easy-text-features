#!/usr/bin/env python2.7
import sys
from ngram import *

INNAME = 'bistudien.csv'

if __name__ == '__main__':
    if len(sys.argv) > 1:
        INNAME = sys.argv[1]
    base, ext = INNAME.rsplit('.', 1)
    transform(wordgram, INNAME, base + '-wordgram.' + ext)
