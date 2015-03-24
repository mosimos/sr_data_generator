#!/usr/bin/python

import sys
import re
import json
import time

#streams triples from a file to a streaming engine
#expects a timestamp in the first line as starttime
#and timestamps at the beginning of every consecutive line

f = open(sys.argv[1], 'r')

starttime = float(f.readline()[:-1])
offset = time.time() - starttime

for line in f:
    l = line.split(' ', 1)
    m = re.match(r"stream_post\((\w+), (\w+), (.+)\).", l[1])
    if m:
        time.sleep(max(0, float(l[0]) + offset - time.time()))
        print(json.dumps(m.groups()))
    else:
        triple = l[1].split(" ")
        if len(triple) == 3:
            print(json.dumps(triple))
        else:
            print('match error')



