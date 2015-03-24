#!/usr/bin/python

import sys
import re
import json
import time

#streams triples from a file to a streaming engine

f = open(sys.argv[1], 'r')

for line in f:
    time.sleep(0.003)
    m = re.match(r"stream_post\((\w+), (\w+), (.+)\).", line)
    if m:
        print(json.dumps(m.groups()))
    else:
        triple = line.split(" ")
        if len(triple) == 3:
            print(json.dumps(triple))
        else:
            print('match error')



