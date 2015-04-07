#!/usr/bin/python

#   Copyright 2015 Andreas Mosburger
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

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



