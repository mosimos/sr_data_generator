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

import json
import time
import argparse

#streams triples from a file to a streaming engine
#expects timestamps at the beginning of every line

parser = argparse.ArgumentParser(description='Stream triples read from capture_file to stdout using the timing provided in the capture file')
parser.add_argument('capture_file', type=argparse.FileType('r'))

args = parser.parse_args()

starttime = float(args.capture_file.readline().split(' ')[0])
offset = time.time() - starttime

args.capture_file.seek(0)

for line in args.capture_file:
    l = line.split(' ', 1)
    time.sleep(max(0, (float(l[0]) + offset - time.time()) / 1000 ))
    triple = l[1].split(' ')
    if len(triple) == 3:
        print(json.dumps(triple))
    else:
        print('match error')



