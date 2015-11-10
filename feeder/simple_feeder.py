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
import json
import time
import argparse

#streams triples from a file to a streaming engine

parser = argparse.ArgumentParser(description='Stream triples read from capture_file to stdout')
parser.add_argument('capture_file', type=argparse.FileType('r'))
parser.add_argument('-d', '--delay', type=float, default=0)

args = parser.parse_args()

for line in args.capture_file:
    time.sleep(args.delay)
    triple = line.rstrip()
    triple = triple.split(" ")
    if len(triple) == 3:
        #simple triple, separated by blanks
        print(json.dumps(triple))
        sys.stdout.flush()
    else:
        if len(triple) == 4:
            #simple triple, separated by blanks, timestamp in the front
            print(json.dumps(triple[1:4]))
            sys.stdout.flush()
        else:
            print('match error')



