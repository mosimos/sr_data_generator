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

f = open(sys.argv[1], 'r')

for line in f:
    time.sleep(0.003)
    m = re.match(r"stream_post\((\w+), (\w+), (.+)\).", line)
    if m:
        print(json.dumps(m.groups()))
    else:
        triple = line.rstrip()
        triple = triple.split(" ")
        if len(triple) == 3:
            print(json.dumps(triple))
        else:
            print('match error')



