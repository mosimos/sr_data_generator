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

import argparse

parser = argparse.ArgumentParser(description='Convert captured triples to ASP facts, split into multiple files each coresponding to a tumbling window.')
parser.add_argument('input_file', type=argparse.FileType('r'), help='input file containing captured triples')
parser.add_argument('size', type=int, help='number of triples/facts in window')
parser.add_argument('output_directory', help='directory where the output files containing facts will be created')

args = parser.parse_args()

filecount = 0
triplecount = 0

outputfile_base = (args.input_file.name.split('/')[-1]).split('.')[0]

out = open(args.output_directory + '/' + outputfile_base + '_' + str(filecount).zfill(3) + '.lp', 'w')


for line in args.input_file:
    triple = line.rstrip()
    triple = triple.split(" ")

    if len(triple) == 4:
        s = triple[1]
        p = triple[2]
        o = triple[3]
        s = s.split('/')[-2] + s.split('/')[-1]
        p = (p.split('/')[-1]).split(':')[-1]

        if triplecount == args.size:
            filecount += 1
            outputfile_base = (args.input_file.name.split('/')[-1]).split('.')[0]
            out.close()
            out = open(args.output_directory + '/' + outputfile_base + '_' + str(filecount).zfill(3) + '.lp', 'w')
            triplecount = 0

        out.write(p + '(' + s + ', ' + o +').\n')
        triplecount += 1

    else:
        print('match error')
        print(triple)

