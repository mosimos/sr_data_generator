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
import time

parser = argparse.ArgumentParser(description='Convert captured triples to ASP facts, split into multiple files, each corresponding to a tumbling window.')
subparsers = parser.add_subparsers(help='operation mode', dest='mode')

parser_rep = subparsers.add_parser('replay', help='mimic the replay_feeder script')
parser_rep.add_argument('window', type=int, help='window size in seconds')
parser_rep.add_argument('input_file', type=argparse.FileType('r'), help='input file containing captured triples')
parser_rep.add_argument('output_directory', help='directory where the output files containing facts will be created')

parser_simp = subparsers.add_parser('simple', help='mimic the simple_feeder script')
parser_simp.add_argument('window', type=int, help='window size in seconds')
parser_simp.add_argument('delay', type=float, help='delay between triples')
parser_simp.add_argument('input_file', type=argparse.FileType('r'), help='input file containing captured triples')
parser_simp.add_argument('output_directory', help='directory where the output files containing facts will be created')

parser_trip = subparsers.add_parser('triples', help='fixed number of triples per window')
parser_trip.add_argument('size', type=float, help='number of triples per window')
parser_trip.add_argument('input_file', type=argparse.FileType('r'), help='input file containing captured triples')
parser_trip.add_argument('output_directory', help='directory where the output files containing facts will be created')


args = parser.parse_args()

filecount = 0
triplecount = 0

outputfile_base = (args.input_file.name.split('/')[-1]).split('.')[0]

out = open(args.output_directory + '/' + outputfile_base + '_' + str(filecount).zfill(3) + '.lp', 'w')

if args.mode == 'replay':
    starttime = float(args.input_file.readline().split(' ')[0])
    offset = time.time() - starttime

    args.input_file.seek(0)

win_start = time.time()


for line in args.input_file:
    triple = line.rstrip()
    triple = triple.split(" ")

    if len(triple) == 4:
        s = triple[1]
        p = triple[2]
        o = triple[3]
        s = s.split('/')[-2] + s.split('/')[-1]
        p = (p.split('/')[-1]).split(':')[-1]

        if args.mode == 'replay':
            time.sleep(max(0, (float(triple[0]) + offset - time.time()) / 1000 ))
            elapsed = time.time() - win_start
            if elapsed >= args.window:
                win_start = time.time()
                filecount += 1
                outputfile_base = (args.input_file.name.split('/')[-1]).split('.')[0]
                out.close()
                out = open(args.output_directory + '/' + outputfile_base + '_' + str(filecount).zfill(3) + '.lp', 'w')

        if args.mode == 'simple':
            time.sleep(args.delay)
            elapsed = time.time() - win_start
            if elapsed >= args.window:
                win_start = time.time()
                filecount += 1
                outputfile_base = (args.input_file.name.split('/')[-1]).split('.')[0]
                out.close()
                out = open(args.output_directory + '/' + outputfile_base + '_' + str(filecount).zfill(3) + '.lp', 'w')

        if args.mode == 'triples' and triplecount == args.size:
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

