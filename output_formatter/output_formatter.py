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
import os
import re

parser = argparse.ArgumentParser(description='Converts results to a single comparable format.')
parser.add_argument('path', help='path to the results')

args = parser.parse_args()

if not os.access(args.path, os.W_OK):
    print('error: can\'t access ' + args.path)
    exit(1)

#matches a fact of arity 1 or more
asp_match = re.compile(r'\w+\(([\w\,]+)\)\.')
semweb_schema_match = re.compile(r'\"(\w+)\"\^\^http.*XMLSchema.*')
spark_match = re.compile(r'List\((.+)\)')

spark_queryset = set()

try:
    os.mkdir(args.path + '/formatted')
except FileExistsError:
    pass


for subdir in os.listdir(args.path):
    if not subdir == 'formatted':
        try:
            os.mkdir(args.path + '/formatted/' + subdir)
        except FileExistsError:
            pass

        for f in os.listdir(args.path + '/' + subdir):
            if os.path.isfile(args.path + '/' + subdir + '/' + f):
                #handle asp and semweb
                asp = False
                inp = open(args.path + '/' + subdir + '/' + f, 'r')
                out = open(args.path + '/formatted/' + subdir + '/' + f, 'w')
                for line in inp:
                    if not line.startswith('%'):
                        if line.startswith('ANSWER'):
                            #we got an ASP file
                            asp = True
                            continue
                        if asp:
                            for fact in line.split(' '):
                                res = asp_match.match(fact)
                                if res:
                                    out.write(res.group(1).replace(',', ' ') + '\n')
                            asp = False
                        elif line.lstrip().startswith('http'):
                            #we got a semweb file
                            outstr = ''
                            splchar = ' '

                            #c-sparql prints with tabs
                            if '\t' in line:
                                splchar = '\t'

                            for dat in line.split(splchar):
                                if dat != '':
                                    res = semweb_schema_match.match(dat)
                                    if res:
                                        outstr = outstr + res.group(1) + ' '
                                    else:
                                        #'http://xxx/xxx/stoptime/597983686' => 'stoptime597983686 '
                                        outstr = outstr + (''.join(dat.split('/')[-2:]) + ' ')
                            out.write(outstr.rstrip() + '\n')
                inp.close()
                out.close()

            elif os.path.isdir(args.path + '/' + subdir + '/' + f):
                #handle spark
                result_files = os.listdir(args.path + '/' + subdir + '/' + f)

                if '_SUCCESS' in result_files:
                    #find out if we have to create a new file or append
                    query = f.split('-')[0]
                    if not query in spark_queryset:
                        spark_queryset.add(query)
                        out = open(args.path + '/formatted/' + subdir + '/' + query + '.txt', 'w')
                    else:
                        out = open(args.path + '/formatted/' + subdir + '/' + query + '.txt', 'a')

                    #go through all the part files
                    for part in [p for p in result_files if p.startswith('part')]:
                        inp = open(args.path + '/' + subdir + '/' + f + '/' + part, 'r')
                        for line in inp:
                            #process line
                            res = spark_match.match(line)
                            if res:
                                outstr = ''
                                for dat in res.group(1).split(', '):
                                    #'http://xxx/xxx/stoptime/597983686' => 'stoptime597983686 '
                                    outstr = outstr + (''.join(dat.split('/')[-2:]) + ' ')
                                out.write(outstr.rstrip() + '\n')
                        inp.close()
                    out.close()




