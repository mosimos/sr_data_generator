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

#TODO limit is pretty wonky because of duplicate elimination - remove it?


from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.plugins.sparql import prepareQuery
from google.transit import gtfs_realtime_pb2
import urllib
import time
import argparse
import ConfigParser
from subprocess import call
import sys, os

parser = argparse.ArgumentParser(description='Capture GTFS-realtime stream to file.')
parser.add_argument('output_file', type=argparse.FileType('w'), help='output file')
parser.add_argument('-t', '--type', choices=['t', 'v', 'b'], help='capture (t)rip updates, (v)ehicles or (b)oth', default='b')
parser.add_argument('-l', '--limit', type=int, default=-1, help='maximum number of triples to capture')

args = parser.parse_args()

trip_update_stream = ''
vehicle_stream = ''
namespace = ''

try:
    conf = ConfigParser.ConfigParser()
    conf.read(os.path.dirname(sys.argv[0]) + '/streams.ini')
    if args.type == 'b' or args.type == 't':
        trip_update_stream = conf.get('streams', 'trip_update')
    if args.type == 'b' or args.type == 'v':
        vehicle_stream = conf.get('streams', 'vehicle')
    namespace = conf.get('streams', 'namespace')
except:
    print 'error when parsing streams.ini file'

ns = Namespace(namespace)


#g = Graph(store='Sleepycat')
#g.open('/home/mosi/rdflibstore')
g = Graph()
#g.parse('./data_portland.ttl', format='turtle')

#print('graph parsed')

#q = prepareQuery('SELECT ?stt_id '
        #'WHERE { '
        #'?trip_id ns1:hasStt ?stt_id. '
        #'?stt_id ns1:isSeq ?seq_nr. '
        #'}', initNs = {'ns1': ns})

feed = gtfs_realtime_pb2.FeedMessage()

count = 0
request_tstamp = 0

print "Starting capture, press Ctrl-C to stop."

while True:
    try:
        if args.type == 'b' or args.type == 't':
            response = urllib.urlopen(trip_update_stream)
            feed.ParseFromString(response.read())

            if feed.header.HasField('timestamp'):
                tstamp = feed.header.timestamp
            else:
                tstamp = time.time()

            for entity in feed.entity:
                if (entity.HasField('trip_update')
                        and entity.trip_update.trip.HasField('trip_id')):
                    trip_id = entity.trip_update.trip.trip_id
                    for stt_update in entity.trip_update.stop_time_update:
                        if (stt_update.HasField('stop_sequence')
                                and stt_update.HasField('arrival')
                                and stt_update.arrival.HasField('delay')):
                            stop_sequence = stt_update.stop_sequence
                            delay = stt_update.arrival.delay

                            args.output_file.write(str(tstamp) + ' ' + str(ns['stoptime/' + str(trip_id) + str(stop_sequence)]) + ' ' + ns['hasDelay'] + ' ' + str(delay) + '\n')

                            #duplicate elimination is tricky here, so we don't count these triples
                            #count += 1

                            #if count == args.limit:
                                #raise KeyboardInterrupt


        if args.type == 'b' or args.type == 'v':
            if request_tstamp == 0:
                response = urllib.urlopen(vehicle_stream)
            else:
                response = urllib.urlopen(vehicle_stream + "&since=" + str(int(request_tstamp)))

            request_tstamp = time.time()
            feed.ParseFromString(response.read())

            for entity in feed.entity:
                if entity.HasField('vehicle'):
                    if entity.vehicle.current_status == 1:
                        if entity.vehicle.trip.HasField('trip_id'):
                            trip_id = entity.vehicle.trip.trip_id
                            if (entity.vehicle.HasField('current_stop_sequence')
                                    and entity.vehicle.HasField('timestamp')):
                                stop_sequence = entity.vehicle.current_stop_sequence
                                tstamp = entity.vehicle.timestamp

                                args.output_file.write(str(tstamp) + ' ' + str(ns['stoptime/' + str(trip_id) + str(stop_sequence)]) + ' ' + ns['hasArrived'] + ' ' + str(tstamp) + '\n')
                                count += 1

                                if count == args.limit:
                                    raise KeyboardInterrupt

        time.sleep(1)


    except KeyboardInterrupt:
        break


#first, sort ignoring timestamps to eliminate duplicate triples
call(["sort", "-k2", "-u", "-o", args.output_file.name + "_s", args.output_file.name])

#sort by timestamp
call(["sort", "-u", "-o", args.output_file.name + "_s2", args.output_file.name + "_s"])

call(["mv", args.output_file.name + "_s2", args.output_file.name])
call(["rm", args.output_file.name + "_s"])


