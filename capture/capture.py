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

# TODO loop (and eliminate potential duplicates?)
# TODO limit


from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.plugins.sparql import prepareQuery
from google.transit import gtfs_realtime_pb2
import urllib
import time
import json
import argparse

parser = argparse.ArgumentParser(description='Capture GTFS-realtime stream to file.')
parser.add_argument('output_file', type=argparse.FileType('w'), help='output file')
parser.add_argument('-t', '--type', choices=['t', 'v', 'b'], help='capture (t)rip updates, (v)ehicles or (b)oth', default='b')
parser.add_argument('-ts', '--trip_update_stream', help='URL of the GTFS-realtime trip update stream', default='http://developer.trimet.org/ws/V1/TripUpdate/?appID=C06C7AC2D0839173A16C6BC28')
parser.add_argument('-vs', '--vehicle_stream', help='URL of the GTFS-realtime vehicle stream', default='http://developer.trimet.org/ws/gtfs/VehiclePositions/?appID=C06C7AC2D0839173A16C6BC28')
parser.add_argument('-n', '--namespace', default='http://kr.tuwien.ac.at/dhsr/')
parser.add_argument('-l', '--limit', type=int, default=-1, help='maximum number of triples to capture')
parser.add_argument('-p', '--plain', action='store_true', help='output triples without timestamp, seperated by spaces')

args = parser.parse_args()

ns = Namespace(args.namespace)

#g = Graph(store='Sleepycat')
#g.open('/home/mosi/rdflibstore')
g = Graph()
#g.parse('./data_portland.ttl', format='turtle')

print('graph parsed')

q = prepareQuery('SELECT ?stt_id '
        'WHERE { '
        '?trip_id ns1:hasStt ?stt_id. '
        '?stt_id ns1:isSeq ?seq_nr. '
        '}', initNs = {'ns1': ns})

feed = gtfs_realtime_pb2.FeedMessage()

if args.type == 'b' or args.type =='t':
    response = urllib.urlopen(args.trip_update_stream)
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

                    if (args.plain):
                        args.output_file.write(str(ns['stoptime/' + str(trip_id) + str(stop_sequence)]) + ' ns1:hasDelay ' + str(delay) + '\n')
                    else:
                        args.output_file.write(str(tstamp) + ' ' + json.dumps([ns['stoptime/' + str(trip_id) + str(stop_sequence)], 'ns1:hasDelay', delay]) + '\n')

                    #query for stoptime id
                    #res = g.query(q, initBindings={'trip_id': ns['trip/' + trip_id], 'seq_nr': Literal(stop_sequence)})

                    #if len(res) == 1:
                        #out.write(str(tstamp) + ' ' + json.dumps([res[0][0], 'ns1:hasDelay', delay]))
                        #print('triple written')
                    #else:
                        #print('error: unable to get stoptime_id for trip_id ' + str(trip_id) + ' seqnr ' + str(stop_sequence))


if args.type == 'b' or args.type =='v':
    response = urllib.urlopen(args.vehicle_stream)
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

                        if (args.plain):
                            args.output_file.write(str(ns['stoptime/' + str(trip_id) + str(stop_sequence)]) + ' ns1:hasArrived ' + str(tstamp) + '\n')
                        else:
                            args.output_file.write(str(tstamp) + ' ' + json.dumps([ns['stoptime/' + str(trip_id) + str(stop_sequence)], 'ns1:hasArrived', tstamp]) + '\n')

                        #query for stoptime id
                        #res = g.query(q, initBindings={'trip_id': ns['trip/' + trip_id], 'seq_nr': Literal(stop_sequence)})

                        #if len(res) == 1:
                            #out.write(str(tstamp) + ' ' + json.dumps([res[0][0], 'ns1:hasArrived', tstamp]))
                            #print('triple written')
                        #else:
                            #print('error: unable to get stoptime_id for trip_id ' + str(trip_id) + ' seqnr ' + str(stop_sequence))



