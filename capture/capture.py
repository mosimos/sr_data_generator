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

#from transitfeed import Loader, Schedule
#from rdflib import Graph, URIRef, Literal, BNode, Namespace
from google.transit import gtfs_realtime_pb2
import urllib
import time
import json

out = open('../capture.triple', 'w')

ns = Namespace('http://kr.tuwien.ac.at/dhsr/')

feed = gtfs_realtime_pb2.FeedMessage()
response = urllib.urlopen('http://developer.trimet.org/ws/V1/TripUpdate/?appID=C06C7AC2D0839173A16C6BC28')
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

                #TODO query for stoptime id
                out.write(str(tstamp) + ' ' + json.dumps([trip_id, stop_sequence, delay]))

