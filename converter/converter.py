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

#TODO exception handling
#TODO performance?

from transitfeed import Loader, Schedule
from rdflib import Graph, URIRef, Literal, BNode, Namespace
import argparse

parser = argparse.ArgumentParser(description='Convert GTFS dataset to RDF.')
parser.add_argument('gtfs_path', help='path to the GTFS dataset')
parser.add_argument('output_file', help='output file')
parser.add_argument('-f', '--format', choices=['xml', 'n3', 'turtle', 'nt', 'pretty-xml', 'trix'], default='turtle')
parser.add_argument('-l', '--limit', type=int, default=-1, help='maximum number of trips to convert')

args = parser.parse_args()

sched = Loader(args.gtfs_path).Load()
g = Graph()

f = open(args.output_file, 'w')

ns = Namespace('http://kr.tuwien.ac.at/dhsr/')

for stop in sched.GetStopList():
    s = ns['stop/' + stop.stop_id]
    g.set((s, ns.hasName, Literal(stop.stop_name)))

for route in sched.GetRouteList():
    r = ns['route/' + route.route_id]
    g.set((r, ns.hasName, Literal(route.route_short_name)))
    g.set((r, ns.hasType, Literal(route.route_type)))

i = 0

for trip in sched.GetTripList():
    if i == args.limit:
        break
    i += 1

    t = ns['trip/' + trip.trip_id]
    g.set((t, ns.isonRoute, ns['route/' + trip.route_id]))
    g.set((t, ns.hasDirection, Literal(trip.direction_id)))
    g.set((t, ns.isAccessible, Literal(trip.wheelchair_accessible)))

    for stoptime in trip.GetStopTimes():
        st = ns['stoptime/' + str(trip.trip_id) + str(stoptime.stop_sequence)]
        g.add((t, ns.hasStt, st))
        g.set((st, ns.atStop, ns['stop/' + stoptime.stop_id]))
        #TODO change for arrival_secs and departure_secs ?
        g.set((st, ns.hasArrtime, Literal(stoptime.arrival_time)))
        g.set((st, ns.hasDeptime, Literal(stoptime.departure_time)))
        g.set((st, ns.isSeq, Literal(stoptime.stop_sequence)))

f.write(g.serialize(format = args.format))
g.close()

