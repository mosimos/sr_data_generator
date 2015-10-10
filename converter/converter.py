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

parser = argparse.ArgumentParser(description='Convert GTFS dataset to RDF or ASP.')
parser.add_argument('gtfs_path', help='path to the GTFS dataset')
parser.add_argument('output_file', type=argparse.FileType('w'), help='output file')
parser.add_argument('-f', '--format', choices=['asp', 'xml', 'turtle', 'nt'], default='turtle')
parser.add_argument('-l', '--limit', type=int, default=-1, help='maximum number of trips to convert')

args = parser.parse_args()

sched = Loader(args.gtfs_path).Load()
asp = False

if args.format == 'asp':
    asp = True

g = Graph()

ns = Namespace('http://kr.tuwien.ac.at/dhsr/')

for stop in sched.GetStopList():
    if asp:
        args.output_file.write('hasName(stop' + stop.stop_id.replace('-', '') + ', "' + stop.stop_name + '").\n')
    else:
        s = ns['stop/' + stop.stop_id]
        g.set((s, ns.hasName, Literal(stop.stop_name)))


for route in sched.GetRouteList():
    if asp:
        args.output_file.write('hasName(route' + route.route_id + ', "' + route.route_short_name + '").\n')
        args.output_file.write('hasType(route' + route.route_id + ', ' + str(route.route_type) + ').\n')
    else:
        r = ns['route/' + route.route_id]
        g.set((r, ns.hasName, Literal(route.route_short_name)))
        g.set((r, ns.hasType, Literal(route.route_type)))

i = 0

for trip in sched.GetTripList():
    if i == args.limit:
        break
    i += 1

    if asp:
        args.output_file.write('isonRoute(trip' + trip.trip_id + ', route' + trip.route_id + ').\n')
        args.output_file.write('hasDirection(trip' + trip.trip_id + ', ' + trip.direction_id + ').\n')
        if trip.wheelchair_accessible != None:
            args.output_file.write('isAccessible(trip' + trip.trip_id + ', ' + str(trip.wheelchair_accessible) + ').\n')
    else:
        t = ns['trip/' + trip.trip_id]
        g.set((t, ns.isonRoute, ns['route/' + trip.route_id]))
        g.set((t, ns.hasDirection, Literal(trip.direction_id)))
        g.set((t, ns.isAccessible, Literal(trip.wheelchair_accessible)))

    for stoptime in trip.GetStopTimes():
        if asp:
            st = str(trip.trip_id) + str(stoptime.stop_sequence)
            args.output_file.write('hasStt(trip' + trip.trip_id + ', stoptime' + st + ').\n')
            args.output_file.write('atStop(stoptime' + st + ', stop' + stoptime.stop_id + ').\n')
            args.output_file.write('hasArrtime(stoptime' + st + ', ' + str(stoptime.arrival_secs) + ').\n')
            args.output_file.write('hasDeptime(stoptime' + st + ', ' + str(stoptime.departure_secs) + ').\n')
            args.output_file.write('isSeq(stoptime' + st + ', ' + str(stoptime.stop_sequence) + ').\n')
        else:
            st = ns['stoptime/' + str(trip.trip_id) + str(stoptime.stop_sequence)]
            g.add((t, ns.hasStt, st))
            g.set((st, ns.atStop, ns['stop/' + stoptime.stop_id]))
            g.set((st, ns.hasArrtime, Literal(stoptime.arrival_secs)))
            g.set((st, ns.hasDeptime, Literal(stoptime.departure_secs)))
            g.set((st, ns.isSeq, Literal(stoptime.stop_sequence)))

if not asp:
    args.output_file.write(g.serialize(format = args.format))
    g.close()

