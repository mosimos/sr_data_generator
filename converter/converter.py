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

#TODO argument handling
#TODO exception handling
#TODO performance?

from transitfeed import Loader, Schedule
from rdflib import Graph, URIRef, Literal, BNode, Namespace

sched = Loader('../gtfs_datasets/austin').Load()
g = Graph()
f = open('../data_austin.rdf', 'w')

ns = Namespace('http://kr.tuwien.ac.at/dhsr/')

for stop in sched.GetStopList():
    s = ns['stop/' + stop.stop_id]
    g.set((s, ns.hasName, Literal(stop.stop_name)))

for route in sched.GetRouteList():
    r = ns['route/' + route.route_id]
    g.set((r, ns.hasName, Literal(route.route_short_name)))
    g.set((r, ns.hasType, Literal(route.route_type)))

for trip in sched.GetTripList():
    t = ns['trip/' + trip.trip_id]
    g.set((t, ns.isonRoute, ns['route/' + trip.route_id]))
    g.set((t, ns.hasDirection, Literal(trip.direction_id)))
    g.set((t, ns.isAccessible, Literal(trip.wheelchair_accessible)))

    for stoptime in trip.GetStopTimes():
        st = BNode()
        g.add((t, ns.hasStt, st))
        g.set((st, ns.atStop, ns['stop/' + stoptime.stop_id]))
        #TODO change for arrival_secs and departure_secs ?
        g.set((st, ns.hasArrtime, Literal(stoptime.arrival_time)))
        g.set((st, ns.hasDeptime, Literal(stoptime.departure_time)))
        g.set((st, ns.isSeq, Literal(stoptime.stop_sequence)))

f.write(g.serialize(format = 'turtle'))

