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
#TODO proper URIs
#TODO performance?

from transitfeed import Loader, Schedule
from rdflib import Graph, URIRef, Literal, BNode

sched = Loader('../gtfs_datasets/austin').Load()
g = Graph()
f = open('../data_austin.rdf', 'w')

hassID = URIRef('http://example.com/hasstopid')
hassname = URIRef('http://example.com/hasstopname')
hasrID = URIRef('http://example.com/hasrouteid')
hasrname = URIRef('http://example.com/hasroutename')
hasrtype = URIRef('http://example.com/hasroutetype')
tripuriprefix = 'http://example.com/trip'
hastID = URIRef('http://example.com/hastripid')
hasdir = URIRef('http://example.com/hasdir')
hasst = URIRef('http://example.com/hasstoptime')
hasarrtime = URIRef('http://example.com/hasarrtime')
hasdeptime = URIRef('http://example.com/hasdeptime')
hasseq = URIRef('http://example.com/hasseq')

for stop in sched.GetStopList():
    s = URIRef(stop.stop_url)
    g.set((s, hassID, Literal(stop.stop_id)))
    g.set((s, hassname, Literal(stop.stop_name)))

for route in sched.GetRouteList():
    r = URIRef(route.route_url)
    g.set((r, hasrID, Literal(route.route_id)))
    g.set((r, hasrname, Literal(route.route_short_name)))
    g.set((r, hasrtype, Literal(route.route_type)))

for trip in sched.GetTripList():
    #t = URIRef(tripuriprefix + trip.trip_id)
    t = BNode()
    g.set((t, hastID, Literal(trip.trip_id)))
    g.set((t, hasrID, list(g[ : hasrID : Literal(trip.route_id)])[0]))  #TODO check if list() is dumb
    g.set((t, hasdir, Literal(trip.direction_id)))

    for stoptime in trip.GetStopTimes():
        st = BNode()
        g.add((t, hasst, st))
        g.set((st, hassID, list(g[ : hassID : Literal(stoptime.stop_id)])[0]))
        #TODO change for arrival_secs and departure_secs ?
        g.set((st, hasarrtime, Literal(stoptime.arrival_time)))
        g.set((st, hasdeptime, Literal(stoptime.departure_time)))
        g.set((st, hasseq, Literal(stoptime.stop_sequence)))

f.write(g.serialize())

