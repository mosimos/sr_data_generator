%REGISTER QUERY q10 AS
%PREFIX ns1: <http://kr.tuwien.ac.at/dhsr/>
%SELECT ?stt_id ?del
%FROM STREAM <http://kr.tuwien.ac.at/dhsr/stream> [RANGE 1s STEP 1s]
%WHERE {
%	?stt_id ns1:hasDelay ?del.
%	}
%ORDER BY ?del

% determine list length
upper(X) :- X = #count { S,D : hasDelay(S, D) }.
slot(1..X) :- upper(X).

% guess a slot for each delay
q(S,D,N) ; -q(S,D,N) :- hasDelay(S,D), slot(N).

% use each slot
used_slot(N) :- q(_,_,N).
:- slot(N), not used_slot(N).

% use each used slot at most once
:- q(S1,D1,N), q(S2,D2,N), not same(S1,D1,S2,D2).

same(S,D,S,D) :- hasDelay(S,D).

% assign each delay to a slot
:- hasDelay(S,D), not assigned(S,D).
assigned(S,D) :- q(S,D,_).

% ensure ordering
:- q(S1,D1,N1), q(S2,D2,N2), N1<N2, D2>D1.

#show q/3.

%hasDelay(max,100). hasDelay(middleA,75). hasDelay(middleB,75). hasDelay(min,66).


