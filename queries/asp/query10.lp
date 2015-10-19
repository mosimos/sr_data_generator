%REGISTER QUERY q10 AS
%PREFIX ns1: <http://kr.tuwien.ac.at/dhsr/>
%SELECT ?stt_id ?del
%FROM STREAM <http://kr.tuwien.ac.at/dhsr/stream> [RANGE 1s STEP 1s]
%WHERE {
%	?stt_id ns1:hasDelay ?del.
%	}
%ORDER BY ?del

%TODO: can this be done?

has_some_before(Stt,Del) :- hasDelay(Stt,Del), hasDelay(SttP,DelP), DelP < Del.

1 { q(Stt,Del,0); -q(Stt,Del,0) } 1 :- hasDelay(Stt,Del), not has_some_before(Stt,Del).

q(Stt2,Del2,NP) :- q(Stt,Del,N), next(Stt,Del,Stt2,Del2), NP=N+1.

before(Stt1,Del1,Stt2,Del2) :- hasDelay(Stt1,Del1), hasDelay(Stt2,Del2), Del1 < Del2.

beforeEq(Stt1,Del1,Stt2,Del2) :- hasDelay(Stt1,Del1), hasDelay(Stt2,Del2), Del1 <= Del2.

1 { next(Stt1,Del1,Stt2,Del2); -next(Stt1,Del1,Stt2,Del2)} 1 :- beforeEq(Stt1,Del1,Stt2,Del2).

-next(Stt1,Del1,Stt2,Del2) :- before(Stt1,Del1,Stt3,Del3), before(Stt3,Del3,Stt2,Del2).

:- q(Stt1,Del1,N), q(Stt2,Del2,N), not same(Stt1,Del1,Stt2,Del2).

same(Stt1,Del1,Stt2,Del2) :- hasDelay(Stt1,Del1), hasDelay(Stt2,Del2),
Stt1=Stt2, Del1=Del2.

#show q/3.

% hasDelay(s0,10). hasDelay(s1,10). hasDelay(s2,20).

% TODO
