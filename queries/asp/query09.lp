%REGISTER QUERY q09 AS
%PREFIX ns1: <http://kr.tuwien.ac.at/dhsr/>
%SELECT (MAX (?del) AS ?M)
%FROM STREAM <http://kr.tuwien.ac.at/dhsr/stream> [RANGE 1s STEP 1s]
%WHERE {
%	?stt_id ns1:hasDelay ?del.
%	}

%TODO: is this right?

q(Del) :- hasDelay(Stt, Del), not smaller_than_any(Stt, Del).
smaller_than_any(Stt1, Del1) :- hasDelay(Stt1, Del1), hasDelay(Stt2, Del2), Del1 < Del2.

