%REGISTER QUERY q10 AS
%PREFIX ns1: <http://kr.tuwien.ac.at/dhsr/>
%SELECT ?stt_id ?del
%FROM STREAM <http://kr.tuwien.ac.at/dhsr/stream> [RANGE 1s STEP 1s]
%WHERE {
%	?stt_id ns1:hasDelay ?del.
%	}
%ORDER BY ?del

%TODO: can this be done?
