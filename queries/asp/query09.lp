q(Del) :- hasDelay(Stt, Del), not smaller_than_any(Stt, Del).
smaller_than_any(Stt1, Del1) :- hasDelay(Stt1, Del1), hasDelay(Stt2, Del2), Del1 < Del2.


%q(Del) :- #max { Del,Stt : hasDelay(Stt, Del) }.

#show q/1.


