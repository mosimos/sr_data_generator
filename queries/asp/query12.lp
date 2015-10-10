q(Stt, null, Del) :- hasDelay(Stt, Del), not some_atStop(Stt, Del).
some_atStop(Stt, Del) :- hasDelay(Stt, Del), atStop(Stt, St).
q(Stt, St, Del) :- hasDelay(Stt, Del), atStop(Stt, St).
#show q/3.
