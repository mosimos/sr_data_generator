q(Stt, null, Del) :- hasDelay(Stt, Del), not some_arr(Stt, Del).
some_arr(stt, Del) :- hasDelay(Stt, Del), hasArrived(Stt, Arr).
q(Stt, Arr, Del) :- hasDelay(Stt, Del), hasArrived(Stt, Arr).
#show q/3.
