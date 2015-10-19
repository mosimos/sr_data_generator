q(Stt, Del, Arr) :- hasDelay(Stt, Del), hasArrived(Stt, Arr).
some_arr(Stt, Del) :- hasDelay(Stt, Del), hasArrived(Stt, Arr).
q(Stt, Del, null) :- hasDelay(Stt, Del), not some_arr(Stt, Del).
#show q/3.


