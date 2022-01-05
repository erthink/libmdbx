# DarwinPthreadBarrier

A pthread_barrier_t implementation for Mac OS/X

There is no pthread_barrier_t in Mac OS/X pthreads. This project fixes
this omission by providing a simple-minded barrier implementation based
on a pair of pthread_mutex_t and pthread_cond_t.

