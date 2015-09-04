# Makefile for liblmdb (Lightning memory-mapped database library).

########################################################################
# Configuration. The compiler options must enable threaded compilation.
#
# Preprocessor macros (for CPPFLAGS) of interest...
# Note that the defaults should already be correct for most
# platforms; you should not need to change any of these.
# Read their descriptions in mdb.c if you do:
#
# - MDB_USE_POSIX_SEM
# - MDB_DSYNC
# - MDB_FDATASYNC
# - MDB_FDATASYNC_WORKS
# - MDB_USE_PWRITEV
#
# There may be other macros in mdb.c of interest. You should
# read mdb.c before changing any of them.
#
CC	?= gcc
CFLAGS	?= -O2 -g -Wall -Werror -Wno-unused-parameter
CFLAGS	+= -pthread
LDLIBS	= -lrt
prefix	?= /usr/local

########################################################################

IHDRS	= lmdb.h
ILIBS	= liblmdb.a liblmdb.so
IPROGS	= mdb_stat mdb_copy mdb_dump mdb_load mdb_chk
IDOCS	= mdb_stat.1 mdb_copy.1 mdb_dump.1 mdb_load.1
PROGS	= $(IPROGS) mtest0 mtest1 mtest2 mtest3 mtest4 mtest5 mtest6 wbench
all:	$(ILIBS) $(PROGS)

install: $(ILIBS) $(IPROGS) $(IHDRS)
	for f in $(IPROGS); do cp $$f $(DESTDIR)$(prefix)/bin; done
	for f in $(ILIBS); do cp $$f $(DESTDIR)$(prefix)/lib; done
	for f in $(IHDRS); do cp $$f $(DESTDIR)$(prefix)/include; done
	for f in $(IDOCS); do cp $$f $(DESTDIR)$(prefix)/man/man1; done

clean:
	rm -rf $(PROGS) *.[ao] *.[ls]o *~ testdb/*

test:	all
	[ -d testdb ] || mkdir testdb && rm -f testdb/* \
		&& echo "*** LMDB-TEST-0" && ./mtest0 && ./mdb_chk -v testdb \
		&& echo "*** LMDB-TEST-1" && ./mtest1 && ./mdb_chk -v testdb \
		&& echo "*** LMDB-TEST-2" && ./mtest2 && ./mdb_chk -v testdb \
		&& echo "*** LMDB-TEST-3" && ./mtest3 && ./mdb_chk -v testdb \
		&& echo "*** LMDB-TEST-4" && ./mtest4 && ./mdb_chk -v testdb \
		&& echo "*** LMDB-TEST-5" && ./mtest5 && ./mdb_chk -v testdb \
		&& echo "*** LMDB-TEST-6" && ./mtest6 && ./mdb_chk -v testdb \
		&& echo "*** LMDB-TESTs - all done"

liblmdb.a:	mdb.o midl.o
	$(AR) rs $@ mdb.o midl.o

liblmdb.so:	mdb.lo midl.lo
#	$(CC) $(LDFLAGS) -pthread -shared -Wl,-Bsymbolic -o $@ mdb.o midl.o $(SOLIBS)
	$(CC) $(LDFLAGS) -pthread -shared -o $@ mdb.lo midl.lo $(SOLIBS)

mdb_stat: mdb_stat.o liblmdb.a
mdb_copy: mdb_copy.o liblmdb.a
mdb_dump: mdb_dump.o liblmdb.a
mdb_load: mdb_load.o liblmdb.a
mdb_chk: mdb_chk.o liblmdb.a
mtest0: mtest0.o liblmdb.a
mtest1: mtest1.o liblmdb.a
mtest2:	mtest2.o liblmdb.a
mtest3:	mtest3.o liblmdb.a
mtest4:	mtest4.o liblmdb.a
mtest5:	mtest5.o liblmdb.a
mtest6:	mtest6.o liblmdb.a
wbench:	wbench.o liblmdb.a

mdb.o: mdb.c lmdb.h midl.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c mdb.c

midl.o: midl.c midl.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c midl.c

mdb.lo: mdb.c lmdb.h midl.h
	$(CC) $(CFLAGS) -fPIC $(CPPFLAGS) -c mdb.c -o $@

midl.lo: midl.c midl.h
	$(CC) $(CFLAGS) -fPIC $(CPPFLAGS) -c midl.c -o $@

%:	%.o
	$(CC) $(CFLAGS) $(LDFLAGS) $^ $(LDLIBS) -o $@

%.o:	%.c lmdb.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $<

COV_FLAGS=-fprofile-arcs -ftest-coverage
COV_OBJS=xmdb.o xmidl.o

coverage: xmtest
	for i in mtest*.c [0-9]*.c; do j=`basename \$$i .c`; $(MAKE) $$j.o; \
		gcc -o x$$j $$j.o $(COV_OBJS) -pthread $(COV_FLAGS); \
		rm -rf testdb; mkdir testdb; ./x$$j; done
	gcov xmdb.c
	gcov xmidl.c

xmtest:	mtest.o xmdb.o xmidl.o
	gcc -o xmtest mtest.o xmdb.o xmidl.o -pthread $(COV_FLAGS)

xmdb.o: mdb.c lmdb.h midl.h
	$(CC) $(CFLAGS) -fPIC $(CPPFLAGS) -O0 $(COV_FLAGS) -c mdb.c -o $@

xmidl.o: midl.c midl.h
	$(CC) $(CFLAGS) -fPIC $(CPPFLAGS) -O0 $(COV_FLAGS) -c midl.c -o $@
