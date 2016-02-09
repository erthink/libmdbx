# GNU Makefile for libmdbx (lightning memory-mapped database library for Linux).

########################################################################
# Configuration. The compiler options must enable threaded compilation.
#
# Preprocessor macros (for XCFLAGS) of interest...
# Note that the defaults should already be correct for most
# platforms; you should not need to change any of these.
# Read their descriptions in mdb.c if you do:
#
# - MDB_USE_ROBUST
#
# There may be other macros in mdb.c of interest. You should
# read mdb.c before changing any of them.
#
DESTDIR	?=
prefix	?= /usr/local
mandir	?= $(prefix)/man

CC	?= gcc
XCFLAGS	?=
CFLAGS	?= -O2 -ggdb3 -Wall -Werror -Wno-unused-parameter -DNDEBUG=1
CFLAGS	+= -pthread $(XCFLAGS)

########################################################################

IHDRS	:= lmdb.h mdbx.h
ILIBS	:= libmdbx.a libmdbx.so
IPROGS	:= mdbx_stat mdbx_copy mdbx_dump mdbx_load mdbx_chk
IDOCS	:= mdb_stat.1 mdb_copy.1 mdb_dump.1 mdb_load.1
PROGS	:= $(IPROGS) mtest0 mtest1 mtest2 mtest3 mtest4 mtest5 mtest6 wbench

SRC_LMDB := mdb.c midl.c lmdb.h midl.h reopen.h barriers.h
SRC_MDBX := $(SRC_LMDB) mdbx.h

.PHONY: mdbx lmdb all install clean check tests coverage

all: $(ILIBS) $(IPROGS)

mdbx: libmdbx.a libmdbx.so

lmdb: liblmdb.a liblmdb.so

install: $(ILIBS) $(IPROGS) $(IHDRS)
	mkdir -p $(DESTDIR)$(prefix)/bin \
		&& cp -t $(DESTDIR)$(prefix)/bin $(IPROGS) && \
	mkdir -p $(DESTDIR)$(prefix)/lib \
		&& cp -t $(DESTDIR)$(prefix)/lib $(ILIBS) && \
	mkdir -p $(DESTDIR)$(prefix)/include \
		&& cp -t $(DESTDIR)$(prefix)/include $(IHDRS) && \
	mkdir -p $(DESTDIR)$(mandir)/man1 \
		&& cp -t $(DESTDIR)$(mandir)/man1 $(IDOCS)

clean:
	rm -rf $(PROGS) @* *.[ao] *.[ls]o *~ testdb/* *.gcov

tests:	mdbx $(PROGS)

check:	tests
	[ -d testdb ] || mkdir testdb && rm -f testdb/* \
		&& echo "*** LMDB-TEST-0" && ./mtest0 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-1" && ./mtest1 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-2" && ./mtest2 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-3" && ./mtest3 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-4" && ./mtest4 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-5" && ./mtest5 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-6" && ./mtest6 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TESTs - all done"

libmdbx.a:	mdbx.o
	$(AR) rs $@ $^

libmdbx.so:	mdbx.lo
	$(CC) $(CFLAGS) $(LDFLAGS) -pthread -shared -o $@ $^

liblmdb.a:	lmdb.o
	$(AR) rs $@ $^

liblmdb.so:	lmdb.lo
	$(CC) $(CFLAGS) $(LDFLAGS) -pthread -shared -o $@ $^

mdbx_stat: mdb_stat.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mdbx_copy: mdb_copy.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mdbx_dump: mdb_dump.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mdbx_load: mdb_load.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mdbx_chk: mdb_chk.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ -Wl,--no-as-needed,-lrt

mtest0: mtest0.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mtest1: mtest1.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mtest2:	mtest2.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mtest3:	mtest3.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mtest4:	mtest4.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mtest5:	mtest5.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mtest6:	mtest6.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

wbench:	wbench.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ -Wl,--no-as-needed,-lrt

mdbx.o: $(SRC_MDBX)
	$(CC) $(CFLAGS) -include mdbx.h -c mdb.c -o $@

mdbx.lo: $(SRC_MDBX)
	$(CC) $(CFLAGS) -include mdbx.h -fPIC -c mdb.c -o $@

lmdb.o: $(SRC_LMDB)
	$(CC) $(CFLAGS) -c mdb.c -o $@

lmdb.lo: $(SRC_LMDB)
	$(CC) $(CFLAGS) -fPIC -c mdb.c -o $@

%:	%.o
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@

%.o:	%.c lmdb.h mdbx.h
	$(CC) $(CFLAGS) -c $<

COV_FLAGS=-fprofile-arcs -ftest-coverage

@gcov-mdb.o: $(SRC_MDBX)
	$(CC) $(CFLAGS) $(COV_FLAGS) -O0 -include mdbx.h -c mdb.c -o $@

coverage: @gcov-mdb.o
	for t in mtest*.c; do x=`basename \$$t .c`; $(MAKE) $$x.o; \
		gcc -o @gcov-$$x $$x.o $^ -pthread $(COV_FLAGS); \
		rm -rf testdb; mkdir testdb; ./@gcov-$$x; done
	gcov @gcov-mdb
