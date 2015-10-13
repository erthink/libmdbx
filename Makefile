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
prefix	?= /usr/local

########################################################################

IHDRS	:= lmdb.h mdbx.h
ILIBS	:= libmdbx.a libmdbx.so
IPROGS	:= mdbx_stat mdbx_copy mdbx_dump mdbx_load mdbx_chk
IDOCS	:= mdb_stat.1 mdb_copy.1 mdb_dump.1 mdb_load.1
PROGS	:= $(IPROGS) mtest0 mtest1 mtest2 mtest3 mtest4 mtest5 mtest6 wbench

SRC_LMDB := mdb.c midl.c lmdb.h midl.h
SRC_MDBX := $(SRC_LMDB) mdbx.h

.PHONY: mdbx lmdb all install clean test coverage

all: $(ILIBS) $(IPROGS)

mdbx: libmdbx.a libmdbx.so

lmdb: liblmdb.a liblmdb.so

install: $(ILIBS) $(IPROGS) $(IHDRS)
	mkdir -p $(DESTDIR)$(prefix)/bin
	mkdir -p $(DESTDIR)$(prefix)/lib
	mkdir -p $(DESTDIR)$(prefix)/include
	mkdir -p $(DESTDIR)$(prefix)/man/man1
	for f in $(IPROGS); do cp $$f $(DESTDIR)$(prefix)/bin; done
	for f in $(ILIBS); do cp $$f $(DESTDIR)$(prefix)/lib; done
	for f in $(IHDRS); do cp $$f $(DESTDIR)$(prefix)/include; done
	for f in $(IDOCS); do cp $$f $(DESTDIR)$(prefix)/man/man1; done

clean:
	rm -rf $(PROGS) @* *.[ao] *.[ls]o *~ testdb/* *.gcov

test:	mdbx $(PROGS)
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
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ -lrt

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
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ -lrt

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
