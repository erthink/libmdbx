# GNU Makefile for libmdbx (reliable lightning memory-mapped DB library for Linux).
# https://github.com/leo-yuriev/libmdbx

########################################################################
# Configuration. The compiler options must enable threaded compilation.
#
# Preprocessor macros (for XCFLAGS) of interest...
# Note that the defaults should already be correct for most
# platforms; you should not need to change any of these.
# Read their descriptions in mdb.c if you do. There may be
# other macros of interest. You should read mdb.c
# before changing any of them.
#

# install sandbox
SANDBOX	?=

# install prefixes (inside sandbox)
prefix	?= /usr/local
mandir	?= $(prefix)/man

# lib/bin suffix for multiarch/biarch, e.g. '.x86_64'
suffix	?=

CC	?= gcc
XCFLAGS	?= -DNDEBUG=1 -DMDB_DEBUG=0
CFLAGS	?= -O2 -g3 -Wall -Werror -Wextra -ffunction-sections
CFLAGS	+= -std=gnu99 -pthread $(XCFLAGS)

# LY: for ability to built with modern glibc,
#     but then run with the old
LDOPS	?= -Wl,--no-as-needed,-lrt

# LY: just for benchmarking
IOARENA ?= ../ioarena.git/@BUILD/src/ioarena

########################################################################

HEADERS		:= lmdb.h mdbx.h
LIBRARIES	:= libmdbx.a libmdbx.so
TOOLS		:= mdbx_stat mdbx_copy mdbx_dump mdbx_load mdbx_chk
MANPAGES	:= mdb_stat.1 mdb_copy.1 mdb_dump.1 mdb_load.1
TESTS		:= mtest0 mtest1 mtest2 mtest3 mtest4 mtest5 mtest6 wbench \
		   yota_test1 yota_test2 mtest7 mtest8

SRC_LMDB	:= mdb.c midl.c lmdb.h midl.h defs.h barriers.h
SRC_MDBX	:= $(SRC_LMDB) mdbx.c mdbx.h

.PHONY: mdbx lmdb all install clean check tests coverage

all: $(LIBRARIES) $(TOOLS)

mdbx: libmdbx.a libmdbx.so

lmdb: liblmdb.a liblmdb.so

tools: $(TOOLS)

install: $(LIBRARIES) $(TOOLS) $(HEADERS)
	mkdir -p $(SANDBOX)$(prefix)/bin$(suffix) \
		&& cp -t $(SANDBOX)$(prefix)/bin$(suffix) $(TOOLS) && \
	mkdir -p $(SANDBOX)$(prefix)/lib$(suffix) \
		&& cp -t $(SANDBOX)$(prefix)/lib$(suffix) $(LIBRARIES) && \
	mkdir -p $(SANDBOX)$(prefix)/include \
		&& cp -t $(SANDBOX)$(prefix)/include $(HEADERS) && \
	mkdir -p $(SANDBOX)$(mandir)/man1 \
		&& cp -t $(SANDBOX)$(mandir)/man1 $(MANPAGES)

clean:
	rm -rf $(TOOLS) $(TESTS) @* *.[ao] *.[ls]o *~ testdb/* *.gcov

tests:	$(TESTS)

check:	tests
	[ -d testdb ] || mkdir testdb && rm -f testdb/* \
		&& echo "*** LMDB-TEST-0" && ./mtest0 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-1" && ./mtest1 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-2" && ./mtest2 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-3" && ./mtest3 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-4" && ./mtest4 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-5" && ./mtest5 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-6" && ./mtest6 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-7" && ./mtest7 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TEST-8" && ./mtest8 && ./mdbx_chk -v testdb \
		&& echo "*** LMDB-TESTs - all done"

libmdbx.a:	mdbx.o
	$(AR) rs $@ $^

libmdbx.so:	mdbx.lo
	$(CC) $(CFLAGS) $(LDFLAGS) -save-temps -pthread -shared $(LDOPS) -o $@ $^

liblmdb.a:	lmdb.o
	$(AR) rs $@ $^

liblmdb.so:	lmdb.lo
	$(CC) $(CFLAGS) $(LDFLAGS) -pthread -shared $(LDOPS) -o $@ $^

mdbx_stat: mdb_stat.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDOPS) -o $@ $^

mdbx_copy: mdb_copy.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDOPS) -o $@ $^

mdbx_dump: mdb_dump.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDOPS) -o $@ $^

mdbx_load: mdb_load.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDOPS) -o $@ $^

mdbx_chk: mdb_chk.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDOPS) -o $@ $^

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

mtest7:	mtest7.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mtest8:	mtest8.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

yota_test1: yota_test1.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

yota_test2: yota_test2.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

wbench:	wbench.o mdbx.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

mdbx.o: $(SRC_MDBX)
	$(CC) $(CFLAGS) -c mdbx.c -o $@

mdbx.lo: $(SRC_MDBX)
	$(CC) $(CFLAGS) -fPIC -c mdbx.c -o $@

lmdb.o: $(SRC_LMDB)
	$(CC) $(CFLAGS) -c mdb.c -o $@

lmdb.lo: $(SRC_LMDB)
	$(CC) $(CFLAGS) -fPIC -c mdb.c -o $@

%:	%.o
	$(CC) $(CFLAGS) $(LDFLAGS) $^ -o $@

%.o:	%.c lmdb.h mdbx.h
	$(CC) $(CFLAGS) -c $<

COFLAGS	= -fprofile-arcs -ftest-coverage

@gcov-mdb.o: $(SRC_MDBX)
	$(CC) $(CFLAGS) $(COFLAGS) -O0 -c mdbx.c -o $@

coverage: @gcov-mdb.o
	for t in mtest*.c; do x=`basename \$$t .c`; $(MAKE) $$x.o; \
		gcc -o @gcov-$$x $$x.o $^ -pthread $(COFLAGS); \
		rm -rf testdb; mkdir testdb; ./@gcov-$$x; done
	gcov @gcov-mdb

ifneq ($(wildcard $(IOARENA)),)

.PHONY: bench clean-bench re-bench

clean-bench:
	rm -rf bench-*.txt _ioarena/*

re-bench: clean-bench bench

NN := 25000000
define bench-rule
bench-$(1).txt: $(3) $(IOARENA) Makefile
	$(IOARENA) -D $(1) -B crud -m nosync -n $(2) | tee $$@ | grep throughput \
	&& $(IOARENA) -D $(1) -B get,iterate -m sync -r 4 -n $(2) | tee -a $$@ | grep throughput \
	|| rm -f $$@

endef

$(eval $(call bench-rule,mdbx,$(NN),libmdbx.so))

$(eval $(call bench-rule,lmdb,$(NN)))

$(eval $(call bench-rule,dummy,$(NN)))

$(eval $(call bench-rule,debug,10))

bench: bench-lmdb.txt bench-mdbx.txt

endif

ci-rule = ( CC=$$(which $1); if [ -n "$$CC" ]; then \
		echo -n "probe by $2 ($$(readlink -f $$(which $$CC))): " && \
		$(MAKE) clean >$1.log 2>$1.err && \
		$(MAKE) CC=$$(readlink -f $$CC) XCFLAGS="-UNDEBUG -DMDB_DEBUG=2" all check 1>$1.log 2>$1.err && echo "OK" \
			|| ( echo "FAILED"; cat $1.err >&2; exit 1 ); \
	else echo "no $2 ($1) for probe"; fi; )
ci:
	@if [ "$$(readlink -f $$(which $(CC)))" != "$$(readlink -f $$(which gcc || echo /bin/false))" -a \
		"$$(readlink -f $$(which $(CC)))" != "$$(readlink -f $$(which clang || echo /bin/false))" -a \
		"$$(readlink -f $$(which $(CC)))" != "$$(readlink -f $$(which icc || echo /bin/false))" ]; then \
		$(call ci-rule,$(CC),default C compiler); \
	fi
	@$(call ci-rule,gcc,GCC)
	@$(call ci-rule,clang,clang LLVM)
	@$(call ci-rule,icc,Intel C)
