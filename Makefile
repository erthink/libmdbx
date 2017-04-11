# GNU Makefile for libmdbx (reliable lightning memory-mapped DB library for Linux).
# https://github.com/ReOpen/libmdbx

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
CXX	?= g++
XCFLAGS	?= -DNDEBUG=1 -DMDB_DEBUG=0 -DLIBMDBX_EXPORTS=1
CFLAGS	?= -O2 -g3 -Wall -Werror -Wextra -ffunction-sections -fPIC -fvisibility=hidden
CFLAGS	+= -D_GNU_SOURCE=1 -std=gnu99 -pthread $(XCFLAGS)
CXXFLAGS = -std=c++11 $(filter-out -std=gnu99,$(CFLAGS))

# LY: '--no-as-needed,-lrt' for ability to built with modern glibc, but then run with the old
LDFLAGS	?= -Wl,--gc-sections,-z,relro,-O,--no-as-needed,-lrt

# LY: just for benchmarking
IOARENA ?= ../ioarena.git/@BUILD/src/ioarena

########################################################################

HEADERS		:= mdbx.h
LIBRARIES	:= libmdbx.a libmdbx.so
TOOLS		:= mdbx_stat mdbx_copy mdbx_dump mdbx_load mdbx_chk
MANPAGES	:= mdbx_stat.1 mdbx_copy.1 mdbx_dump.1 mdbx_load.1

MDBX_SRC	:= mdbx.h $(addprefix src/, mdbx.c osal.c lck-posix.c defs.h bits.h osal.h midl.h)

.PHONY: mdbx all install clean check coverage

all: $(LIBRARIES) $(TOOLS) test/test

mdbx: libmdbx.a libmdbx.so

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
	rm -rf $(TOOLS) test/test @* *.[ao] *.[ls]o *~ tmp.db/* *.gcov *.log *.err

check:	test/test
	test/test --pathname=tmp.db --basic --dont-cleanup-after && ./mdbx_chk -vn tmp.db

mdbx.o: $(MDBX_SRC) Makefile
	$(CC) $(CFLAGS) -c src/mdbx.c -o $@

osal.o: $(MDBX_SRC) Makefile
	$(CC) $(CFLAGS) -c src/osal.c -o $@

lck-posix.o: $(MDBX_SRC) Makefile
	$(CC) $(CFLAGS) -c src/lck-posix.c -o $@

libmdbx.a:	mdbx.o osal.o lck-posix.o
	$(AR) rs $@ $?

libmdbx.so:	mdbx.o osal.o lck-posix.o
	$(CC) $(CFLAGS) -save-temps $^ -pthread -shared $(LDFLAGS) -o $@

mdbx_%:	src/tools/mdbx_%.c libmdbx.a
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

test/%.o: test/%.cc $(wildcard test/*.h) Makefile
	$(CXX) $(CXXFLAGS) -Isrc -c $(filter %.cc, $^) -o $@

test/test: $(patsubst %.cc,%.o,$(filter-out test/osal-windows.cc, $(wildcard test/*.cc))) libmdbx.a
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

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

$(eval $(call bench-rule,dummy,$(NN)))

$(eval $(call bench-rule,debug,10))

bench: bench-mdbx.txt

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
