# GNU Makefile for libmdbx, https://github.com/leo-yuriev/libmdbx

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
ifeq ($(shell (export LC_ALL=C; ($(CC) --version 2>&1; $(CC) -v 2>&1) | grep -q -i 'e2k' && echo yes)),yes)
CFLAGS	?= -O3 -g3 -Wall -Werror -Wextra -ffunction-sections -fPIC -fvisibility=hidden
else
CFLAGS	?= -O2 -g3 -Wall -Werror -Wextra -ffunction-sections -fPIC -fvisibility=hidden
endif

XCFLAGS	?= -DNDEBUG=1 -DMDBX_DEBUG=0 -DLIBMDBX_EXPORTS=1
CFLAGS	+= -D_GNU_SOURCE=1 -std=gnu11 -pthread $(XCFLAGS)
CXXFLAGS = -std=c++11 $(filter-out -std=gnu11,$(CFLAGS))
TESTDB	?= $(shell [ -d /dev/shm ] && echo /dev/shm || echo /tmp)/mdbx-test.db
TESTLOG ?= $(shell [ -d /dev/shm ] && echo /dev/shm || echo /tmp)/mdbx-test.log

# LY: '--no-as-needed,-lrt' for ability to built with modern glibc, but then run with the old
LDFLAGS	?= -Wl,--gc-sections,-z,relro,-O,--no-as-needed,-lrt
EXE_LDFLAGS ?= $(LDFLAGS) -static

# LY: just for benchmarking
IOARENA ?= $(shell \
  (test -x ../ioarena/@BUILD/src/ioarena && echo ../ioarena/@BUILD/src/ioarena) || \
  (test -x ../../@BUILD/src/ioarena && echo ../../@BUILD/src/ioarena) || \
  (test -x ../../src/ioarena && echo ../../src/ioarena) || which ioarena)
NN	?= 25000000

########################################################################

HEADERS		:= mdbx.h
LIBRARIES	:= libmdbx.a libmdbx.so
TOOLS		:= mdbx_stat mdbx_copy mdbx_dump mdbx_load mdbx_chk
MANPAGES	:= mdbx_stat.1 mdbx_copy.1 mdbx_dump.1 mdbx_load.1
SHELL		:= /bin/bash

CORE_SRC	:= $(filter-out src/lck-windows.c, $(wildcard src/*.c))
CORE_INC	:= $(wildcard src/*.h)
CORE_OBJ	:= $(patsubst %.c,%.o,$(CORE_SRC))
TEST_SRC	:= $(filter-out test/osal-windows.cc, $(wildcard test/*.cc))
TEST_INC	:= $(wildcard test/*.h)
TEST_OBJ	:= $(patsubst %.cc,%.o,$(TEST_SRC))

.PHONY: mdbx all install clean check coverage

all: $(LIBRARIES) $(TOOLS) test/test example

mdbx: libmdbx.a libmdbx.so

example: mdbx.h tutorial/sample-mdbx.c libmdbx.so
	$(CC) $(CFLAGS) -I. tutorial/sample-mdbx.c ./libmdbx.so -o example

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
	rm -rf $(TOOLS) test/test @* *.[ao] *.[ls]o *~ tmp.db/* *.gcov *.log *.err src/*.o test/*.o

check:	all
	rm -f $(TESTDB) $(TESTLOG) && (set -o pipefail; test/test --pathname=$(TESTDB) --dont-cleanup-after basic | tee -a $(TESTLOG) | tail -n 42) && ./mdbx_chk -vvn $(TESTDB)

check-singleprocess:	all
	rm -f $(TESTDB) $(TESTLOG) && (set -o pipefail; test/test --pathname=$(TESTDB) --dont-cleanup-after --hill | tee -a $(TESTLOG) | tail -n 42) && ./mdbx_chk -vvn $(TESTDB)

check-fault:	all
	rm -f $(TESTDB) $(TESTLOG) && (set -o pipefail; test/test --pathname=$(TESTDB) --inject-writefault=42 --dump-config --dont-cleanup-after basic | tee -a $(TESTLOG) | tail -n 42) && ./mdbx_chk -vvn $(TESTDB)

define core-rule
$(patsubst %.c,%.o,$(1)): $(1) $(CORE_INC) mdbx.h Makefile
	$(CC) $(CFLAGS) -c $(1) -o $$@

endef
$(foreach file,$(CORE_SRC),$(eval $(call core-rule,$(file))))

define test-rule
$(patsubst %.cc,%.o,$(1)): $(1) $(TEST_INC) mdbx.h Makefile
	$(CXX) $(CXXFLAGS) -c $(1) -o $$@

endef
$(foreach file,$(TEST_SRC),$(eval $(call test-rule,$(file))))

libmdbx.a: $(CORE_OBJ)
	$(AR) rs $@ $?

libmdbx.so: $(CORE_OBJ)
	$(CC) $(CFLAGS) -save-temps $^ -pthread -shared $(LDFLAGS) -o $@

mdbx_%:	src/tools/mdbx_%.c libmdbx.a
	$(CC) $(CFLAGS) $^ $(EXE_LDFLAGS) -o $@

test/test: $(TEST_OBJ) libmdbx.a
	$(CXX) $(CXXFLAGS) $^ $(EXE_LDFLAGS) -o $@

###############################################################################

ifneq ($(wildcard $(IOARENA)),)

.PHONY: bench clean-bench re-bench

clean-bench:
	rm -rf bench-*.txt _ioarena/*

re-bench: clean-bench bench

define bench-rule
bench-$(1)_$(2).txt: $(3) $(IOARENA) Makefile
	LD_LIBRARY_PATH="./:$$$${LD_LIBRARY_PATH}" \
		$(IOARENA) -D $(1) -B crud -m nosync -n $(2) \
		| tee $$@ | grep throughput && \
	LD_LIBRARY_PATH="./:$$$${LD_LIBRARY_PATH}" \
		$(IOARENA) -D $(1) -B get,iterate -m sync -r 4 -n $(2) \
		| tee -a $$@ | grep throughput \
	|| mv -f $$@ $$@.error

endef

$(eval $(call bench-rule,mdbx,$(NN),libmdbx.so))

$(eval $(call bench-rule,sophia,$(NN)))
$(eval $(call bench-rule,leveldb,$(NN)))
$(eval $(call bench-rule,rocksdb,$(NN)))
$(eval $(call bench-rule,wiredtiger,$(NN)))
$(eval $(call bench-rule,forestdb,$(NN)))
$(eval $(call bench-rule,lmdb,$(NN)))
$(eval $(call bench-rule,nessdb,$(NN)))
$(eval $(call bench-rule,sqlite3,$(NN)))
$(eval $(call bench-rule,ejdb,$(NN)))
$(eval $(call bench-rule,vedisdb,$(NN)))
$(eval $(call bench-rule,dummy,$(NN)))

$(eval $(call bench-rule,debug,10))

bench: bench-mdbx_$(NN).txt

.PHONY: bench-debug

bench-debug: bench-debug_10.txt

bench-quartet: bench-mdbx_$(NN).txt bench-lmdb_$(NN).txt bench-rocksdb_$(NN).txt bench-wiredtiger_$(NN).txt

endif

###############################################################################

ci-rule = ( CC=$$(which $1); if [ -n "$$CC" ]; then \
		echo -n "probe by $2 ($$(readlink -f $$(which $$CC))): " && \
		$(MAKE) clean >$1.log 2>$1.err && \
		$(MAKE) CC=$$(readlink -f $$CC) XCFLAGS="-UNDEBUG -DMDBX_DEBUG=2 -DLIBMDBX_EXPORTS=1" check 1>$1.log 2>$1.err && echo "OK" \
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

###############################################################################

CROSS_LIST = alpha-linux-gnu-gcc mips-linux-gnu-gcc \
	powerpc64-linux-gnu-gcc powerpc-linux-gnu-gcc \
	arm-linux-gnueabihf-gcc aarch64-linux-gnu-gcc

# hppa-linux-gnu-gcc		- don't supported by current qemu release
# s390x-linux-gnu-gcc		- qemu troubles (hang/abort)
# sh4-linux-gnu-gcc		- qemu troubles (pread syscall, etc)
# mips64-linux-gnuabi64-gcc	- qemu troubles (pread syscall, etc)
# sparc64-linux-gnu-gcc		- qemu troubles (fcntl for F_SETLK/F_GETLK)
CROSS_LIST_NOQEMU = hppa-linux-gnu-gcc s390x-linux-gnu-gcc \
	sh4-linux-gnu-gcc mips64-linux-gnuabi64-gcc sparc64-linux-gnu-gcc

cross-gcc:
	@echo "CORRESPONDING CROSS-COMPILERs ARE REQUIRED."
	@echo "FOR INSTANCE: apt install g++-aarch64-linux-gnu g++-alpha-linux-gnu g++-arm-linux-gnueabihf g++-hppa-linux-gnu g++-mips-linux-gnu g++-mips64-linux-gnuabi64 g++-powerpc-linux-gnu g++-powerpc64-linux-gnu g++-s390x-linux-gnu g++-sh4-linux-gnu"
	@for CC in $(CROSS_LIST_NOQEMU) $(CROSS_LIST); do \
		echo "===================== $$CC"; \
		$(MAKE) clean && CC=$$CC CXX=$$(echo $$CC | sed 's/-gcc/-g++/') EXE_LDFLAGS=-static $(MAKE) all || exit $$?; \
	done

#
# Unfortunately qemu don't provide robust support for futexes.
# Therefore it is impossible to run full multi-process tests.
cross-qemu:
	@echo "CORRESPONDING CROSS-COMPILERs AND QEMUs ARE REQUIRED."
	@echo "FOR INSTANCE: apt install binfmt-support qemu-user-static qemu-user qemu-system-arm qemu-system-mips qemu-system-misc qemu-system-ppc qemu-system-sparc g++-aarch64-linux-gnu g++-alpha-linux-gnu g++-arm-linux-gnueabihf g++-hppa-linux-gnu g++-mips-linux-gnu g++-mips64-linux-gnuabi64 g++-powerpc-linux-gnu g++-powerpc64-linux-gnu g++-s390x-linux-gnu g++-sh4-linux-gnu"
	@for CC in $(CROSS_LIST); do \
		echo "===================== $$CC + qemu"; \
		$(MAKE) clean && CC=$$CC CXX=$$(echo $$CC | sed 's/-gcc/-g++/') EXE_LDFLAGS=-static $(MAKE) check-singleprocess || exit $$?; \
	done
