# This makefile is for GNU Make 3.80 or above, and nowadays provided
# just for compatibility and preservation of traditions.
#
# Please use CMake in case of any difficulties or
# problems with this old-school's magic.
#
################################################################################
#
# Use `make options` to list the available libmdbx build options.
#
# Note that the defaults should already be correct for most platforms;
# you should not need to change any of these. Read their descriptions
# in README and source code (see src/options.h) if you do.
#

SHELL   := env bash

# install sandbox
DESTDIR ?=

# install prefixes (inside sandbox)
prefix  ?= /usr/local
mandir  ?= $(prefix)/man

# lib/bin suffix for multiarch/biarch, e.g. '.x86_64'
suffix  ?=

INSTALL ?= install
CC      ?= gcc
CFLAGS_EXTRA ?=
LD      ?= ld
MDBX_BUILD_OPTIONS ?= -DNDEBUG=1
CFLAGS  ?= -std=gnu11 -O2 -g -Wall -Werror -Wextra -Wpedantic -ffunction-sections -fPIC -fvisibility=hidden -pthread -Wno-error=attributes $(CFLAGS_EXTRA)
# -Wno-tautological-compare
CXX     ?= g++
# Choosing C++ standard with deferred simple variable expansion trick
CXXSTD  ?= $(eval CXXSTD := $$(shell PROBE=$$$$([ -f mdbx.c++ ] && echo mdbx.c++ || echo src/mdbx.c++); for std in gnu++20 c++20 gnu++2a c++2a gnu++17 c++17 gnu++14 c++14 gnu+11 c++11; do $(CXX) -std=$$$${std} -c $$$${PROBE} -o /dev/null 2>/dev/null >/dev/null && echo "-std=$$$${std}" && exit; done))$(CXXSTD)
CXXFLAGS = $(CXXSTD) $(filter-out -std=gnu11,$(CFLAGS))

# TIP: Try append '--no-as-needed,-lrt' for ability to built with modern glibc, but then use with the old.
LIBS    ?= $(shell uname | grep -qi SunOS && echo "-lkstat") $(shell uname | grep -qi -e Darwin -e OpenBSD || echo "-lrt") $(shell uname | grep -qi Windows && echo "-lntdll")

LDFLAGS ?= $(shell $(LD) --help 2>/dev/null | grep -q -- --gc-sections && echo '-Wl,--gc-sections,-z,relro,-O1')$(shell $(LD) --help 2>/dev/null | grep -q -- -dead_strip && echo '-Wl,-dead_strip')
EXE_LDFLAGS ?= -pthread

################################################################################

UNAME      := $(shell uname -s 2>/dev/null || echo Unknown)
define uname2sosuffix
  case "$(UNAME)" in
    Darwin*|Mach*) echo dylib;;
    CYGWIN*|MINGW*|MSYS*|Windows*) echo dll;;
    *) echo so;;
  esac
endef
SO_SUFFIX  := $(shell $(uname2sosuffix))
HEADERS    := mdbx.h mdbx.h++
LIBRARIES  := libmdbx.a libmdbx.$(SO_SUFFIX)
TOOLS      := mdbx_stat mdbx_copy mdbx_dump mdbx_load mdbx_chk mdbx_drop
MANPAGES   := mdbx_stat.1 mdbx_copy.1 mdbx_dump.1 mdbx_load.1 mdbx_chk.1 mdbx_drop.1

.PHONY: all help options lib tools clean install uninstall
.PHONY: install-strip install-no-strip strip libmdbx mdbx show-options

ifeq ("$(origin V)", "command line")
  MDBX_BUILD_VERBOSE := $(V)
endif
ifndef MDBX_BUILD_VERBOSE
  MDBX_BUILD_VERBOSE := 0
endif

ifeq ($(MDBX_BUILD_VERBOSE),1)
  QUIET :=
  HUSH :=
  $(info ## TIP: Use `make V=0` for quiet.)
else
  QUIET := @
  HUSH := >/dev/null
  $(info ## TIP: Use `make V=1` for verbose.)
endif

all: show-options $(LIBRARIES) $(TOOLS)

help:
	@echo "  make all                 - build libraries and tools"
	@echo "  make help                - print this help"
	@echo "  make options             - list build options"
	@echo "  make lib                 - build libraries"
	@echo "  make tools               - built tools"
	@echo "  make clean               "
	@echo "  make install             "
	@echo "  make uninstall           "
	@echo ""
	@echo "  make strip               - strip debug symbols from binaries"
	@echo "  make install-no-strip    - install explicitly without strip"
	@echo "  make install-strip       - install explicitly with strip"
	@echo ""
	@echo "  make bench               - run ioarena-benchmark"
	@echo "  make bench-couple        - run ioarena-benchmark for mdbx and lmdb"
	@echo "  make bench-triplet       - run ioarena-benchmark for mdbx, lmdb, sqlite3"
	@echo "  make bench-quartet       - run ioarena-benchmark for mdbx, lmdb, rocksdb, wiredtiger"
	@echo "  make bench-clean         - remove temp database(s) after benchmark"
#> dist-cutoff-begin
	@echo ""
	@echo "  make test                - basic test"
	@echo "  make check               - basic test"
	@echo "  make memcheck            - build with Valgrind's and test with memcheck tool"
	@echo "  make test-valgrind       - build with Valgrind's and test with memcheck tool"
	@echo "  make test-asan           - build with AddressSanitizer and test"
	@echo "  make test-leak           - build with LeakSanitizer and test"
	@echo "  make test-ubsan          - build with UndefinedBehaviourSanitizer and test"
	@echo "  make cross-gcc           - run cross-compilation testset"
	@echo "  make cross-qemu          - run cross-compilation and execution testset with QEMU"
	@echo "  make gcc-analyzer        - run gcc-analyzer (mostly useless for now)"
	@echo "  make build-test          - build test executable(s)"
	@echo "  make test-fault          - run transaction owner failure testcase"
	@echo "  make test-singleprocess  - run single-process test (used by make cross-qemu)"
	@echo ""
	@echo "  make dist                - build amalgamated source code"
	@echo "  make doxygen             - build HTML documentation"
	@echo "  make release-assets      - build release assets"
	@echo "  make reformat            - reformat source code with clang-format"
#< dist-cutoff-end

show-options:
	@echo "  MDBX_BUILD_OPTIONS =$(MDBX_BUILD_OPTIONS)"
	@echo '## TIP: Use `make options` to listing available build options.'
	@echo "  CFLAGS   =$(CFLAGS)"
	@echo "  CXXFLAGS =$(CXXFLAGS)"
	@echo "  LDFLAGS  =$(LDFLAGS) $(LIBS) $(EXE_LDFLAGS)"
	@echo '## TIP: Use `make help` to listing available targets.'

options:
	@echo "  INSTALL      =$(INSTALL)"
	@echo "  DESTDIR      =$(DESTDIR)"
	@echo "  prefix       =$(prefix)"
	@echo "  mandir       =$(mandir)"
	@echo "  suffix       =$(suffix)"
	@echo ""
	@echo "  CC           =$(CC)"
	@echo "  CFLAGS_EXTRA =$(CFLAGS_EXTRA)"
	@echo "  CFLAGS       =$(CFLAGS)"
	@echo "  CXX          =$(CXX)"
	@echo "  CXXSTD       =$(CXXSTD)"
	@echo "  CXXFLAGS     =$(CXXFLAGS)"
	@echo ""
	@echo "  LD           =$(LD)"
	@echo "  LDFLAGS      =$(LDFLAGS)"
	@echo "  EXE_LDFLAGS  =$(EXE_LDFLAGS)"
	@echo "  LIBS         =$(LIBS)"
	@echo ""
	@echo "  MDBX_BUILD_OPTIONS =$(MDBX_BUILD_OPTIONS)"
	@echo ""
	@echo "## Assortment items for MDBX_BUILD_OPTIONS:"
	@echo "##   Note that the defaults should already be correct for most platforms;"
	@echo "##   you should not need to change any of these. Read their descriptions"
#> dist-cutoff-begin
ifeq ($(wildcard mdbx.c),mdbx.c)
#< dist-cutoff-end
	@echo "##   in README and source code (see mdbx.c) if you do."
	@grep -h '#ifndef MDBX_' mdbx.c | grep -v BUILD | uniq | sed 's/#ifndef /  /'
#> dist-cutoff-begin
else
	@echo "##   in README and source code (see src/options.h) if you do."
	@grep -h '#ifndef MDBX_' src/internals.h src/options.h | grep -v BUILD | uniq | sed 's/#ifndef /  /'
endif
#< dist-cutoff-end

lib libmdbx mdbx: libmdbx.a libmdbx.$(SO_SUFFIX)

tools: $(TOOLS)

strip: all
	@echo '  STRIP libmdbx.$(SO_SUFFIX) $(TOOLS)'
	$(TRACE )strip libmdbx.$(SO_SUFFIX) $(TOOLS)

clean:
	@echo '  REMOVE ...'
	$(QUIET)rm -rf $(TOOLS) mdbx_test @* *.[ao] *.[ls]o *.$(SO_SUFFIX) *.dSYM *~ tmp.db/* \
		*.gcov *.log *.err src/*.o test/*.o mdbx_example dist \
		config.h src/config.h src/version.c *.tar*

libmdbx.a: mdbx-static.o mdbx++-static.o
	@echo '  AR $@'
	$(QUIET)$(AR) rcs $@ $? $(HUSH)

libmdbx.$(SO_SUFFIX): mdbx-dylib.o mdbx++-dylib.o
	@echo '  LD $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $^ -pthread -shared $(LDFLAGS) $(LIBS) -o $@

#> dist-cutoff-begin
ifeq ($(wildcard mdbx.c),mdbx.c)
#< dist-cutoff-end

################################################################################
# Amalgamated source code, i.e. distributed after `make dist`
MAN_SRCDIR := man1/

config.h: mdbx.c $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)(echo '#define MDBX_BUILD_TIMESTAMP "$(shell date +%Y-%m-%dT%H:%M:%S%z)"' \
	&& echo '#define MDBX_BUILD_FLAGS "$(CXXSTD) $(CFLAGS) $(LDFLAGS) $(LIBS)"' \
	&& echo '#define MDBX_BUILD_COMPILER "$(shell (LC_ALL=C $(CC) --version || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	&& echo '#define MDBX_BUILD_TARGET "$(shell set -o pipefail; (LC_ALL=C $(CC) -v 2>&1 | grep -i '^Target:' | cut -d ' ' -f 2- || (LC_ALL=C $(CC) --version | grep -qi e2k && echo E2K) || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	) > $@

mdbx-dylib.o: config.h mdbx.c mdbx.h $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c mdbx.c -o $@

mdbx-static.o: config.h mdbx.c mdbx.h $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c mdbx.c -o $@

mdbx++-dylib.o: config.h mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c mdbx.c++ -o $@

mdbx++-static.o: config.h mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c mdbx.c++ -o $@

mdbx_%:	mdbx_%.c libmdbx.a
	@echo '  CC+LD $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' $^ $(EXE_LDFLAGS) $(LIBS) -o $@

#> dist-cutoff-begin
else
################################################################################
# Plain (non-amalgamated) sources with test

.PHONY: build-test check cross-gcc cross-qemu dist doxygen gcc-analyzer
.PHONY: reformat release-assets tags test test-asan test-fault test-leak
.PHONY: test-singleprocess test-ubsan test-valgrind memcheck

define uname2osal
  case "$(UNAME)" in
    CYGWIN*|MINGW*|MSYS*|Windows*) echo windows;;
    *) echo unix;;
  esac
endef

define uname2titer
  case "$(UNAME)" in
    Darwin*|Mach*) echo 2;;
    *) echo 12;;
  esac
endef

DIST_EXTRA := LICENSE README.md CMakeLists.txt GNUmakefile Makefile ChangeLog.md VERSION config.h.in ntdll.def \
	$(addprefix man1/, $(MANPAGES)) cmake/compiler.cmake cmake/profile.cmake cmake/utils.cmake
DIST_SRC   := mdbx.h mdbx.h++ mdbx.c mdbx.c++ $(addsuffix .c, $(TOOLS))

TEST_DB    ?= $(shell [ -d /dev/shm ] && echo /dev/shm || echo /tmp)/mdbx-test.db
TEST_LOG   ?= $(shell [ -d /dev/shm ] && echo /dev/shm || echo /tmp)/mdbx-test.log
TEST_OSAL  := $(shell $(uname2osal))
TEST_ITER  := $(shell $(uname2titer))
TEST_SRC   := test/osal-$(TEST_OSAL).cc $(filter-out $(wildcard test/osal-*.cc), $(wildcard test/*.cc))
TEST_INC   := $(wildcard test/*.h)
TEST_OBJ   := $(patsubst %.cc,%.o,$(TEST_SRC))
TAR        ?= $(shell which gnu-tar || echo tar)
ZIP        ?= $(shell which zip || echo "echo 'Please install zip'")
CLANG_FORMAT ?= $(shell (which clang-format-12 || which clang-format-11 || which clang-format-10 || which clang-format) 2>/dev/null)

reformat:
	@echo '  RUNNING clang-format...'
	$(QUIET)if [ -n "$(CLANG_FORMAT)" ]; then \
		git ls-files | grep -E '\.(c|cxx|cc|cpp|h|hxx|hpp)(\.in)?$$' | xargs -r $(CLANG_FORMAT) -i --style=file; \
	else \
		echo "clang-format version 8..12 not found for 'reformat'"; \
	fi

MAN_SRCDIR := src/man1/
ALLOY_DEPS := $(wildcard src/*)
git_DIR := $(shell if [ -d .git ]; then echo .git; elif [ -s .git -a -f .git ]; then grep '^gitdir: ' .git | cut -d ':' -f 2; else echo git_directory_is_absent; fi)
MDBX_GIT_VERSION = $(shell set -o pipefail; git describe --tags 2>&- | sed -n 's|^v*\([0-9]\{1,\}\.[0-9]\{1,\}\.[0-9]\{1,\}\)\(.*\)|\1|p' || echo 'Please fetch tags and/or use non-obsolete git version')
MDBX_GIT_REVISION = $(shell set -o pipefail; git rev-list --count HEAD ^`git tag --sort=-version:refname 2>&- | sed -n '/^\(v[0-9]\+\.[0-9]\+\.[0-9]\+\)*/p;q' || echo 'git_tag_with_sort_was_failed'` 2>&- || echo 'Please use non-obsolete git version')
MDBX_GIT_TIMESTAMP = $(shell git show --no-patch --format=%cI HEAD 2>&- || echo 'Please install latest get version')
MDBX_GIT_DESCRIBE = $(shell git describe --tags --long --dirty=-dirty 2>&- || echo 'Please fetch tags and/or install non-obsolete git version')
MDBX_VERSION_SUFFIX = $(shell set -o pipefail; echo -n '$(MDBX_GIT_DESCRIBE)' | tr -c -s '[a-zA-Z0-9]' _)
MDBX_BUILD_SOURCERY = $(shell set -o pipefail; $(MAKE) CXXSTD= -s src/version.c >/dev/null && (openssl dgst -r -sha256 src/version.c || sha256sum src/version.c || shasum -a 256 src/version.c) 2>/dev/null | cut -d ' ' -f 1 || (echo 'Please install openssl or sha256sum or shasum' >&2 && echo sha256sum_is_no_available))_$(MDBX_VERSION_SUFFIX)
MDBX_DIST_DIR = libmdbx-$(MDBX_VERSION_SUFFIX)

# Extra options mdbx_test utility
MDBX_TEST_EXTRA ?=

check: test dist

test: build-test
	@echo '  RUNNING `mdbx_test basic`...'
	$(QUIET)rm -f $(TEST_DB) $(TEST_LOG).gz && (set -o pipefail; \
		(./mdbx_test --table=+data.integer --keygen.split=29 --datalen.min=min --datalen.max=max --progress --console=no --repeat=$(TEST_ITER) --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_TEST_EXTRA) basic && \
		./mdbx_test --mode=-writemap,-nosync-safe,-lifo --progress --console=no --repeat=12 --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_TEST_EXTRA) basic) \
		| tee >(gzip --stdout > $(TEST_LOG).gz) | tail -n 42) \
	&& ./mdbx_chk -vvn $(TEST_DB) && ./mdbx_chk -vvn $(TEST_DB)-copy

test-singleprocess: all mdbx_test
	@echo '  RUNNING `mdbx_test --nested`...'
	$(QUIET)rm -f $(TEST_DB) $(TEST_LOG).gz && (set -o pipefail; \
		(./mdbx_test --table=+data.integer --keygen.split=29 --datalen.min=min --datalen.max=max --progress --console=no --repeat=42 --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_TEST_EXTRA) --hill && \
		./mdbx_test --progress --console=no --repeat=2 --pathname=$(TEST_DB) --dont-cleanup-before --dont-cleanup-after --copy && \
		./mdbx_test --mode=-writemap,-nosync-safe,-lifo --progress --console=no --repeat=42 --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_TEST_EXTRA) --nested) \
		| tee >(gzip --stdout > $(TEST_LOG).gz) | tail -n 42) \
	&& ./mdbx_chk -vvn $(TEST_DB) && ./mdbx_chk -vvn $(TEST_DB)-copy

test-fault: all mdbx_test
	@echo '  RUNNING `mdbx_test --inject-writefault=42 basic`...'
	$(QUIET)rm -f $(TEST_DB) $(TEST_LOG).gz && (set -o pipefail; ./mdbx_test --progress --console=no --pathname=$(TEST_DB) --inject-writefault=42 --dump-config --dont-cleanup-after $(MDBX_TEST_EXTRA) basic \
		| tee >(gzip --stdout > $(TEST_LOG).gz) | tail -n 42) \
	; ./mdbx_chk -vvnw $(TEST_DB) && ([ ! -e $(TEST_DB)-copy ] || ./mdbx_chk -vvn $(TEST_DB)-copy)

VALGRIND=valgrind --trace-children=yes --log-file=valgrind-%p.log --leak-check=full --track-origins=yes --error-exitcode=42 --suppressions=test/valgrind_suppress.txt
memcheck test-valgrind:
	@echo "  RUNNING \`mdbx_test basic\` under Valgrind's memcheck..."
	$(QUIET)$(MAKE) CXXSTD= clean && $(MAKE) CXXSTD=$(CXXSTD) CFLAGS_EXTRA="-Ofast -DMDBX_USE_VALGRIND" build-test && \
	rm -f valgrind-*.log $(TEST_DB) $(TEST_LOG).gz && (set -o pipefail; ( \
		$(VALGRIND) ./mdbx_test --table=+data.integer --keygen.split=29 --datalen.min=min --datalen.max=max --progress --console=no --repeat=2 --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_TEST_EXTRA) basic && \
		$(VALGRIND) ./mdbx_test --progress --console=no --pathname=$(TEST_DB) --dont-cleanup-before --dont-cleanup-after --copy && \
		$(VALGRIND) ./mdbx_test --mode=-writemap,-nosync-safe,-lifo --progress --console=no --repeat=4 --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_TEST_EXTRA) basic && \
		$(VALGRIND) ./mdbx_chk -vvn $(TEST_DB) && \
		$(VALGRIND) ./mdbx_chk -vvn $(TEST_DB)-copy \
	) | tee >(gzip --stdout > $(TEST_LOG).gz) | tail -n 42)

gcc-analyzer:
	@echo '  RE-BUILD with `-fanalyzer` option...'
	@echo "NOTE: There a lot of false-positive warnings at 2020-05-01 by pre-release GCC-10 (20200328, Red Hat 10.0.1-0.11)"
	$(QUIET)$(MAKE) CXXSTD=$(CXXSTD) --always-make CFLAGS_EXTRA="-Og -fanalyzer -Wno-error" build-test

test-ubsan:
	@echo '  RE-CHECK with `-fsanitize=undefined` option...'
	$(QUIET)$(MAKE) CXXSTD= clean && $(MAKE) CXXSTD=$(CXXSTD) CFLAGS_EXTRA="-Ofast -fsanitize=undefined -fsanitize-undefined-trap-on-error" check

test-asan:
	@echo '  RE-CHECK with `-fsanitize=address` option...'
	$(QUIET)$(MAKE) CXXSTD= clean && $(MAKE) CXXSTD=$(CXXSTD) CFLAGS_EXTRA="-Os -fsanitize=address" check

test-leak:
	@echo '  RE-CHECK with `-fsanitize=leak` option...'
	$(QUIET)$(MAKE) CXXSTD= clean && $(MAKE) CXXSTD=$(CXXSTD) CFLAGS_EXTRA="-fsanitize=leak" check

mdbx_example: mdbx.h example/example-mdbx.c libmdbx.$(SO_SUFFIX)
	@echo '  CC+LD $@'
	$(QUIET)$(CC) $(CFLAGS) -I. example/example-mdbx.c ./libmdbx.$(SO_SUFFIX) -o $@

build-test: all mdbx_example mdbx_test

define test-rule
$(patsubst %.cc,%.o,$(1)): $(1) $(TEST_INC) mdbx.h $(lastword $(MAKEFILE_LIST))
	@echo '  CC $$@'
	$(QUIET)$$(CXX) $$(CXXFLAGS) $$(MDBX_BUILD_OPTIONS) -c $(1) -o $$@

endef
$(foreach file,$(TEST_SRC),$(eval $(call test-rule,$(file))))

mdbx_%:	src/mdbx_%.c libmdbx.a
	@echo '  CC+LD $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' $^ $(EXE_LDFLAGS) $(LIBS) -o $@

mdbx_test: $(TEST_OBJ) libmdbx.$(SO_SUFFIX)
	@echo '  LD $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(TEST_OBJ) -Wl,-rpath . -L . -l mdbx $(EXE_LDFLAGS) $(LIBS) -o $@

$(git_DIR)/HEAD $(git_DIR)/index $(git_DIR)/refs/tags:
	@echo '*** ' >&2
	@echo '*** Please don''t use tarballs nor zips which are automatically provided by Github !' >&2
	@echo '*** These archives do not contain version information and thus are unfit to build libmdbx.' >&2
	@echo '*** You can vote for ability of disabling auto-creation such unsuitable archives at https://github.community/t/disable-tarball' >&2
	@echo '*** ' >&2
	@echo '*** Instead of above, just clone the git repository, either download a tarball or zip with the properly amalgamated source core.' >&2
	@echo '*** For embedding libmdbx use a git-submodule or the amalgamated source code.' >&2
	@echo '*** ' >&2
	@echo '*** Please, avoid using any other techniques.' >&2
	@echo '*** ' >&2
	@false

src/version.c: src/version.c.in $(lastword $(MAKEFILE_LIST)) $(git_DIR)/HEAD $(git_DIR)/index $(git_DIR)/refs/tags
	@echo '  MAKE $@'
	$(QUIET)sed \
		-e "s|@MDBX_GIT_TIMESTAMP@|$(MDBX_GIT_TIMESTAMP)|" \
		-e "s|@MDBX_GIT_TREE@|$(shell git show --no-patch --format=%T HEAD || echo 'Please install latest get version')|" \
		-e "s|@MDBX_GIT_COMMIT@|$(shell git show --no-patch --format=%H HEAD || echo 'Please install latest get version')|" \
		-e "s|@MDBX_GIT_DESCRIBE@|$(MDBX_GIT_DESCRIBE)|" \
		-e "s|\$${MDBX_VERSION_MAJOR}|$(shell echo '$(MDBX_GIT_VERSION)' | cut -d . -f 1)|" \
		-e "s|\$${MDBX_VERSION_MINOR}|$(shell echo '$(MDBX_GIT_VERSION)' | cut -d . -f 2)|" \
		-e "s|\$${MDBX_VERSION_RELEASE}|$(shell echo '$(MDBX_GIT_VERSION)' | cut -d . -f 3)|" \
		-e "s|\$${MDBX_VERSION_REVISION}|$(MDBX_GIT_REVISION)|" \
	src/version.c.in > $@

src/config.h: src/version.c $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)(echo '#define MDBX_BUILD_TIMESTAMP "$(shell date +%Y-%m-%dT%H:%M:%S%z)"' \
	&& echo '#define MDBX_BUILD_FLAGS "$(CXXSTD) $(CFLAGS) $(LDFLAGS) $(LIBS)"' \
	&& echo '#define MDBX_BUILD_COMPILER "$(shell (LC_ALL=C $(CC) --version || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	&& echo '#define MDBX_BUILD_TARGET "$(shell set -o pipefail; (LC_ALL=C $(CC) -v 2>&1 | grep -i '^Target:' | cut -d ' ' -f 2- || (LC_ALL=C $(CC) --version | grep -qi e2k && echo E2K) || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	&& echo '#define MDBX_BUILD_SOURCERY $(MDBX_BUILD_SOURCERY)' \
	) > $@

mdbx-dylib.o: src/config.h src/version.c src/alloy.c $(ALLOY_DEPS) $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c src/alloy.c -o $@

mdbx-static.o: src/config.h src/version.c src/alloy.c $(ALLOY_DEPS) $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c src/alloy.c -o $@

docs/Doxyfile: docs/Doxyfile.in src/version.c
	@echo '  MAKE $@'
	$(QUIET)sed \
		-e "s|@MDBX_GIT_TIMESTAMP@|$(MDBX_GIT_TIMESTAMP)|" \
		-e "s|@MDBX_GIT_TREE@|$(shell git show --no-patch --format=%T HEAD || echo 'Please install latest get version')|" \
		-e "s|@MDBX_GIT_COMMIT@|$(shell git show --no-patch --format=%H HEAD || echo 'Please install latest get version')|" \
		-e "s|@MDBX_GIT_DESCRIBE@|$(MDBX_GIT_DESCRIBE)|" \
		-e "s|\$${MDBX_VERSION_MAJOR}|$(shell echo '$(MDBX_GIT_VERSION)' | cut -d . -f 1)|" \
		-e "s|\$${MDBX_VERSION_MINOR}|$(shell echo '$(MDBX_GIT_VERSION)' | cut -d . -f 2)|" \
		-e "s|\$${MDBX_VERSION_RELEASE}|$(shell echo '$(MDBX_GIT_VERSION)' | cut -d . -f 3)|" \
		-e "s|\$${MDBX_VERSION_REVISION}|$(MDBX_GIT_REVISION)|" \
	docs/Doxyfile.in > $@

define md-extract-section
docs/__$(1).md: $(2)
	@echo '  EXTRACT $1'
	$(QUIET)sed -n '/<!-- section-begin $(1) -->/,/<!-- section-end -->/p' $$< > $$@ && test -s $$@

endef
$(foreach section,overview mithril characteristics improvements history usage performance bindings,$(eval $(call md-extract-section,$(section),README.md)))

docs/overall.md: docs/__overview.md docs/_toc.md docs/__mithril.md docs/__history.md AUTHORS LICENSE
	@echo '  MAKE $@'
	$(QUIET)echo -e "\\mainpage Overall\n\\section brief Brief" | cat - $(filter %.md, $^) > $@ && echo -e "\n\n\nLicense\n=======\n" | cat AUTHORS - LICENSE >> $@

docs/intro.md: docs/_preface.md docs/__characteristics.md docs/__improvements.md docs/_restrictions.md docs/__performance.md
	@echo '  MAKE $@'
	$(QUIET)cat $^ | sed 's/^Performance comparison$$/Performance comparison {#performance}/' > $@

docs/usage.md: docs/__usage.md docs/_starting.md docs/__bindings.md
	@echo '  MAKE $@'
	$(QUIET)echo -e "\\page usage Usage\n\\section getting Building & Embedding" | cat - $^ | sed 's/^Bindings$$/Bindings {#bindings}/' > $@

doxygen: docs/Doxyfile docs/overall.md docs/intro.md docs/usage.md mdbx.h mdbx.h++ src/options.h ChangeLog.md AUTHORS LICENSE
	@echo '  RUNNING doxygen...'
	$(QUIET)rm -rf docs/html && \
	cat mdbx.h | tr '\n' '\r' | sed -e 's/LIBMDBX_INLINE_API\s*(\s*\([^,]\+\),\s*\([^,]\+\),\s*(\s*\([^)]\+\)\s*)\s*)\s*{/inline \1 \2(\3) {/g' | tr '\r' '\n' > docs/mdbx.h && \
	cp mdbx.h++ src/options.h ChangeLog.md docs/ && (cd docs && doxygen Doxyfile $(HUSH)) && cp AUTHORS LICENSE docs/html/

mdbx++-dylib.o: src/config.h src/mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c src/mdbx.c++ -o $@

mdbx++-static.o: src/config.h src/mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c src/mdbx.c++ -o $@

dist: tags dist-checked.tag libmdbx-sources-$(MDBX_VERSION_SUFFIX).tar.gz $(lastword $(MAKEFILE_LIST))
	@echo '  AMALGAMATION is done'

tags:
	@echo '  FETCH git tags...'
	$(QUIET)git fetch --tags --force

release-assets: libmdbx-sources-$(MDBX_VERSION_SUFFIX).tar.gz libmdbx-sources-$(MDBX_VERSION_SUFFIX).zip
	@echo '  RELEASE ASSETS are done'

dist-checked.tag: $(addprefix dist/, $(DIST_SRC) $(DIST_EXTRA))
	@echo -n '  VERIFY amalgamated sources...'
	$(QUIET)rm -rf $@ \
	&& if grep -R "define xMDBX_ALLOY" dist | grep -q MDBX_BUILD_SOURCERY; then echo "sed output is WRONG!" >&2; exit 2; fi \
	&& rm -rf dist-check && cp -r -p dist dist-check && ($(MAKE) -C dist-check > dist-check/build.log 2> dist-check/build.err || (cat dist-check/build.err && exit 1)) \
	&& touch $@ || (echo " FAILED! See dist-check/build.err" >&2; exit 2) && echo " Ok" \
	&& rm dist/@tmp-shared_internals.inc

libmdbx-sources-$(MDBX_VERSION_SUFFIX).tar.gz: dist-checked.tag
	@echo '  CREATE $@'
	$(QUIET)$(TAR) -c $(shell LC_ALL=C $(TAR) --help | grep -q -- '--owner' && echo '--owner=0 --group=0') -f - -C dist $(DIST_SRC) $(DIST_EXTRA) | gzip -c -9 > $@

libmdbx-sources-$(MDBX_VERSION_SUFFIX).zip: dist-checked.tag
	@echo '  CREATE $@'
	$(QUIET)rm -rf $@ && (cd dist && $(ZIP) -9 ../$@ $(DIST_SRC) $(DIST_EXTRA))

dist/mdbx.h: mdbx.h src/version.c $(lastword $(MAKEFILE_LIST))
	@echo '  COPY $@'
	$(QUIET)mkdir -p dist && cp $< $@

dist/mdbx.h++: mdbx.h++ src/version.c $(lastword $(MAKEFILE_LIST))
	@echo '  COPY $@'
	$(QUIET)mkdir -p dist && cp $< $@

dist/@tmp-shared_internals.inc: src/version.c $(ALLOY_DEPS) $(lastword $(MAKEFILE_LIST))
	@echo '  ALLOYING...'
	$(QUIET)mkdir -p dist \
	&& echo '#define xMDBX_ALLOY 1' > dist/@tmp-sed.inc && echo '#define MDBX_BUILD_SOURCERY $(MDBX_BUILD_SOURCERY)' >> dist/@tmp-sed.inc \
	&& sed \
		-e '/#pragma once/r dist/@tmp-sed.inc' \
		-e 's|#include "../mdbx.h"|@INCLUDE "mdbx.h"|' \
		-e '/#include "defs.h"/r src/defs.h' \
		-e '/#include "osal.h"/r src/osal.h' \
		-e '/#include "options.h"/r src/options.h' \
		src/internals.h > $@ \
	&& rm -rf dist/@tmp-sed.inc

dist/mdbx.c: dist/@tmp-shared_internals.inc $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)mkdir -p dist && (cat dist/@tmp-shared_internals.inc \
	&& cat src/core.c src/osal.c src/version.c src/lck-windows.c src/lck-posix.c \
	) | grep -v -e '#include "' -e '#pragma once' | sed 's|@INCLUDE|#include|' > $@

dist/mdbx.c++: dist/@tmp-shared_internals.inc src/mdbx.c++ $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)mkdir -p dist && (cat dist/@tmp-shared_internals.inc && cat src/mdbx.c++) \
	| grep -v -e '#include "' -e '#pragma once' | sed 's|@INCLUDE|#include|;s|"mdbx.h"|"mdbx.h++"|' > $@

define dist-tool-rule
dist/$(1).c: src/$(1).c src/wingetopt.h src/wingetopt.c \
		dist/@tmp-shared_internals.inc $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $$@'
	$(QUIET)mkdir -p dist && sed \
		-e '/#include "internals.h"/r dist/@tmp-shared_internals.inc' \
		-e '/#include "wingetopt.h"/r src/wingetopt.c' \
		src/$(1).c \
	| grep -v -e '#include "' -e '#pragma once' -e '#define xMDBX_ALLOY' \
	| sed 's|@INCLUDE|#include|' > $$@

endef
$(foreach file,$(TOOLS),$(eval $(call dist-tool-rule,$(file))))

define dist-extra-rule
dist/$(1): $(1)
	@echo '  REFINE $$@'
	$(QUIET)mkdir -p $$(dir $$@) && sed -e '/^#> dist-cutoff-begin/,/^#< dist-cutoff-end/d' $$< > $$@

endef
$(foreach file,$(filter-out man1/% VERSION %.in ntdll.def,$(DIST_EXTRA)),$(eval $(call dist-extra-rule,$(file))))

dist/VERSION: src/version.c
	@echo '  MAKE $@'
	$(QUIET)mkdir -p dist/ && echo "$(MDBX_GIT_VERSION).$(MDBX_GIT_REVISION)" > $@

dist/ntdll.def: src/ntdll.def
	@echo '  COPY $@'
	$(QUIET)mkdir -p dist/cmake/ && cp $< $@

dist/config.h.in: src/config.h.in
	@echo '  COPY $@'
	$(QUIET)mkdir -p dist/cmake/ && cp $< $@

dist/man1/mdbx_%.1: src/man1/mdbx_%.1
	@echo '  COPY $@'
	$(QUIET)mkdir -p dist/man1/ && cp $< $@

endif

################################################################################
# Cross-compilation simple test

CROSS_LIST = sparc64-linux-gnu-gcc alpha-linux-gnu-gcc mips-linux-gnu-gcc \
	powerpc64-linux-gnu-gcc powerpc-linux-gnu-gcc \
	arm-linux-gnueabihf-gcc aarch64-linux-gnu-gcc \
	sh4-linux-gnu-gcc mips64-linux-gnuabi64-gcc \
	hppa-linux-gnu-gcc s390x-linux-gnu-gcc

## On Ubuntu Focal (20.04) with QEMU 4.2 (1:4.2-3ubuntu6.6) & GCC 9.3 (9.3.0-17ubuntu1~20.04)
# hppa-linux-gnu-gcc          - works (previously: don't supported by qemu)
# s390x-linux-gnu-gcc         - works (previously: qemu hang/abort)
# sparc64-linux-gnu-gcc       - coredump (qemu mmap-troubles, previously: qemu fails fcntl for F_SETLK/F_GETLK)
# alpha-linux-gnu-gcc         - coredump (qemu mmap-troubles)
CROSS_LIST_NOQEMU =

cross-gcc:
	@echo '  Re-building by cross-compiler for: $(CROSS_LIST_NOQEMU) $(CROSS_LIST)'
	@echo "CORRESPONDING CROSS-COMPILERs ARE REQUIRED."
	@echo "FOR INSTANCE: apt install g++-aarch64-linux-gnu g++-alpha-linux-gnu g++-arm-linux-gnueabihf g++-hppa-linux-gnu g++-mips-linux-gnu g++-mips64-linux-gnuabi64 g++-powerpc-linux-gnu g++-powerpc64-linux-gnu g++-s390x-linux-gnu g++-sh4-linux-gnu g++-sparc64-linux-gnu"
	$(QUIET)for CC in $(CROSS_LIST_NOQEMU) $(CROSS_LIST); do \
		echo "===================== $$CC"; \
		$(MAKE) CXXSTD= clean && CC=$$CC CXX=$$(echo $$CC | sed 's/-gcc/-g++/') EXE_LDFLAGS=-static $(MAKE) all || exit $$?; \
	done

# Unfortunately qemu don't provide robust support for futexes.
# Therefore it is impossible to run full multi-process tests.
cross-qemu:
	@echo '  Re-building by cross-compiler and re-check by QEMU for: $(CROSS_LIST)'
	@echo "CORRESPONDING CROSS-COMPILERs AND QEMUs ARE REQUIRED."
	@echo "FOR INSTANCE: "
	@echo "	1) apt install g++-aarch64-linux-gnu g++-alpha-linux-gnu g++-arm-linux-gnueabihf g++-hppa-linux-gnu g++-mips-linux-gnu g++-mips64-linux-gnuabi64 g++-powerpc-linux-gnu g++-powerpc64-linux-gnu g++-s390x-linux-gnu g++-sh4-linux-gnu g++-sparc64-linux-gnu"
	@echo "	2) apt install binfmt-support qemu-user-static qemu-user qemu-system-arm qemu-system-mips qemu-system-misc qemu-system-ppc qemu-system-sparc"
	$(QUIET)for CC in $(CROSS_LIST); do \
		echo "===================== $$CC + qemu"; \
		$(MAKE) CXXSTD= clean && \
			MDBX_TEST_EXTRA="$(MDBX_TEST_EXTRA)$$(echo $$CC | grep -q -e alpha -e sparc && echo ' --size-upper=64M --size-lower=64M')" \
			CC=$$CC CXX=$$(echo $$CC | sed 's/-gcc/-g++/') EXE_LDFLAGS=-static MDBX_BUILD_OPTIONS="-DMDBX_SAFE4QEMU $(MDBX_BUILD_OPTIONS)" \
			$(MAKE) test-singleprocess || exit $$?; \
	done

#< dist-cutoff-end

install: $(LIBRARIES) $(TOOLS) $(HEADERS)
	@echo '  INSTALLING...'
	$(INSTALL) -D -p $(EXE_INSTALL_FLAGS) -t $(DESTDIR)$(prefix)/bin$(suffix) $(TOOLS) && \
	$(INSTALL) -D -p $(EXE_INSTALL_FLAGS) -t $(DESTDIR)$(prefix)/lib$(suffix) $(filter-out libmdbx.a,$(LIBRARIES)) && \
	$(INSTALL) -D -p -t $(DESTDIR)$(prefix)/lib$(suffix) libmdbx.a && \
	$(INSTALL) -D -p -m 444 -t $(DESTDIR)$(prefix)/include $(HEADERS) && \
	$(INSTALL) -D -p -m 444 -t $(DESTDIR)$(mandir)/man1 $(addprefix $(MAN_SRCDIR), $(MANPAGES))

install-strip: EXE_INSTALL_FLAGS = -s
install-strip: install

install-no-strip: EXE_INSTALL_FLAGS =
install-no-strip: install

uninstall:
	@echo '  UNINSTALLING/REMOVE...'
	$(QUIET)rm -f $(addprefix $(DESTDIR)$(prefix)/bin$(suffix)/,$(TOOLS)) \
		$(addprefix $(DESTDIR)$(prefix)/lib$(suffix)/,$(LIBRARIES)) \
		$(addprefix $(DESTDIR)$(prefix)/include/,$(HEADERS)) \
		$(addprefix $(DESTDIR)$(mandir)/man1/,$(MANPAGES))

################################################################################
# Benchmarking by ioarena

IOARENA ?= $(shell \
  (test -x ../ioarena/@BUILD/src/ioarena && echo ../ioarena/@BUILD/src/ioarena) || \
  (test -x ../../@BUILD/src/ioarena && echo ../../@BUILD/src/ioarena) || \
  (test -x ../../src/ioarena && echo ../../src/ioarena) || which ioarena 2>&- || \
  echo '\#\# TIP: Clone and build the https://github.com/pmwkaa/ioarena.git within a neighbouring directory for availability of benchmarking.' >&2)
NN	?= 25000000
BENCH_CRUD_MODE ?= nosync

ifneq ($(wildcard $(IOARENA)),)

.PHONY: bench bench-clean bench-couple re-bench bench-quartet bench-triplet

bench-clean:
	@echo '  REMOVE bench-*.txt _ioarena/*'
	$(QUIET)rm -rf bench-*.txt _ioarena/*

re-bench: bench-clean bench

define bench-rule
bench-$(1)_$(2).txt: $(3) $(IOARENA) $(lastword $(MAKEFILE_LIST))
	@echo '  RUNNING ioarena for $1/$2...'
	$(QUIET)LD_LIBRARY_PATH="./:$$$${LD_LIBRARY_PATH}" \
		$(IOARENA) -D $(1) -B crud -m $(BENCH_CRUD_MODE) -n $(2) \
		| tee $$@ | grep throughput && \
	LD_LIBRARY_PATH="./:$$$${LD_LIBRARY_PATH}" \
		$(IOARENA) -D $(1) -B get,iterate -m $(BENCH_CRUD_MODE) -r 4 -n $(2) \
		| tee -a $$@ | grep throughput \
	|| mv -f $$@ $$@.error

endef

$(eval $(call bench-rule,mdbx,$(NN),libmdbx.$(SO_SUFFIX)))

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
bench: bench-mdbx_$(NN).txt
bench-quartet: bench-mdbx_$(NN).txt bench-lmdb_$(NN).txt bench-rocksdb_$(NN).txt bench-wiredtiger_$(NN).txt
bench-triplet: bench-mdbx_$(NN).txt bench-lmdb_$(NN).txt bench-sqlite3_$(NN).txt
bench-couple: bench-mdbx_$(NN).txt bench-lmdb_$(NN).txt

# $(eval $(call bench-rule,debug,10))
# .PHONY: bench-debug
# bench-debug: bench-debug_10.txt

endif
