# This makefile is for GNU Make 3.81 or above, and nowadays provided
# just for compatibility and preservation of traditions.
#
# Please use CMake in case of any difficulties or
# problems with this old-school's magic.
#
################################################################################
#
# Basic internal definitions. For a customizable variables and options see below.
#
$(info // The GNU Make $(MAKE_VERSION))
SHELL         := $(shell env bash -c 'echo $$BASH')
MAKE_VERx3    := $(shell printf "%3s%3s%3s" $(subst ., ,$(MAKE_VERSION)))
make_lt_3_81  := $(shell expr "$(MAKE_VERx3)" "<" "  3 81")
ifneq ($(make_lt_3_81),0)
$(error Please use GNU Make 3.81 or above)
endif
make_ge_4_1   := $(shell expr "$(MAKE_VERx3)" ">=" "  4  1")
make_ge_4_4   := $(shell expr "$(MAKE_VERx3)" ">=" "  4  4")
SRC_PROBE_C   := $(shell [ -f mdbx.c ] && echo mdbx.c || echo src/osal.c)
SRC_PROBE_CXX := $(shell [ -f mdbx.c++ ] && echo mdbx.c++ || echo src/mdbx.c++)
UNAME         := $(shell uname -s 2>/dev/null || echo Unknown)

define cxx_filesystem_probe
  int main(int argc, const char*argv[]) {
    mdbx::filesystem::path probe(argv[0]);
    if (argc != 1) throw mdbx::filesystem::filesystem_error(std::string("fake"), std::error_code());
    return mdbx::filesystem::is_directory(probe.relative_path());
  }
endef
#
################################################################################
#
# Use `make options` to list the available libmdbx build options.
#
# Note that the defaults should already be correct for most platforms;
# you should not need to change any of these. Read their descriptions
# in README and source code (see src/options.h) if you do.
#

# install sandbox
DESTDIR ?=
INSTALL ?= install
# install prefixes (inside sandbox)
prefix  ?= /usr/local
mandir  ?= $(prefix)/man
# lib/bin suffix for multiarch/biarch, e.g. '.x86_64'
suffix  ?=

# toolchain
CC      ?= gcc
CXX     ?= g++
CFLAGS_EXTRA ?=
LD      ?= ld
CMAKE	?= cmake
CMAKE_OPT ?=
CTEST	?= ctest
CTEST_OPT ?=
# target directory for `make dist`
DIST_DIR ?= dist

# build options
MDBX_BUILD_OPTIONS   ?=-DNDEBUG=1
MDBX_BUILD_TIMESTAMP ?=$(if $(SOURCE_DATE_EPOCH),$(SOURCE_DATE_EPOCH),$(shell date +%Y-%m-%dT%H:%M:%S%z))
MDBX_BUILD_CXX       ?=YES
MDBX_BUILD_METADATA  ?=

# probe and compose common compiler flags with variable expansion trick (seems this work two times per session for GNU Make 3.81)
CFLAGS       ?= $(strip $(eval CFLAGS := -std=gnu11 -O2 -g -Wall -Werror -Wextra -Wpedantic -ffunction-sections -fPIC -fvisibility=hidden -pthread -Wno-error=attributes $$(shell for opt in -fno-semantic-interposition -Wno-unused-command-line-argument -Wno-tautological-compare; do [ -z "$$$$($(CC) '-DMDBX_BUILD_FLAGS="probe"' $$$${opt} -c $(SRC_PROBE_C) -o /dev/null >/dev/null 2>&1 || echo failed)" ] && echo "$$$${opt} "; done)$(CFLAGS_EXTRA))$(CFLAGS))

# choosing C++ standard with variable expansion trick (seems this work two times per session for GNU Make 3.81)
CXXSTD       ?= $(eval CXXSTD := $$(shell for std in gnu++23 c++23 gnu++2b c++2b gnu++20 c++20 gnu++2a c++2a gnu++17 c++17 gnu++1z c++1z gnu++14 c++14 gnu++1y c++1y gnu+11 c++11 gnu++0x c++0x; do $(CXX) -std=$$$${std} -DMDBX_BUILD_CXX=1 -c $(SRC_PROBE_CXX) -o /dev/null 2>probe4std-$$$${std}.err >/dev/null && echo "-std=$$$${std}" && exit; done))$(CXXSTD)
CXXFLAGS     ?= $(strip $(CXXSTD) $(filter-out -std=gnu11,$(CFLAGS)))

# libraries and options for linking
EXE_LDFLAGS  ?= -pthread
ifneq ($(make_ge_4_1),1)
# don't use variable expansion trick as workaround for bugs of GNU Make before 4.1
LIBS         ?= $(shell $(uname2libs))
LDFLAGS      ?= $(shell $(uname2ldflags))
LIB_STDCXXFS ?= $(shell echo '$(cxx_filesystem_probe)' | cat mdbx.h++ - | sed $$'1s/\xef\xbb\xbf//' | $(CXX) -x c++ $(CXXFLAGS) -Wno-error - -Wl,--allow-multiple-definition -lstdc++fs $(LIBS) $(LDFLAGS) $(EXE_LDFLAGS) -o /dev/null 2>probe4lstdfs.err >/dev/null && echo '-Wl,--allow-multiple-definition -lstdc++fs')
else
# using variable expansion trick to avoid repeaded probes
LIBS         ?= $(eval LIBS := $$(shell $$(uname2libs)))$(LIBS)
LDFLAGS      ?= $(eval LDFLAGS := $$(shell $$(uname2ldflags)))$(LDFLAGS)
LIB_STDCXXFS ?= $(eval LIB_STDCXXFS := $$(shell echo '$$(cxx_filesystem_probe)' | cat mdbx.h++ - | sed $$$$'1s/\xef\xbb\xbf//' | $(CXX) -x c++ $(CXXFLAGS) -Wno-error - -Wl,--allow-multiple-definition -lstdc++fs $(LIBS) $(LDFLAGS) $(EXE_LDFLAGS) -o /dev/null 2>probe4lstdfs.err >/dev/null && echo '-Wl,--allow-multiple-definition -lstdc++fs'))$(LIB_STDCXXFS)
endif

ifneq ($(make_ge_4_4),1)
.NOTPARALLEL:
WAIT         =
else
WAIT         = .WAIT
endif

################################################################################

define uname2sosuffix
  case "$(UNAME)" in
    Darwin*|Mach*) echo dylib;;
    CYGWIN*|MINGW*|MSYS*|Windows*) echo dll;;
    *) echo so;;
  esac
endef

define uname2ldflags
  case "$(UNAME)" in
    CYGWIN*|MINGW*|MSYS*|Windows*)
      echo '-Wl,--gc-sections,-O1';
      ;;
    *)
      $(LD) --help 2>/dev/null | grep -q -- --gc-sections && echo '-Wl,--gc-sections,-z,relro,-O1';
      $(LD) --help 2>/dev/null | grep -q -- -dead_strip && echo '-Wl,-dead_strip';
      ;;
  esac
endef

# TIP: try add the'-Wl, --no-as-needed,-lrt' for ability to built with modern glibc, but then use with the old.
define uname2libs
  case "$(UNAME)" in
    CYGWIN*|MINGW*|MSYS*|Windows*)
      echo '-lm -lntdll -lwinmm';
      ;;
    *SunOS*|*Solaris*)
      echo '-lm -lkstat -lrt';
      ;;
    *Darwin*|OpenBSD*)
      echo '-lm';
      ;;
    *)
      echo '-lm -lrt';
      ;;
  esac
endef

SO_SUFFIX  := $(shell $(uname2sosuffix))
HEADERS    := mdbx.h mdbx.h++
LIBRARIES  := libmdbx.a libmdbx.$(SO_SUFFIX)
TOOLS      := chk copy drop dump load stat
MDBX_TOOLS := $(addprefix mdbx_,$(TOOLS))
MANPAGES   := mdbx_stat.1 mdbx_copy.1 mdbx_dump.1 mdbx_load.1 mdbx_chk.1 mdbx_drop.1
TIP        := // TIP:

.PHONY: all help options lib libs tools clean install uninstall check_buildflags_tag tools-static
.PHONY: install-strip install-no-strip strip libmdbx mdbx show-options lib-static lib-shared cmake-build ninja

boolean = $(if $(findstring $(strip $($1)),YES Yes yes y ON On on 1 true True TRUE),1,$(if $(findstring $(strip $($1)),NO No no n OFF Off off 0 false False FALSE),,$(error Wrong value `$($1)` of $1 for YES/NO option)))
select_by = $(if $(call boolean,$(1)),$(2),$(3))

ifeq ("$(origin V)", "command line")
  MDBX_BUILD_VERBOSE := $(V)
endif
ifndef MDBX_BUILD_VERBOSE
  MDBX_BUILD_VERBOSE := 0
endif

ifeq ($(call boolean,MDBX_BUILD_VERBOSE),1)
  QUIET :=
  HUSH :=
  $(info $(TIP) Use `make V=0` for quiet.)
else
  QUIET := @
  HUSH := >/dev/null
  $(info $(TIP) Use `make V=1` for verbose.)
endif

ifeq ($(UNAME),Darwin)
  $(info $(TIP) Use `brew install gnu-sed gnu-tar` and add ones to the beginning of the PATH.)
endif

all: show-options $(LIBRARIES) $(MDBX_TOOLS)

help:
	@echo "  make all                 - build libraries and tools"
	@echo "  make help                - print this help"
	@echo "  make options             - list build options"
	@echo "  make lib                 - build libraries, also lib-static and lib-shared"
	@echo "  make tools               - build the tools"
	@echo "  make tools-static        - build the tools with statically linking with system libraries and compiler runtime"
	@echo "  make clean               "
	@echo "  make install             "
	@echo "  make uninstall           "
	@echo "  make cmake-build | ninja - build by CMake & Ninja"
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
	@echo "  make check               - smoke test with amalgamation and installation checking"
	@echo "  make smoke               - fast smoke test"
	@echo "  make smoke-memcheck      - build with Valgrind support and run smoke test under memcheck tool"
	@echo "  make smoke-fault         - execute transaction owner failure smoke testcase"
	@echo "  make smoke-singleprocess - execute single-process smoke test"
	@echo "  make test                - basic test"
	@echo "  make test-memcheck       - build with Valgrind support and run basic test under memcheck tool"
	@echo "  make test-long           - execute long test which runs for several weeks, or until interruption"
	@echo "  make test-asan           - build with AddressSanitizer and run basic test"
	@echo "  make test-leak           - build with LeakSanitizer and run basic test"
	@echo "  make test-ubsan          - build with UndefinedBehaviourSanitizer and run basic test"
	@echo "  make test-singleprocess  - execute single-process basic test (also used by make cross-qemu)"
	@echo "  make cross-gcc           - check cross-compilation without test execution"
	@echo "  make cross-qemu          - run cross-compilation and execution basic test with QEMU"
	@echo "  make gcc-analyzer        - run gcc-analyzer (mostly useless for now)"
	@echo "  make build-test          - build test executable(s)"
	@echo ""
	@echo "  make dist                - build amalgamated source code"
	@echo "  make doxygen             - build HTML documentation"
	@echo "  make release-assets      - build release assets"
	@echo "  make reformat            - reformat source code with clang-format"
#< dist-cutoff-end

show-options:
	@echo "  MDBX_BUILD_OPTIONS   = $(MDBX_BUILD_OPTIONS)"
	@echo "  MDBX_BUILD_CXX       = $(MDBX_BUILD_CXX)"
	@echo "  MDBX_BUILD_TIMESTAMP = $(MDBX_BUILD_TIMESTAMP)"
	@echo "  MDBX_BUILD_METADATA  = $(MDBX_BUILD_METADATA)"
	@echo '$(TIP) Use `make options` to listing available build options.'
	@echo $(call select_by,MDBX_BUILD_CXX,"  CXX      =`which $(CXX)` | `$(CXX) --version | head -1`","  CC       =`which $(CC)` | `$(CC) --version | head -1`")
	@echo $(call select_by,MDBX_BUILD_CXX,"  CXXFLAGS =$(CXXFLAGS)","  CFLAGS   =$(CFLAGS)")
	@echo $(call select_by,MDBX_BUILD_CXX,"  LDFLAGS  =$(LDFLAGS) $(LIB_STDCXXFS) $(LIBS) $(EXE_LDFLAGS)","  LDFLAGS  =$(LDFLAGS) $(LIBS) $(EXE_LDFLAGS)")
	@echo '$(TIP) Use `make help` to listing available targets.'

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
	@echo "  MDBX_BUILD_OPTIONS   = $(MDBX_BUILD_OPTIONS)"
	@echo "  MDBX_BUILD_TIMESTAMP = $(MDBX_BUILD_TIMESTAMP)"
	@echo "  MDBX_BUILD_METADATA  = $(MDBX_BUILD_METADATA)"
	@echo ""
	@echo "## Assortment items for MDBX_BUILD_OPTIONS:"
	@echo "##   Note that the defaults should already be correct for most platforms;"
	@echo "##   you should not need to change any of these. Read their descriptions"
#> dist-cutoff-begin
ifeq ($(wildcard mdbx.c),mdbx.c)
#< dist-cutoff-end
	@echo "##   in README and source code (see mdbx.c) if you do."
	@grep -h '#ifndef MDBX_' mdbx.c | grep -v BUILD | sort -u | sed 's/#ifndef /  /'
#> dist-cutoff-begin
else
	@echo "##   in README and source code (see src/options.h) if you do."
	@grep -h '#ifndef MDBX_' src/*.h | grep -v BUILD | sort -u | sed 's/#ifndef /  /'
endif
#< dist-cutoff-end

lib libs libmdbx mdbx: libmdbx.a libmdbx.$(SO_SUFFIX)

tools: $(MDBX_TOOLS)
tools-static: $(addsuffix .static,$(MDBX_TOOLS)) $(addsuffix .static-lto,$(MDBX_TOOLS))

strip: all
	@echo '  STRIP libmdbx.$(SO_SUFFIX) $(MDBX_TOOLS)'
	$(TRACE )strip libmdbx.$(SO_SUFFIX) $(MDBX_TOOLS)

clean:
	@echo '  REMOVE ...'
	$(QUIET)rm -rf $(MDBX_TOOLS) mdbx_test @* *.[ao] *.[ls]o *.$(SO_SUFFIX) *.dSYM *~ tmp.db/* \
		*.gcov *.log *.err src/*.o test/*.o mdbx_example dist @dist-check \
		config.h src/config.h src/version.c *.tar* @buildflags.tag @dist-checked.tag \
		mdbx_*.static mdbx_*.static-lto CMakeFiles

MDBX_BUILD_FLAGS =$(strip MDBX_BUILD_CXX=$(MDBX_BUILD_CXX) $(MDBX_BUILD_OPTIONS) $(call select_by,MDBX_BUILD_CXX,$(CXXFLAGS) $(LDFLAGS) $(LIB_STDCXXFS) $(LIBS),$(CFLAGS) $(LDFLAGS) $(LIBS)))
check_buildflags_tag:
	$(QUIET)if [ "$(MDBX_BUILD_FLAGS)" != "$$(cat @buildflags.tag 2>&1)" ]; then \
		echo -n "  CLEAN for build with specified flags..." && \
		$(MAKE) IOARENA=false CXXSTD= -s clean >/dev/null && echo " Ok" && \
		echo '$(MDBX_BUILD_FLAGS)' > @buildflags.tag; \
	fi

@buildflags.tag: check_buildflags_tag

lib-static libmdbx.a: mdbx-static.o $(call select_by,MDBX_BUILD_CXX,mdbx++-static.o)
	@echo '  AR $@'
	$(QUIET)$(AR) rcs $@ $? $(HUSH)

lib-shared libmdbx.$(SO_SUFFIX): mdbx-dylib.o $(call select_by,MDBX_BUILD_CXX,mdbx++-dylib.o)
	@echo '  LD $@'
	$(QUIET)$(call select_by,MDBX_BUILD_CXX,$(CXX) $(CXXFLAGS),$(CC) $(CFLAGS)) $^ -pthread -shared $(LDFLAGS) $(call select_by,MDBX_BUILD_CXX,$(LIB_STDCXXFS)) $(LIBS) -o $@

ninja: cmake-build
cmake-build:
	@echo "  RUN: cmake -G Ninja && cmake --build"
	$(QUIET)mkdir -p @cmake-ninja-build && $(CMAKE) $(CMAKE_OPT) -G Ninja -S . -B @cmake-ninja-build && $(CMAKE) --build @cmake-ninja-build

ctest: cmake-build
	@echo "  RUN: ctest .."
	$(QUIET)$(CTEST) --test-dir @cmake-ninja-build --parallel `(nproc | sysctl -n hw.ncpu | echo 2) 2>/dev/null` --schedule-random $(CTEST_OPT)

#> dist-cutoff-begin
ifeq ($(wildcard mdbx.c),mdbx.c)
#< dist-cutoff-end

################################################################################
# Amalgamated source code, i.e. distributed after `make dist`
MAN_SRCDIR := man1/

config.h: @buildflags.tag $(WAIT) mdbx.c $(lastword $(MAKEFILE_LIST)) LICENSE NOTICE
	@echo '  MAKE $@'
	$(QUIET)(echo '#define MDBX_BUILD_TIMESTAMP "$(MDBX_BUILD_TIMESTAMP)"' \
	&& echo "#define MDBX_BUILD_FLAGS \"$$(cat @buildflags.tag)\"" \
	&& echo '#define MDBX_BUILD_COMPILER "$(shell (LC_ALL=C $(CC) --version || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	&& echo '#define MDBX_BUILD_TARGET "$(shell set -o pipefail; (LC_ALL=C $(CC) -v 2>&1 | grep -i '^Target:' | cut -d ' ' -f 2- || (LC_ALL=C $(CC) --version | grep -qi e2k && echo E2K) || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	&& echo '#define MDBX_BUILD_CXX $(call select_by,MDBX_BUILD_CXX,1,0)' \
	&& echo '#define MDBX_BUILD_METADATA "$(MDBX_BUILD_METADATA)"' \
	) >$@

mdbx-dylib.o: config.h mdbx.c mdbx.h $(lastword $(MAKEFILE_LIST)) LICENSE NOTICE
	@echo '  CC $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c mdbx.c -o $@

mdbx-static.o: config.h mdbx.c mdbx.h $(lastword $(MAKEFILE_LIST)) LICENSE NOTICE
	@echo '  CC $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c mdbx.c -o $@

mdbx++-dylib.o: config.h mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST)) LICENSE NOTICE
	@echo '  CC $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c mdbx.c++ -o $@

mdbx++-static.o: config.h mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST)) LICENSE NOTICE
	@echo '  CC $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c mdbx.c++ -o $@

mdbx_%:	mdbx_%.c mdbx-static.o
	@echo '  CC+LD $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' $^ $(EXE_LDFLAGS) $(LIBS) -o $@

mdbx_%.static: mdbx_%.c mdbx-static.o
	@echo '  CC+LD $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' $^ $(EXE_LDFLAGS) -static -Wl,--strip-all -o $@

mdbx_%.static-lto: mdbx_%.c config.h mdbx.c mdbx.h
	@echo '  CC+LD $@'
	$(QUIET)$(CC) $(CFLAGS) -Os -flto $(MDBX_BUILD_OPTIONS) '-DLIBMDBX_API=' '-DMDBX_CONFIG_H="config.h"' \
		$< mdbx.c $(EXE_LDFLAGS) $(LIBS) -static -Wl,--strip-all -o $@

#> dist-cutoff-begin
else
################################################################################
# Plain (non-amalgamated) sources with test

.PHONY: build-test build-test-with-valgrind check cross-gcc cross-qemu dist doxygen gcc-analyzer long-test
.PHONY: reformat release-assets tags smoke test test-asan smoke-fault test-leak
.PHONY: smoke-singleprocess test-singleprocess test-ubsan test-valgrind test-memcheck memcheck smoke-memcheck
.PHONY: smoke-assertion test-assertion long-test-assertion test-ci test-ci-extra

test-ci-extra: test-ci cross-gcc cross-qemu

test-ci: check \
	smoke-singleprocess smoke-fault smoke-memcheck smoke \
	test-leak test-asan test-ubsan test-singleprocess test test-memcheck

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

DIST_EXTRA := LICENSE NOTICE README.md CMakeLists.txt GNUmakefile Makefile ChangeLog.md VERSION.json config.h.in ntdll.def \
	$(addprefix man1/, $(MANPAGES)) cmake/compiler.cmake cmake/profile.cmake cmake/utils.cmake .clang-format-ignore
DIST_SRC   := mdbx.h mdbx.h++ mdbx.c mdbx.c++ $(addsuffix .c, $(MDBX_TOOLS))

TEST_DB    ?= $(shell [ -d /dev/shm ] && echo /dev/shm || echo /tmp)/mdbx-test.db
TEST_LOG   ?= $(shell [ -d /dev/shm ] && echo /dev/shm || echo /tmp)/mdbx-test.log
TEST_OSAL  := $(shell $(uname2osal))
TEST_ITER  := $(shell $(uname2titer))
TEST_SRC   := test/osal-$(TEST_OSAL).c++ $(filter-out $(wildcard test/osal-*.c++),$(wildcard test/*.c++)) $(call select_by,MDBX_BUILD_CXX,,src/mdbx.c++)
TEST_INC   := $(wildcard test/*.h++)
TEST_OBJ   := $(patsubst %.c++,%.o,$(TEST_SRC))
ifndef SED
SED        := $(shell which gnu-sed 2>&- || echo sed)
endif
TAR        ?= $(shell which gnu-tar 2>&- || echo tar)
ZIP        ?= $(shell which zip || echo "echo 'Please install zip'")
CLANG_FORMAT ?= $(shell (which clang-format-19 || which clang-format) 2>/dev/null)

reformat:
	@echo '  RUNNING clang-format...'
	$(QUIET)if [ -n "$(CLANG_FORMAT)" ]; then \
		git ls-files | grep -E '\.(c|c++|h|h++)(\.in)?$$' | xargs -r $(CLANG_FORMAT) -i --style=file; \
	else \
		echo "clang-format version 19 not found for 'reformat'"; \
	fi

MAN_SRCDIR := src/man1/
ALLOY_DEPS := $(shell git ls-files src/ | grep -e /tools -e /man -v)
MDBX_GIT_DIR := $(shell if [ -d .git ]; then echo .git; elif [ -s .git -a -f .git ]; then grep '^gitdir: ' .git | cut -d ':' -f 2; else echo git_directory_is_absent; fi)
MDBX_GIT_LASTVTAG := $(shell git describe --tags --dirty=-DIRTY --abbrev=0 '--match=v[0-9]*' 2>&- || echo 'Please fetch tags and/or install non-obsolete git version')
MDBX_GIT_3DOT := $(shell set -o pipefail; echo "$(MDBX_GIT_LASTVTAG)" | $(SED) -n 's|^v*\([0-9]\{1,\}\.[0-9]\{1,\}\.[0-9]\{1,\}\)\(.*\)|\1|p' || echo 'Please fetch tags and/or use non-obsolete git version')
MDBX_GIT_TWEAK := $(shell set -o pipefail; git rev-list $(shell git describe --tags --abbrev=0 '--match=v[0-9]*')..HEAD --count 2>&- || echo 'Please fetch tags and/or use non-obsolete git version')
MDBX_GIT_TIMESTAMP := $(shell git show --no-patch --format=%cI HEAD 2>&- || echo 'Please install latest get version')
MDBX_GIT_DESCRIBE := $(shell git describe --tags --long --dirty '--match=v[0-9]*' 2>&- || echo 'Please fetch tags and/or install non-obsolete git version')
MDBX_GIT_PRERELEASE := $(shell echo "$(MDBX_GIT_LASTVTAG)" | $(SED) -n 's|^v*\([0-9]\{1,\}\.[0-9]\{1,\}\.[0-9]\{1,\}\)\(.*\)-\([-.0-1a-zA-Z]\+\)|\3|p')
MDBX_VERSION_PURE = $(MDBX_GIT_3DOT)$(if $(filter-out 0,$(MDBX_GIT_TWEAK)),.$(MDBX_GIT_TWEAK),)$(if $(MDBX_GIT_PRERELEASE),-$(MDBX_GIT_PRERELEASE),)
MDBX_VERSION_IDENT = $(shell set -o pipefail; echo -n '$(MDBX_GIT_DESCRIBE)' | tr -c -s '[a-zA-Z0-9.]' _)
MDBX_VERSION_NODOT = $(subst .,_,$(MDBX_VERSION_IDENT))
MDBX_BUILD_SOURCERY = $(shell set -o pipefail; $(MAKE) IOARENA=false CXXSTD= -s src/version.c >/dev/null && (openssl dgst -r -sha256 src/version.c || sha256sum src/version.c || shasum -a 256 src/version.c) 2>/dev/null | cut -d ' ' -f 1 || (echo 'Please install openssl or sha256sum or shasum' >&2 && echo sha256sum_is_no_available))_$(MDBX_VERSION_NODOT)
MDBX_DIST_DIR = libmdbx-$(MDBX_VERSION_NODOT)

# Extra options mdbx_test utility
MDBX_SMOKE_EXTRA ?=

check: DESTDIR = $(shell pwd)/@check-install
check: CMAKE_OPT = -Werror=dev
check: smoke-assertion ninja dist install test ctest

smoke-assertion: MDBX_BUILD_OPTIONS:=$(strip $(MDBX_BUILD_OPTIONS) -DMDBX_FORCE_ASSERTIONS=1 -UNDEBUG -DMDBX_DEBUG=0)
smoke-assertion: smoke
test-assertion: MDBX_BUILD_OPTIONS:=$(strip $(MDBX_BUILD_OPTIONS) -DMDBX_FORCE_ASSERTIONS=1 -UNDEBUG -DMDBX_DEBUG=0)
test-assertion: smoke
long-test-assertion: MDBX_BUILD_OPTIONS:=$(strip $(MDBX_BUILD_OPTIONS) -DMDBX_FORCE_ASSERTIONS=1 -UNDEBUG -DMDBX_DEBUG=0)
long-test-assertion: smoke

smoke: build-test
	@echo '  SMOKE `mdbx_test basic`...'
	$(QUIET)rm -f $(TEST_DB) $(TEST_LOG).gz && (set -o pipefail; \
		(./mdbx_test --table=+data.integer --keygen.split=29 --datalen.min=min --datalen.max=max --progress --console=no --repeat=$(TEST_ITER) --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_SMOKE_EXTRA) basic && \
		./mdbx_test --mode=-writemap,-nosync-safe,-lifo --progress --console=no --repeat=$(TEST_ITER) --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_SMOKE_EXTRA) basic) \
		| tee >(gzip --stdout >$(TEST_LOG).gz) | tail -n 42) \
	&& ./mdbx_chk -vvn $(TEST_DB) && ./mdbx_chk -vvn $(TEST_DB)-copy

smoke-singleprocess: build-test
	@echo '  SMOKE `mdbx_test --nested`...'
	$(QUIET)rm -f $(TEST_DB) $(TEST_LOG).gz && (set -o pipefail; \
		(./mdbx_test --table=+data.integer --keygen.split=29 --datalen.min=min --datalen.max=max --progress --console=no --repeat=42 --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_SMOKE_EXTRA) --hill && \
		./mdbx_test --progress --console=no --repeat=2 --pathname=$(TEST_DB) --dont-cleanup-before --dont-cleanup-after --copy && \
		./mdbx_test --mode=-writemap,-nosync-safe,-lifo --progress --console=no --repeat=42 --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_SMOKE_EXTRA) --nested) \
		| tee >(gzip --stdout >$(TEST_LOG).gz) | tail -n 42) \
	&& ./mdbx_chk -vvn $(TEST_DB) && ./mdbx_chk -vvn $(TEST_DB)-copy

smoke-fault: build-test
	@echo '  SMOKE `mdbx_test --inject-writefault=42 basic`...'
	$(QUIET)rm -f $(TEST_DB) $(TEST_LOG).gz && (set -o pipefail; ./mdbx_test --progress --console=no --pathname=$(TEST_DB) --inject-writefault=42 --dump-config --dont-cleanup-after $(MDBX_SMOKE_EXTRA) basic \
		| tee >(gzip --stdout >$(TEST_LOG).gz) | tail -n 42) \
	; ./mdbx_chk -vvnw $(TEST_DB) && ([ ! -e $(TEST_DB)-copy ] || ./mdbx_chk -vvn $(TEST_DB)-copy)

test: build-test
	@echo '  RUNNING `test/stochastic.sh --loops 2`...'
	$(QUIET)test/stochastic.sh --dont-check-ram-size --loops 2 --db-upto-mb 256 --skip-make --taillog >$(TEST_LOG) || (cat $(TEST_LOG) && false)

long-test: test-long
test-long: build-test
	@echo '  RUNNING `test/stochastic.sh --loops 42`...'
	$(QUIET)test/stochastic.sh --loops 42 --db-upto-mb 1024 --extra --skip-make --taillog

test-singleprocess: build-test
	@echo '  RUNNING `test/stochastic.sh --single --loops 2`...'
	$(QUIET)test/stochastic.sh --dont-check-ram-size --single --loops 2 --db-upto-mb 256 --skip-make --taillog >$(TEST_LOG) || (cat $(TEST_LOG) && false)

test-valgrind: test-memcheck
test-memcheck: CFLAGS_EXTRA=-Ofast -DENABLE_MEMCHECK
test-memcheck: build-test
	@echo '  RUNNING `test/stochastic.sh --with-valgrind --loops 2`...'
	$(QUIET)test/stochastic.sh --with-valgrind --loops 2 --db-upto-mb 256 --skip-make >$(TEST_LOG) || (cat $(TEST_LOG) && false)

memcheck: smoke-memcheck
smoke-memcheck: VALGRIND=valgrind --trace-children=yes --log-file=valgrind-%p.log --leak-check=full --track-origins=yes --read-var-info=yes --error-exitcode=42 --suppressions=test/valgrind_suppress.txt
smoke-memcheck: CFLAGS_EXTRA=-Ofast -DENABLE_MEMCHECK
smoke-memcheck: build-test
	@echo "  SMOKE \`mdbx_test basic\` under Valgrind's memcheck..."
	$(QUIET)rm -f valgrind-*.log $(TEST_DB) $(TEST_LOG).gz && (set -o pipefail; ( \
		$(VALGRIND) ./mdbx_test --table=+data.fixed --keygen.split=29 --datalen=35 --progress --console=no --repeat=2 --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_SMOKE_EXTRA) basic && \
		$(VALGRIND) ./mdbx_test --progress --console=no --pathname=$(TEST_DB) --dont-cleanup-before --dont-cleanup-after --copy && \
		$(VALGRIND) ./mdbx_test --mode=-writemap,-nosync-safe,-lifo --progress --console=no --repeat=4 --pathname=$(TEST_DB) --dont-cleanup-after $(MDBX_SMOKE_EXTRA) basic && \
		$(VALGRIND) ./mdbx_chk -vvn $(TEST_DB) && \
		$(VALGRIND) ./mdbx_chk -vvn $(TEST_DB)-copy \
	) | tee >(gzip --stdout >$(TEST_LOG).gz) | tail -n 42)

gcc-analyzer:
	@echo '  RE-BUILD with `-fanalyzer` option...'
	@echo "NOTE: There a lot of false-positive warnings at 2020-05-01 by pre-release GCC-10 (20200328, Red Hat 10.0.1-0.11)"
	$(QUIET)$(MAKE) IOARENA=false CXXSTD=$(CXXSTD) CFLAGS_EXTRA="-Og -fanalyzer -Wno-error" build-test

test-ubsan:
	@echo '  RE-TEST with `-fsanitize=undefined` option...'
	$(QUIET)$(MAKE) IOARENA=false CXXSTD=$(CXXSTD) CFLAGS_EXTRA="-DENABLE_UBSAN -Ofast -fsanitize=undefined -fsanitize-undefined-trap-on-error" test

test-asan:
	@echo '  RE-TEST with `-fsanitize=address` option...'
	$(QUIET)$(MAKE) IOARENA=false CXXSTD=$(CXXSTD) CFLAGS_EXTRA="-Os -fsanitize=address" test

test-leak:
	@echo '  RE-TEST with `-fsanitize=leak` option...'
	$(QUIET)$(MAKE) IOARENA=false CXXSTD=$(CXXSTD) CFLAGS_EXTRA="-fsanitize=leak" test

mdbx_example: mdbx.h example/example-mdbx.c libmdbx.$(SO_SUFFIX)
	@echo '  CC+LD $@'
	$(QUIET)$(CC) $(CFLAGS) -I. example/example-mdbx.c ./libmdbx.$(SO_SUFFIX) -o $@

build-test: all mdbx_example mdbx_test

define test-rule
$(patsubst %.c++,%.o,$(1)): $(1) $(TEST_INC) $(HEADERS) $(lastword $(MAKEFILE_LIST))
	@echo '  CC $$@'
	$(QUIET)$$(CXX) $$(CXXFLAGS) $$(MDBX_BUILD_OPTIONS) -DMDBX_BUILD_CXX=1 -DMDBX_WITHOUT_MSVC_CRT=0 -c $(1) -o $$@

endef
$(foreach file,$(TEST_SRC),$(eval $(call test-rule,$(file))))

define tool-rule
mdbx_$(1):	src/tools/$(1).c libmdbx.a
	@echo '  CC+LD $$@'
	$(QUIET)$$(CC) $$(CFLAGS) $$(MDBX_BUILD_OPTIONS) -Isrc '-DMDBX_CONFIG_H="config.h"' $$^ $$(EXE_LDFLAGS) $$(LIBS) -o $$@

mdbx_$(1).static:	src/tools/$(1).c mdbx-static.o
	@echo '  CC+LD $$@'
	$(QUIET)$$(CC) $$(CFLAGS) $$(MDBX_BUILD_OPTIONS) -Isrc '-DMDBX_CONFIG_H="config.h"' $$^ $$(EXE_LDFLAGS) $$(LIBS) -static -Wl,--strip-all -o $$@

mdbx_$(1).static-lto: src/tools/$(1).c src/config.h src/version.c src/alloy.c $(ALLOY_DEPS)
	@echo '  CC+LD $$@'
	$(QUIET)$$(CC) $$(CFLAGS) -Os -flto $$(MDBX_BUILD_OPTIONS) -Isrc '-DLIBMDBX_API=' '-DMDBX_CONFIG_H="config.h"' \
		$$< src/alloy.c $$(EXE_LDFLAGS) $$(LIBS) -static -Wl,--strip-all -o $$@

endef
$(foreach file,$(TOOLS),$(eval $(call tool-rule,$(file))))

mdbx_test: $(TEST_OBJ) libmdbx.$(SO_SUFFIX)
	@echo '  LD $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(TEST_OBJ) -Wl,-rpath . -L . -l mdbx $(EXE_LDFLAGS) $(LIBS) -o $@

$(MDBX_GIT_DIR)/HEAD $(MDBX_GIT_DIR)/index $(MDBX_GIT_DIR)/refs/tags:
	@echo '*** ' >&2
	@echo '*** Please don''t use tarballs nor zips which are automatically provided by Github !' >&2
	@echo '*** These archives do not contain version information and thus are unfit to build libmdbx.' >&2
	@echo '*** ' >&2
	@echo '*** Instead just follow the https://libmdbx.dqdkfa.ru/usage.html' >&2
	@echo '*** PLEASE, AVOID USING ANY OTHER TECHNIQUES.' >&2
	@echo '*** ' >&2
	@false

src/version.c: src/version.c.in $(lastword $(MAKEFILE_LIST)) $(MDBX_GIT_DIR)/HEAD $(MDBX_GIT_DIR)/index $(MDBX_GIT_DIR)/refs/tags LICENSE NOTICE
	@echo '  MAKE $@'
	$(QUIET)$(SED) \
		-e "s|@MDBX_GIT_TIMESTAMP@|$(MDBX_GIT_TIMESTAMP)|" \
		-e "s|@MDBX_GIT_TREE@|$(shell git show --no-patch --format=%T HEAD || echo 'Please install latest get version')|" \
		-e "s|@MDBX_GIT_COMMIT@|$(shell git show --no-patch --format=%H HEAD || echo 'Please install latest get version')|" \
		-e "s|@MDBX_GIT_DESCRIBE@|$(MDBX_GIT_DESCRIBE)|" \
		-e "s|\$${MDBX_VERSION_MAJOR}|$(shell echo '$(MDBX_GIT_3DOT)' | cut -d . -f 1)|" \
		-e "s|\$${MDBX_VERSION_MINOR}|$(shell echo '$(MDBX_GIT_3DOT)' | cut -d . -f 2)|" \
		-e "s|\$${MDBX_VERSION_PATCH}|$(shell echo '$(MDBX_GIT_3DOT)' | cut -d . -f 3)|" \
		-e "s|\$${MDBX_VERSION_TWEAK}|$(MDBX_GIT_TWEAK)|" \
		-e "s|@MDBX_VERSION_PRERELEASE@|$(MDBX_GIT_PRERELEASE)|" \
		-e "s|@MDBX_VERSION_PURE@|$(MDBX_VERSION_PURE)|" \
	src/version.c.in >$@

src/config.h: @buildflags.tag $(WAIT) src/version.c $(lastword $(MAKEFILE_LIST)) LICENSE NOTICE
	@echo '  MAKE $@'
	$(QUIET)(echo '#define MDBX_BUILD_TIMESTAMP "$(MDBX_BUILD_TIMESTAMP)"' \
	&& echo "#define MDBX_BUILD_FLAGS \"$$(cat @buildflags.tag)\"" \
	&& echo '#define MDBX_BUILD_COMPILER "$(shell (LC_ALL=C $(CC) --version || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	&& echo '#define MDBX_BUILD_TARGET "$(shell set -o pipefail; (LC_ALL=C $(CC) -v 2>&1 | grep -i '^Target:' | cut -d ' ' -f 2- || (LC_ALL=C $(CC) --version | grep -qi e2k && echo E2K) || echo 'Please use GCC or CLANG compatible compiler') | head -1)"' \
	&& echo '#define MDBX_BUILD_SOURCERY $(MDBX_BUILD_SOURCERY)' \
	&& echo '#define MDBX_BUILD_CXX $(call select_by,MDBX_BUILD_CXX,1,0)' \
	&& echo '#define MDBX_BUILD_METADATA "$(MDBX_BUILD_METADATA)"' \
	) >$@

mdbx-dylib.o: src/config.h src/version.c src/alloy.c $(ALLOY_DEPS) $(lastword $(MAKEFILE_LIST)) LICENSE NOTICE
	@echo '  CC $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c src/alloy.c -o $@

mdbx-static.o: src/config.h src/version.c src/alloy.c $(ALLOY_DEPS) $(lastword $(MAKEFILE_LIST)) LICENSE NOTICE
	@echo '  CC $@'
	$(QUIET)$(CC) $(CFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c src/alloy.c -o $@

docs/Doxyfile: docs/Doxyfile.in src/version.c $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)$(SED) \
		-e "s|@MDBX_GIT_TIMESTAMP@|$(MDBX_GIT_TIMESTAMP)|" \
		-e "s|@MDBX_GIT_TREE@|$(shell git show --no-patch --format=%T HEAD || echo 'Please install latest get version')|" \
		-e "s|@MDBX_GIT_COMMIT@|$(shell git show --no-patch --format=%H HEAD || echo 'Please install latest get version')|" \
		-e "s|@MDBX_GIT_DESCRIBE@|$(MDBX_GIT_DESCRIBE)|" \
		-e "s|\$${MDBX_VERSION_MAJOR}|$(shell echo '$(MDBX_GIT_3DOT)' | cut -d . -f 1)|" \
		-e "s|\$${MDBX_VERSION_MINOR}|$(shell echo '$(MDBX_GIT_3DOT)' | cut -d . -f 2)|" \
		-e "s|\$${MDBX_VERSION_PATCH}|$(shell echo '$(MDBX_GIT_3DOT)' | cut -d . -f 3)|" \
		-e "s|\$${MDBX_VERSION_TWEAK}|$(MDBX_GIT_TWEAK)|" \
		-e "s|@MDBX_VERSION_PRERELEASE@|$(MDBX_GIT_PRERELEASE)|" \
		-e "s|@MDBX_VERSION_PURE@|$(MDBX_VERSION_PURE)|" \
	docs/Doxyfile.in >$@

define md-extract-section
docs/__$(1).md: $(2) $(lastword $(MAKEFILE_LIST))
	@echo '  EXTRACT $1'
	$(QUIET)$(SED) -n '/<!-- section-begin $(1) -->/,/<!-- section-end -->/p' $(2) >$$@ && test -s $$@

endef
$(foreach section,overview mithril characteristics improvements history usage performance bindings,$(eval $(call md-extract-section,$(section),README.md)))

docs/contrib.fame: src/version.c $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)echo "" > $@ && git fame --show-email --format=md --silent-progress -w -M -C | grep '^|' >> $@

docs/overall.md: docs/__overview.md docs/_toc.md docs/__mithril.md docs/__history.md COPYRIGHT LICENSE NOTICE $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)echo -e "\\mainpage Overall\n\\section brief Brief" | cat - $(filter %.md, $^) >$@ && echo -e "\n\n\nLicense\n=======\n" | cat - LICENSE >>$@

docs/intro.md: docs/_preface.md docs/__characteristics.md docs/__improvements.md docs/_restrictions.md docs/__performance.md
	@echo '  MAKE $@'
	$(QUIET)cat $^ | $(SED) 's/^Performance comparison$$/Performance comparison {#performance}/;s/^Improvements beyond LMDB$$/Improvements beyond LMDB {#improvements}/' >$@

docs/usage.md: docs/__usage.md docs/_starting.md docs/__bindings.md
	@echo '  MAKE $@'
	$(QUIET)echo -e "\\page usage Usage\n\\section getting Building & Embedding" | cat - $^ | $(SED) 's/^Bindings$$/Bindings {#bindings}/' >$@

doxygen: docs/Doxyfile docs/overall.md docs/intro.md docs/usage.md mdbx.h mdbx.h++ src/options.h ChangeLog.md COPYRIGHT LICENSE NOTICE docs/favicon.ico docs/manifest.webmanifest docs/ld+json $(lastword $(MAKEFILE_LIST))
	@echo '  RUNNING doxygen...'
	$(QUIET)rm -rf docs/html && \
	cat mdbx.h | tr '\n' '\r' | $(SED) -e 's/LIBMDBX_INLINE_API\s*(\s*\([^,]\+\),\s*\([^,]\+\),\s*(\s*\([^)]\+\)\s*)\s*)\s*{/inline \1 \2(\3) {/g' | tr '\r' '\n' >docs/mdbx.h && \
	cp mdbx.h++ src/options.h ChangeLog.md docs/ && (cd docs && doxygen Doxyfile $(HUSH)) && cp COPYRIGHT LICENSE NOTICE docs/favicon.ico docs/manifest.webmanifest docs/html/ && \
	$(SED) -i docs/html/index.html -e '/\/MathJax.js"><\/script>/r docs/ld+json'

mdbx++-dylib.o: src/config.h src/mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -DLIBMDBX_EXPORTS=1 -c src/mdbx.c++ -o $@

mdbx++-static.o: src/config.h src/mdbx.c++ mdbx.h mdbx.h++ $(lastword $(MAKEFILE_LIST))
	@echo '  CC $@'
	$(QUIET)$(CXX) $(CXXFLAGS) $(MDBX_BUILD_OPTIONS) '-DMDBX_CONFIG_H="config.h"' -ULIBMDBX_EXPORTS -c src/mdbx.c++ -o $@

dist: tags $(WAIT) @dist-checked.tag libmdbx-sources-$(MDBX_VERSION_IDENT).tar.gz $(lastword $(MAKEFILE_LIST))
	@echo '  AMALGAMATION is done'

tags:
	@echo '  FETCH git tags...'
	$(QUIET)git fetch --tags --force

release-assets: libmdbx-amalgamated-$(MDBX_GIT_3DOT).zpaq \
	libmdbx-amalgamated-$(MDBX_GIT_3DOT).tar.xz \
	libmdbx-amalgamated-$(MDBX_GIT_3DOT).tar.bz2 \
	libmdbx-amalgamated-$(MDBX_GIT_3DOT).tar.gz \
	libmdbx-amalgamated-$(subst .,_,$(MDBX_GIT_3DOT)).zip
	$(QUIET)([ \
		"$$(set -o pipefail; git describe | $(SED) -n '/^v[0-9]\{1,\}\.[0-9]\{1,\}\.[0-9]\{1,\}$$/p' || echo fail-left)" \
	== \
		"$$(git describe --tags --dirty=-dirty || echo fail-right)" ] \
		|| (echo 'ERROR: Is not a valid release because not in the clean state with a suitable annotated tag!!!' >&2 && false)) \
	&& echo '  RELEASE ASSETS are done'

@dist-checked.tag: $(addprefix $(DIST_DIR)/, $(DIST_SRC) $(DIST_EXTRA))
	@echo -n '  VERIFY amalgamated sources...'
	$(QUIET)rm -rf $@ $(DIST_DIR)/@tmp-essentials.inc $(DIST_DIR)/@tmp-internals.inc \
	&& if grep -R "define xMDBX_ALLOY" dist | grep -q MDBX_BUILD_SOURCERY; then echo "sed output is WRONG!" >&2; exit 2; fi \
	&& rm -rf @dist-check && cp -r -p $(DIST_DIR) @dist-check && ($(MAKE) -j IOARENA=false CXXSTD=$(CXXSTD) -C @dist-check all ninja >@dist-check.log 2>@dist-check.err || (cat @dist-check.err && exit 1)) \
	&& touch $@ || (echo " FAILED! See @dist-check.log and @dist-check.err" >&2; exit 2) && echo " Ok"

%.tar.gz: @dist-checked.tag
	@echo '  CREATE $@'
	$(QUIET)$(TAR) -c $(shell LC_ALL=C $(TAR) --help | grep -q -- '--owner' && echo '--owner=0 --group=0') -f - -C dist $(DIST_SRC) $(DIST_EXTRA) | gzip -c -9 >$@

%.tar.xz: @dist-checked.tag
	@echo '  CREATE $@'
	$(QUIET)$(TAR) -c $(shell LC_ALL=C $(TAR) --help | grep -q -- '--owner' && echo '--owner=0 --group=0') -f - -C dist $(DIST_SRC) $(DIST_EXTRA) | xz -9 -z >$@

%.tar.bz2: @dist-checked.tag
	@echo '  CREATE $@'
	$(QUIET)$(TAR) -c $(shell LC_ALL=C $(TAR) --help | grep -q -- '--owner' && echo '--owner=0 --group=0') -f - -C dist $(DIST_SRC) $(DIST_EXTRA) | bzip2 -9 -z >$@

%.zip: @dist-checked.tag
	@echo '  CREATE $@'
	$(QUIET)rm -rf $@ && (cd dist && $(ZIP) -9 ../$@ $(DIST_SRC) $(DIST_EXTRA)) &>@zip.log

%.zpaq: @dist-checked.tag
	@echo '  CREATE $@'
	$(QUIET)rm -rf $@ && (cd dist && zpaq a ../$@ $(DIST_SRC) $(DIST_EXTRA) -m59) &>@zpaq.log

$(DIST_DIR)/@tmp-essentials.inc: src/version.c $(ALLOY_DEPS) $(lastword $(MAKEFILE_LIST))
	@echo '  ALLOYING...'
	$(QUIET)mkdir -p dist \
	&& (grep -v '#include ' src/alloy.c && echo '#define MDBX_BUILD_SOURCERY $(MDBX_BUILD_SOURCERY)' \
	&& $(SED) \
		-e 's|#include "../mdbx.h"|@INCLUDE "mdbx.h"|' \
		-e '/#include "preface.h"/r src/preface.h' \
		-e '/#include "osal.h"/r src/osal.h' \
		-e '/#include "options.h"/r src/options.h' \
		-e '/#include "atomics-types.h"/r src/atomics-types.h' \
		-e '/#include "layout-dxb.h"/r src/layout-dxb.h' \
		-e '/#include "layout-lck.h"/r src/layout-lck.h' \
		-e '/#include "logging_and_debug.h"/r src/logging_and_debug.h' \
		-e '/#include "utils.h"/r src/utils.h' \
		-e '/#include "pnl.h"/r src/pnl.h' \
		src/essentials.h \
	| $(SED) \
		-e '/#pragma once/d' -e '/#include "/d' \
		-e '/ clang-format o/d' -e '/ \*INDENT-O/d' \
		| grep -v '^/// ') >$@

$(DIST_DIR)/@tmp-internals.inc: $(DIST_DIR)/@tmp-essentials.inc src/version.c $(ALLOY_DEPS) $(lastword $(MAKEFILE_LIST))
	$(QUIET)(cat $(DIST_DIR)/@tmp-essentials.inc \
	&& $(SED) \
		-e '/#include "essentials.h"/d' \
		-e '/#include "atomics-ops.h"/r src/atomics-ops.h' \
		-e '/#include "proto.h"/r src/proto.h' \
		-e '/#include "txl.h"/r src/txl.h' \
		-e '/#include "unaligned.h"/r src/unaligned.h' \
		-e '/#include "cogs.h"/r src/cogs.h' \
		-e '/#include "cursor.h"/r src/cursor.h' \
		-e '/#include "dbi.h"/r src/dbi.h' \
		-e '/#include "dpl.h"/r src/dpl.h' \
		-e '/#include "gc.h"/r src/gc.h' \
		-e '/#include "lck.h"/r src/lck.h' \
		-e '/#include "meta.h"/r src/meta.h' \
		-e '/#include "node.h"/r src/node.h' \
		-e '/#include "page-iov.h"/r src/page-iov.h' \
		-e '/#include "page-ops.h"/r src/page-ops.h' \
		-e '/#include "spill.h"/r src/spill.h' \
		-e '/#include "sort.h"/r src/sort.h' \
		-e '/#include "tls.h"/r src/tls.h' \
		-e '/#include "walk.h"/r src/walk.h' \
		-e '/#include "windows-import.h"/r src/windows-import.h' \
		src/internals.h \
	| $(SED) \
		-e '/#pragma once/d' -e '/#include "/d' \
		-e '/ clang-format o/d' -e '/ \*INDENT-O/d' \
		| grep -v '^/// ') >$@

$(DIST_DIR)/mdbx.c: $(DIST_DIR)/@tmp-internals.inc $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)(cat $(DIST_DIR)/@tmp-internals.inc $(shell git ls-files src/*.c | grep -v alloy) src/version.c | $(SED) \
		-e '/#include "debug_begin.h"/r src/debug_begin.h' \
		-e '/#include "debug_end.h"/r src/debug_end.h' \
	) | $(SED) -e '/#include "/d;/#pragma once/d' -e 's|@INCLUDE|#include|' \
		-e '/ clang-format o/d;/ \*INDENT-O/d' -e '3i /* clang-format off */' | cat -s >$@

$(DIST_DIR)/mdbx.c++: $(DIST_DIR)/@tmp-essentials.inc src/mdbx.c++ $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)cat $(DIST_DIR)/@tmp-essentials.inc src/mdbx.c++ | $(SED) \
		-e '/#define xMDBX_ALLOY/d' \
		-e '/#include "/d;/#pragma once/d' \
		-e 's|@INCLUDE|#include|;s|"mdbx.h"|"mdbx.h++"|' \
		-e '/ clang-format o/d;/ \*INDENT-O/d' -e '3i /* clang-format off */' | cat -s >$@

define dist-tool-rule
$(DIST_DIR)/mdbx_$(1).c: src/tools/$(1).c src/tools/wingetopt.h src/tools/wingetopt.c \
		$(DIST_DIR)/@tmp-internals.inc $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $$@'
	$(QUIET)mkdir -p dist && $(SED) \
		-e '/#include "essentials.h"/r $(DIST_DIR)/@tmp-essentials.inc' \
		-e '/#include "wingetopt.h"/r src/tools/wingetopt.c' \
		-e '/ clang-format o/d' -e '/ \*INDENT-O/d' \
		src/tools/$(1).c \
	| $(SED) -e '/#include "/d;/#pragma once/d;/#define xMDBX_ALLOY/d' -e 's|@INCLUDE|#include|' \
		-e '/ clang-format o/d;/ \*INDENT-O/d' -e '9i /* clang-format off */' | cat -s >$$@

endef
$(foreach file,$(TOOLS),$(eval $(call dist-tool-rule,$(file))))

define dist-extra-rule
$(DIST_DIR)/$(1): $(1) src/version.c $(lastword $(MAKEFILE_LIST))
	@echo '  REFINE $$@'
	$(QUIET)mkdir -p $$(dir $$@) && $(SED) -e '/^#> dist-cutoff-begin/,/^#< dist-cutoff-end/d' $$< | cat -s >$$@

endef
$(foreach file,mdbx.h mdbx.h++ $(filter-out man1/% VERSION.json .clang-format-ignore %.in ntdll.def,$(DIST_EXTRA)),$(eval $(call dist-extra-rule,$(file))))

$(DIST_DIR)/VERSION.json: src/version.c
	@echo '  MAKE $@'
	$(QUIET)mkdir -p $(DIST_DIR)/ && echo "{ \"git_describe\": \"$(MDBX_GIT_DESCRIBE)\", \"git_timestamp\": \"$(MDBX_GIT_TIMESTAMP)\", \"git_tree\": \"$(shell git show --no-patch --format=%T HEAD 2>&1)\", \"git_commit\": \"$(shell git show --no-patch --format=%H HEAD 2>&1)\", \"semver\": \"$(MDBX_VERSION_PURE)\" }" >$@

$(DIST_DIR)/.clang-format-ignore: $(lastword $(MAKEFILE_LIST))
	@echo '  MAKE $@'
	$(QUIET)echo "$(filter-out %.h %h++,$(DIST_SRC))" | tr ' ' \\n > $@

$(DIST_DIR)/ntdll.def: src/ntdll.def
	@echo '  COPY $@'
	$(QUIET)mkdir -p $(DIST_DIR)/ && cp $< $@

$(DIST_DIR)/config.h.in: src/config.h.in
	@echo '  COPY $@'
	$(QUIET)mkdir -p $(DIST_DIR)/ && cp $< $@

$(DIST_DIR)/man1/mdbx_%.1: src/man1/mdbx_%.1
	@echo '  COPY $@'
	$(QUIET)mkdir -p $(DIST_DIR)/man1/ && cp $< $@

endif

################################################################################
# Cross-compilation simple test

CROSS_LIST = \
	mips64-linux-gnuabi64-gcc mips-linux-gnu-gcc \
	hppa-linux-gnu-gcc s390x-linux-gnu-gcc \
	powerpc64-linux-gnu-gcc powerpc-linux-gnu-gcc \
	arm-linux-gnueabihf-gcc aarch64-linux-gnu-gcc

## On Ubuntu Focal (22.04) with QEMU 6.2 (1:6.2+dfsg-2ubuntu6.6) & GCC 11.3 (11.3.0-1ubuntu1~22.04)
# sh4-linux-gnu-gcc           - coredump (qemu mmap-troubles)
# sparc64-linux-gnu-gcc       - coredump (qemu mmap-troubles, previously: qemu fails fcntl for F_SETLK/F_GETLK)
# alpha-linux-gnu-gcc         - coredump (qemu mmap-troubles)
# risc64-linux-gnu-gcc        - coredump (qemu qemu fails fcntl for F_SETLK/F_GETLK)
CROSS_LIST_NOQEMU = sh4-linux-gnu-gcc sparc64-linux-gnu-gcc alpha-linux-gnu-gcc riscv64-linux-gnu-gcc

cross-gcc:
	@echo '  Re-building by cross-compiler for: $(CROSS_LIST_NOQEMU) $(CROSS_LIST)'
	@echo "CORRESPONDING CROSS-COMPILERs ARE REQUIRED."
	@echo "FOR INSTANCE: sudo apt install \$$(apt list 'g++-*' | grep 'g++-[a-z0-9]\+-linux-gnu/' | cut -f 1 -d / | sort -u)"
	$(QUIET)for CC in $(CROSS_LIST_NOQEMU) $(CROSS_LIST); do \
		echo "===================== $$CC"; \
		$(MAKE) IOARENA=false CXXSTD= clean && CC=$$CC CXX=$$(echo $$CC | $(SED) 's/-gcc/-g++/') EXE_LDFLAGS=-static $(MAKE) IOARENA=false all || exit $$?; \
	done

# Unfortunately qemu don't provide robust support for futexes.
# Therefore it is impossible to run full multi-process tests.
cross-qemu:
	@echo '  Re-building by cross-compiler and re-check by QEMU for: $(CROSS_LIST)'
	@echo "CORRESPONDING CROSS-COMPILERs AND QEMUs ARE REQUIRED."
	@echo "FOR INSTANCE: "
	@echo "	1) sudo apt install \$$(apt list 'g++-*' | grep 'g++-[a-z0-9]\+-linux-gnu/' | cut -f 1 -d / | sort -u)"
	@echo "	2) sudo apt install binfmt-support qemu-user-static qemu-user \$$(apt list 'qemu-system-*' | grep 'qemu-system-[a-z0-9]\+/' | cut -f 1 -d / | sort -u)"
	$(QUIET)for CC in $(CROSS_LIST); do \
		echo "===================== $$CC + qemu"; \
		$(MAKE) IOARENA=false CXXSTD= clean && \
			CC=$$CC CXX=$$(echo $$CC | $(SED) 's/-gcc/-g++/') EXE_LDFLAGS=-static MDBX_BUILD_OPTIONS="-DMDBX_SAFE4QEMU $(MDBX_BUILD_OPTIONS)" \
			$(MAKE) IOARENA=false smoke-singleprocess test-singleprocess || exit $$?; \
	done

#< dist-cutoff-end

install: $(LIBRARIES) $(MDBX_TOOLS) $(HEADERS)
	@echo '  INSTALLING...'
	$(QUIET)mkdir -p $(DESTDIR)$(prefix)/bin$(suffix) && \
		$(INSTALL) -p $(EXE_INSTALL_FLAGS) $(MDBX_TOOLS) $(DESTDIR)$(prefix)/bin$(suffix)/ && \
	mkdir -p $(DESTDIR)$(prefix)/lib$(suffix)/ && \
		$(INSTALL) -p $(EXE_INSTALL_FLAGS) $(filter-out libmdbx.a,$(LIBRARIES)) $(DESTDIR)$(prefix)/lib$(suffix)/ && \
	mkdir -p $(DESTDIR)$(prefix)/lib$(suffix)/ && \
		$(INSTALL) -p libmdbx.a $(DESTDIR)$(prefix)/lib$(suffix)/ && \
	mkdir -p $(DESTDIR)$(prefix)/include/ && \
		$(INSTALL) -p -m 444 $(HEADERS) $(DESTDIR)$(prefix)/include/ && \
	mkdir -p $(DESTDIR)$(mandir)/man1/ && \
		$(INSTALL) -p -m 444 $(addprefix $(MAN_SRCDIR), $(MANPAGES)) $(DESTDIR)$(mandir)/man1/

install-strip: EXE_INSTALL_FLAGS = -s
install-strip: install

install-no-strip: EXE_INSTALL_FLAGS =
install-no-strip: install

uninstall:
	@echo '  UNINSTALLING/REMOVE...'
	$(QUIET)rm -f $(addprefix $(DESTDIR)$(prefix)/bin$(suffix)/,$(MDBX_TOOLS)) \
		$(addprefix $(DESTDIR)$(prefix)/lib$(suffix)/,$(LIBRARIES)) \
		$(addprefix $(DESTDIR)$(prefix)/include/,$(HEADERS)) \
		$(addprefix $(DESTDIR)$(mandir)/man1/,$(MANPAGES))

################################################################################
# Benchmarking by ioarena

ifeq ($(origin IOARENA),undefined)
IOARENA := $(shell \
  (test -x ../ioarena/@BUILD/src/ioarena && echo ../ioarena/@BUILD/src/ioarena) || \
  (test -x ../../@BUILD/src/ioarena && echo ../../@BUILD/src/ioarena) || \
  (test -x ../../src/ioarena && echo ../../src/ioarena) || which ioarena 2>&- || \
  (echo false && echo '$(TIP) Clone and build the https://abf.io/erthink/ioarena.git within a neighbouring directory for availability of benchmarking.' >&2))
endif
NN	?= 25000000
BENCH_CRUD_MODE ?= nosync

bench-clean:
	@echo '  REMOVE bench-*.txt _ioarena/*'
	$(QUIET)rm -rf bench-*.txt _ioarena/*

re-bench: bench-clean bench

ifeq ($(or $(IOARENA),false),false)
bench bench-quartet bench-triplet bench-couple:
	$(QUIET)echo 'The `ioarena` benchmark is required.' >&2 && \
	echo 'Please clone and build the https://abf.io/erthink/ioarena.git within a neighbouring `ioarena` directory.' >&2 && \
	false

else

.PHONY: bench bench-clean bench-couple re-bench bench-quartet bench-triplet

define bench-rule
bench-$(1)_$(2).txt: $(3) $(IOARENA) $(lastword $(MAKEFILE_LIST))
	@echo '  RUNNING ioarena for $1/$2...'
	$(QUIET)(export LD_LIBRARY_PATH="./:$$$${LD_LIBRARY_PATH}"; \
		ldd $(IOARENA) | grep -i $(1) && \
		$(IOARENA) -D $(1) -B batch -m $(BENCH_CRUD_MODE) -n $(2) \
			| tee $$@ | grep throughput | $(SED) 's/throughput/batch×N/' && \
		$(IOARENA) -D $(1) -B crud -m $(BENCH_CRUD_MODE) -n $(2) \
			| tee -a $$@ | grep throughput | $(SED) 's/throughput/   crud/' && \
		$(IOARENA) -D $(1) -B iterate,get,iterate,get,iterate -m $(BENCH_CRUD_MODE) -r 4 -n $(2) \
			| tee -a $$@ | grep throughput | $(SED) '0,/throughput/{s/throughput/iterate/};s/throughput/    get/' && \
		$(IOARENA) -D $(1) -B delete -m $(BENCH_CRUD_MODE) -n $(2) \
			| tee -a $$@ | grep throughput | $(SED) 's/throughput/ delete/' && \
	true) || mv -f $$@ $$@.error

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
