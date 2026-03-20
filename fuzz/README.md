# OSS-Fuzz Integration – Local Testing Guide

This guide explains how to build, run, and test fuzzers locally using OSS-Fuzz.

## Prerequisites

- Docker
- Python 3
- Git
- Clang

~~~ sh
git clone https://github.com/Segwaz/oss-fuzz.git
cd oss-fuzz
~~~

## 1. Build image

~~~sh
python3 infra/helper.py build_image libmdbx
~~~
~~~
~~~

## 1. Build fuzzers

~~~ sh
python3 infra/helper.py build_fuzzers libmdbx
~~~
~~~
~~~

## 2. Run a fuzzer

~~~ sh
python3 infra/helper.py run_fuzzer libmdbx fuzz_raw_db_format
~~~

## 3. Reproduce

In libmdbx repository, make an instrumented build of libdbmx.a:

~~~ sh
make libmdbx.a CC=clang CXX=clang++ CFLAGS="$CFLAGS -fsanitize=fuzzer,address" \
    CXXFLAGS="$CXXFLAGS -fsanitize=fuzzer,address"
~~~

Then build the fuzzer:

~~~ sh
~~~sh
clang fuzz/fuzz_raw_db_format.c fuzz/logger.c fuzz/mode_desc.c -I. -I fuzz/ \
    libmdbx.a -fsanitize=fuzzer,address -o fuzz_raw_db_format
~~~
~~~
~~~ 

Run it with the testcase:

~~~sh
./fuzz_raw_db_format crash-XXXXXX
~~~

Debug output can be seen by setting MDBX_DEBUG in fuzz.h.
~~~
~~~

## Generate the seeds

Build the seed generator and run it:

~~~sh
gcc fuzz/utils/raw_db_gen.c fuzz/mode_desc.c -I. -I fuzz libmdbx.a -o seed_gen
./seed_gen
~~~
~~~

Seeds are in ./corpus. To integrate with OSS-Fuzz they must be placed in a zip  
file named <fuzzer_name>_seed_corpus.zip placed in ./fuzz/seed directory.
