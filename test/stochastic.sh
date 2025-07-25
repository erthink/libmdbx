#!/usr/bin/env bash

# Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru>
# SPDX-License-Identifier: Apache-2.0

if [ "${BASH_VERSION}" \< "4.3" ]; then
  echo "Bash-shell 4.3 or later is REQUIRED." >&2
  echo "Present Bash-shell version is '${BASH_VERSION}' (BASH_VERSION)" >&2
  exit
fi

LIST=basic
FROM=1
UPTO=9999999
MONITOR=
LOOPS=
SKIP_MAKE=no
GEOMETRY_JITTER=yes
BANNER="$(which banner 2>/dev/null || echo echo)"
UNAME="$(uname -s 2>/dev/null || echo Unknown)"
DB_UPTO_MB=17408
PAGESIZE=min
DONT_CHECK_RAM=no
EXTRA=no
TAILLOG=0
DELAY=0
REPORT_DEPTH=no
REPEAT=11
ROUNDS=1
SMALL=no
NUMABIND=

while [ -n "$1" ]
do
  case "$1" in
  --help)
    echo "--multi                Engage multi-process test scenario (default)"
    echo "--single               Execute series of single-process tests (for QEMU, etc)"
    echo "--nested               Execute only 'nested' testcase"
    echo "--hill                 Execute only 'hill' testcase"
    echo "--append               Execute only 'append' testcase"
    echo "--ttl                  Execute only 'ttl' testcase"
    echo "--with-valgrind        Run tests under Valgrind's memcheck tool"
    echo "--skip-make            Don't (re)build libmdbx and test's executable"
    echo "--from NN              Start iterating from the NN ops per test case"
    echo "--upto NN              Don't run tests with more than NN ops per test case"
    echo "--repeat NN            Repeat each testcase NN times within test run"
    echo "--rounds NN            Cycle each n-ops/wbatch case NN times"
    echo "--loops NN             Stop after the NN loops"
    echo "--dir PATH             Specifies directory for test DB and other files (it will be cleared)"
    echo "--db-upto-mb NN        Limits upper size of test DB to the NN megabytes"
    echo "--db-upto-gb NN        --''--''--''--''--''--''--''--''--  NN gigabytes"
    echo "--no-geometry-jitter   Disable jitter for geometry upper-size"
    echo "--pagesize NN          Use specified page size (256 is minimal and used by default)"
    echo "--numa NODE            Bind to the specified NUMA node"
    echo "--dont-check-ram-size  Don't check available RAM"
    echo "--extra                Iterate extra modes/flags"
    echo "--taillog              Dump tail of test log on failure"
    echo "--delay NN             Delay NN seconds before run test"
    echo "--report-depth         Report tree depth (tee+grep log)"
    echo "--small                Small transactions/batch/nops pattern"
    echo "--help                 Print this usage help and exit"
    exit -2
  ;;
  --taillog)
    TAILLOG=3333
  ;;
  --multi)
    LIST=basic
  ;;
  --single)
    LIST="--nested --hill --append --ttl --copy"
  ;;
  --nested)
    LIST="--nested"
  ;;
  --hill)
    LIST="--hill"
  ;;
  --append)
    LIST="--append"
  ;;
  --ttl)
    LIST="--ttl"
  ;;
  --with-valgrind)
    echo " NOTE: Valgrind could produce some false-positive warnings"
    echo "       in multi-process environment with shared memory."
    echo "       For instance, when the process 'A' explicitly marks a memory"
    echo "       region as 'undefined', the process 'B' fill it,"
    echo "       and after this process 'A' read such region, etc."
    MONITOR="valgrind --trace-children=yes --log-file=valgrind-%p.log --leak-check=full --track-origins=yes --read-var-info=yes --error-exitcode=42 --suppressions=test/valgrind_suppress.txt"
    rm -f valgrind-*.log
  ;;
  --skip-make)
    SKIP_MAKE=yes
  ;;
  --from)
    FROM=$(($2))
    if [ -z "$FROM" -o "$FROM" -lt 1 ]; then
      echo "Invalid value '$FROM' for --from option"
      exit -2
    fi
    shift
  ;;
  --upto)
    UPTO=$(($2))
    if [ -z "$UPTO" -o "$UPTO" -lt 1 ]; then
      echo "Invalid value '$UPTO' for --upto option"
      exit -2
    fi
    shift
  ;;
  --repeat|--reps|--rep)
    REPEAT=$(($2))
    if [ -z "$REPEAT" -o "$REPEAT" -lt 1 -o "$REPEAT" -gt 99 ]; then
      echo "Invalid value '$REPEAT' for --repeat option"
      exit -2
    fi
    shift
  ;;
  --rounds)
    ROUNDS=$(($2))
    if [ -z "$ROUNDS" -o "$ROUNDS" -lt 1 -o "$ROUNDS" -gt 99 ]; then
      echo "Invalid value '$ROUNDS' for --rounds option"
      exit -2
    fi
    shift
  ;;
  --loops)
    LOOPS=$(($2))
    if [ -z "$LOOPS" -o "$LOOPS" -lt 1 -o "$LOOPS" -gt 99 ]; then
      echo "Invalid value '$LOOPS' for --loops option"
      exit -2
    fi
    shift
  ;;
  --dir)
    TESTDB_DIR="$2"
    if [ -z "$TESTDB_DIR" ]; then
      echo "Invalid value '$TESTDB_DIR' for --dir option"
      exit -2
    fi
    shift
  ;;
  --db-upto-mb)
    DB_UPTO_MB=$(($2))
    if [ -z "$DB_UPTO_MB" -o "$DB_UPTO_MB" -lt 1 -o "$DB_UPTO_MB" -gt 4194304 ]; then
      echo "Invalid value '$DB_UPTO_MB' for --db-upto-mb option"
      exit -2
    fi
    shift
  ;;
  --db-upto-gb)
    DB_UPTO_MB=$(($2 * 1024))
    if [ -z "$DB_UPTO_MB" -o "$DB_UPTO_MB" -lt 1 -o "$DB_UPTO_MB" -gt 4194304 ]; then
      echo "Invalid value '$2' for --db-upto-gb option"
      exit -2
    fi
    shift
  ;;
  --no-geometry-jitter)
    GEOMETRY_JITTER=no
  ;;
  --pagesize|--page-size)
    case "$2" in
      min|max|256|512|1024|2048|4096|8192|16386|32768|65536)
        PAGESIZE=$2
      ;;
      1|1k|1K|k|K)
        PAGESIZE=$((1024*1))
      ;;
      2|2k|2K)
        PAGESIZE=$((1024*2))
      ;;
      4|4k|4K)
        PAGESIZE=$((1024*4))
      ;;
      8|8k|8K)
        PAGESIZE=$((1024*8))
      ;;
      16|16k|16K)
        PAGESIZE=$((1024*16))
      ;;
      32|32k|32K)
        PAGESIZE=$((1024*32))
      ;;
      64|64k|64K)
        PAGESIZE=$((1024*64))
      ;;
      *)
        echo "Invalid page size '$2'"
        exit -2
      ;;
    esac
    shift
  ;;
  --dont-check-ram-size)
    DONT_CHECK_RAM=yes
  ;;
  --extra)
    EXTRA=yes
  ;;
  --delay)
    DELAY=$(($2))
    shift
  ;;
  --report-depth)
    REPORT_DEPTH=yes
  ;;
  --small)
    SMALL=yes
  ;;
  --numa)
    NUMANODE=$2
    if [[ ! $NUMANODE =~ ^[0-9]+$ ]]; then
      echo "Invalid value '$NUMANODE' for --numa option, expect an integer of NUMA-node"
      exit -2
    fi
    NUMABIND="numactl --membind ${NUMANODE} --cpunodebind ${NUMANODE}"
    shift
  ;;
  *)
    echo "Unknown option '$1'"
    exit -2
  ;;
  esac
 shift
done

set -euo pipefail
if [ -z "$MONITOR" ]; then
  export MALLOC_CHECK_=7 MALLOC_PERTURB_=42
fi

if ! which $([ "$SKIP_MAKE" == "no" ] && echo make cc c++) tee >/dev/null; then
  echo "Please install the following prerequisites: make cc c++ tee banner" >&2
  exit 1
fi

###############################################################################
# 1. clean data from prev runs and examine available RAM

WANNA_MOUNT=0
case ${UNAME} in
  Linux)
    MAKE=make
    if [ -z "${TESTDB_DIR:-}" ]; then
      for old_test_dir in $(ls -d /dev/shm/mdbx-test.[0-9]* 2>/dev/null); do
        rm -rf $old_test_dir
      done
      TESTDB_DIR="/dev/shm/mdbx-test.$$"
    fi
    mkdir -p $TESTDB_DIR && rm -f $TESTDB_DIR/*

    if LC_ALL=C free | grep -q -i available; then
      ram_avail_mb=$(($(LC_ALL=C free | grep -i Mem: | tr -s '[:blank:]' ' ' | cut -d ' ' -f 7) / 1024))
    else
      ram_avail_mb=$(($(LC_ALL=C free | grep -i Mem: | tr -s '[:blank:]' ' ' | cut -d ' ' -f 4) / 1024))
    fi
  ;;

  FreeBSD)
    MAKE=gmake
    if [ -z "${TESTDB_DIR:-}" ]; then
      for old_test_dir in $(ls -d /tmp/mdbx-test.[0-9]* 2>/dev/null); do
        umount $old_test_dir && rm -r $old_test_dir
      done
      TESTDB_DIR="/tmp/mdbx-test.$$"
      rm -rf $TESTDB_DIR && mkdir -p $TESTDB_DIR
      WANNA_MOUNT=1
    else
      mkdir -p $TESTDB_DIR && rm -f $TESTDB_DIR/*
    fi
    ram_avail_mb=$(($(LC_ALL=C vmstat -s | grep -ie '[0-9] pages free$' | cut -d p -f 1) * ($(LC_ALL=C vmstat -s | grep -ie '[0-9] bytes per page$' | cut -d b -f 1) / 1024) / 1024))
  ;;

  Darwin)
    MAKE=make
    if [ -z "${TESTDB_DIR:-}" ]; then
      for vol in $(ls -d /Volumes/mdx[0-9]*[0-9]tst 2>/dev/null); do
        disk=$(mount | grep $vol | cut -d ' ' -f 1)
        echo "umount: volume $vol disk $disk"
        hdiutil unmount $vol -force
        hdiutil detach $disk
      done
      TESTDB_DIR="/Volumes/mdx$$tst"
      WANNA_MOUNT=1
    else
      mkdir -p $TESTDB_DIR && rm -f $TESTDB_DIR/*
    fi
    pagesize=$(($(LC_ALL=C vm_stat | grep -o 'page size of [0-9]\+ bytes' | cut -d' ' -f 4) / 1024))
    freepages=$(LC_ALL=C vm_stat | grep '^Pages free:' | grep -o '[0-9]\+\.$' | cut -d'.' -f 1)
    ram_avail_mb=$((pagesize * freepages / 1024))
    echo "pagesize ${pagesize}K, freepages ${freepages}, ram_avail_mb ${ram_avail_mb}"
  ;;

  MSYS*|MINGW*)
    if [ -z "${TESTDB_DIR:-}" ]; then
      for old_test_dir in $(ls -d /tmp/mdbx-test.[0-9]* 2>/dev/null); do
        rm -rf $old_test_dir
      done
      TESTDB_DIR="/tmp/mdbx-test.$$"
    fi
    mkdir -p $TESTDB_DIR && rm -f $TESTDB_DIR/*

    echo "FIXME: Fake support for ${UNAME}"
    ram_avail_mb=32768
  ;;

  *)
    echo "FIXME: ${UNAME} not supported by this script"
    exit 2
  ;;
esac

ulimit -c unlimited || echo "failed set unlimited core-dump size" >&2
rm -f ${TESTDB_DIR}/*

###############################################################################
# 2. estimate reasonable RAM space for test-db

echo "=== ${ram_avail_mb}M RAM available"
if [ $DONT_CHECK_RAM = yes ]; then
  db_size_mb=$DB_UPTO_MB
  ram_reserve4logs_mb=64
else
  ram_reserve4logs_mb=1234
  if [ $ram_avail_mb -lt $ram_reserve4logs_mb ]; then
    echo "=== At least ${ram_reserve4logs_mb}Mb RAM required"
    exit 3
  fi

#
# В режимах отличных от MDBX_WRITEMAP изменения до записи в файл
# будут накапливаться в памяти, что может потребовать свободной
# памяти размером с БД. Кроме этого, в тест входит сценарий
# создания копия БД на ходу. Поэтому БД не может быть больше 1/3
# от доступной памяти. Однако, следует учесть что malloc() будет
# не сразу возвращать выделенную память системе, а также
# предусмотреть места для логов.
#
# In non-MDBX_WRITEMAP modes, updates (dirty pages) will
# accumulate in memory before writing to the disk, which may
# require a free memory up to the size of a whole database. In
# addition, the test includes a script create a copy of the
# database on the go. Therefore, the database cannot be more 1/3
# of available memory. Moreover, should be taken into account
# that malloc() will not return the allocated memory to the
# system immediately, as well some space is required for logs.
#
  db_size_mb=$(((ram_avail_mb - ram_reserve4logs_mb) / 4))
  if [ $db_size_mb -gt $DB_UPTO_MB ]; then
    db_size_mb=$DB_UPTO_MB
  fi
fi
echo "=== use ${db_size_mb}M for DB"

###############################################################################
# 3. Create test-directory in ramfs/tmpfs, i.e. create/format/mount if required
case ${UNAME} in
  Linux)
    ulimit -c unlimited
    if [ "$(cat /proc/sys/kernel/core_pattern)" != "core.%p" ]; then
      echo "core.%p > /proc/sys/kernel/core_pattern" >&2
      if [ $(id -u) -ne 0 -a -n "$(which sudo 2>/dev/null)" ]; then
        echo "core.%p" | sudo tee /proc/sys/kernel/core_pattern || true
      else
        (echo "core.%p" > /proc/sys/kernel/core_pattern) || true
      fi
    fi
  ;;

  FreeBSD)
    if [[ WANNA_MOUNT ]]; then
      mount -t tmpfs tmpfs $TESTDB_DIR
    fi
  ;;

  Darwin)
    if [[ WANNA_MOUNT ]]; then
      ramdisk_size_mb=$((42 + db_size_mb * 2 + ram_reserve4logs_mb))
      number_of_sectors=$((ramdisk_size_mb * 2048))
      ramdev=$(hdiutil attach -nomount ram://${number_of_sectors})
      diskutil erasevolume ExFAT "mdx$$tst" ${ramdev}
    fi
  ;;

  MSYS*|MINGW*)
    echo "FIXME: Fake support for ${UNAME}"
  ;;

  *)
    echo "FIXME: ${UNAME} not supported by this script"
    exit 2
  ;;
esac

###############################################################################
# 4. build the test executables

if [ "$SKIP_MAKE" != "yes" ]; then
  ${MAKE} -j$(which nproc  >/dev/null 2>/dev/null && nproc || echo 2) build-test
fi

###############################################################################
# 5. run stochastic iterations

if which setsid >/dev/null 2>/dev/null; then
  SETSID=$(which setsid)
else
  SETSID=""
fi

if which lz4 >/dev/null; then
  function logger {
    ${SETSID} lz4 -z -c > ${TESTDB_DIR}/long.log.lz4 || echo "FAILED 'lz4 -z -c > ${TESTDB_DIR}/long.log.lz4'" >&2
  }
  function taillog {
    if [ -s ${TESTDB_DIR}/long.log.lz4 ]; then
      echo "=============================================== last ${TAILLOG} lines"
      lz4 -d -c ${TESTDB_DIR}/long.log.lz4 | tail -n ${TAILLOG}
    else
      echo "=============================================== no test log"
    fi
  }
elif which gzip >/dev/null; then
  function logger {
    ${SETSID} gzip -c -k > ${TESTDB_DIR}/long.log.gz || echo "FAILED 'gzip -c -k > ${TESTDB_DIR}/long.log.gz'" >&2
  }
  function taillog {
    if [ -s ${TESTDB_DIR}/long.log.gz ]; then
      echo "=============================================== last ${TAILLOG} lines"
      gzip -d -c ${TESTDB_DIR}/long.log.gz | tail -n ${TAILLOG}
    else
      echo "=============================================== no test log"
   fi
  }
else
  function logger {
    cat > ${TESTDB_DIR}/long.log || echo "FAILED 'cat > ${TESTDB_DIR}/long.log'" >&2
  }
  function taillog {
    if [ -s ${TESTDB_DIR}/long.log ]; then
      echo "=============================================== last ${TAILLOG} lines"
      tail -n ${TAILLOG} ${TESTDB_DIR}/long.log
    else
      echo "=============================================== no test log"
    fi
  }
fi

if [ "$EXTRA" != "no" ]; then
  options=(perturb nomeminit nordahead writemap lifo nostickythreads validation)
else
  options=(writemap lifo nostickythreads)
fi
syncmodes=("" ,+nosync-safe ,+nosync-utterly ,+nometasync)
function join { local IFS="$1"; shift; echo "$*"; }

function bits2options {
  local bits=$1
  local i
  local list=()
  for ((i = 0; i < ${#options[@]}; ++i)); do
    list[$i]=$( (( (bits & (1 << i)) != 0 )) && echo -n '+' || echo -n '-'; echo ${options[$i]})
  done
  join , ${list[@]}
}

LFD=0
trap "echo 'SIGPIPE(ignored)'" SIGPIPE

function failed {
  set +euo pipefail
  echo "FAILED" >&2
  if [ ${LFD} -ne 0 ]; then
    sleep 0.05
    echo "@@@ END-OF-LOG/FAILED" >&${LFD}
    sleep 0.05
    exec {LFD}>&-
    LFD=0
  fi
  if [ ${TAILLOG} -gt 0 ]; then
    taillog
  fi
  exit 1
}

function on_exit {
  set +euo pipefail
  if [ ${LFD} -ne 0 ]; then
    sleep 0.05
    echo "@@@ END-OF-LOG/EXIT" >&${LFD}
    sleep 0.05
    exec {LFD}>&-
    LFD=0
  fi
  echo "--- EXIT" >&2
}
trap on_exit EXIT

function probe {
  echo "----------------------------------------------- $(date)"
  echo "PROBE №${caption}"
  rm -f ${TESTDB_DIR}/* || failed
  for case in $LIST
  do
    echo "${speculum} --random-writemap=no --ignore-dbfull --repeat=${REPEAT} --pathname=${TESTDB_DIR}/long.db --cleanup-after=no --geometry-jitter=${GEOMETRY_JITTER} $@ $case"
    if [[ ${REPORT_DEPTH} = "yes" && ($case = "basic" || $case = "--hill") ]]; then
      if [ -z "${TEE4PIPE:-}" ]; then
        TEE4PIPE=$(tee --help | grep -q ' -p' && echo "tee -i -p" || echo "tee -i")
      fi
      exec {LFD}> >(${TEE4PIPE} >(logger) | grep -e reach -e achieve)
    else
      exec {LFD}> >(logger)
    fi
    ${NUMABIND} ${MONITOR} ./mdbx_test ${speculum} --random-writemap=no --ignore-dbfull --repeat=${REPEAT} --pathname=${TESTDB_DIR}/long.db --cleanup-after=no --geometry-jitter=${GEOMETRY_JITTER} "$@" $case >&${LFD} \
      && ${NUMABIND} ${MONITOR} ./mdbx_chk -q ${TESTDB_DIR}/long.db | tee ${TESTDB_DIR}/long-chk.log \
      && ([ ! -e ${TESTDB_DIR}/long.db-copy ] || ${NUMABIND} ${MONITOR} ./mdbx_chk -q ${TESTDB_DIR}/long.db-copy | tee ${TESTDB_DIR}/long-chk-copy.log) \
      || failed
    if [ ${LFD} -ne 0 ]; then
      echo "@@@ END-OF-LOG/ITERATION" >&${LFD}
      exec {LFD}>&-
      LFD=0
    fi
  done
}

function pass {
  for ((round=1; round <= ROUNDS; ++round)); do
    echo "======================================================================="
    if [[ $ROUNDS > 1 ]]; then
      ${BANNER} "$nops / $wbatch / round $round of $ROUNDS"
    else
      ${BANNER} "$nops / $wbatch"
    fi
    subcase=0
    for ((bits=2**${#options[@]}; --bits >= 0; )); do
      seed=$(($(date +%s) + RANDOM))

      split=30
      caption="$((++count)) int-key,with-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) int-key,int-data, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.integer --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) with-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) int-key,fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}

      split=24
      caption="$((++count)) int-key,with-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) int-key,int-data, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.integer --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) with-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) int-key,fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}

      split=16
      caption="$((++count)) int-key,w/o-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,-data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) int-key,with-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) int-key,int-data, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.integer --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) w/o-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=-data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) with-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) int-key,fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}

      if [ "$EXTRA" != "no" ]; then
        split=10
        caption="$((++count)) int-key,w/o-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
          --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,-data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
          --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
        caption="$((++count)) int-key,with-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
          --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
          --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
        caption="$((++count)) int-key,int-data, split=${split}, case $((++subcase)) of ${cases}" probe \
          --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.integer --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
          --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
        caption="$((++count)) w/o-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
          --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=-data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
          --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
        caption="$((++count)) with-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
          --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
          --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
        caption="$((++count)) int-key,fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
          --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
          --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
        caption="$((++count)) fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
          --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
          --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      fi

      split=4
      caption="$((++count)) int-key,w/o-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,-data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) int-key,int-data, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.integer --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) w/o-dups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=-data.multi --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) int-key,fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+key.integer,+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
      caption="$((++count)) fixdups, split=${split}, case $((++subcase)) of ${cases}" probe \
        --prng-seed=${seed} --pagesize=$PAGESIZE --size-upper-upto=${db_size_mb}M --table=+data.fixed --keygen.split=${split} --keylen.min=min --keylen.max=max --datalen=rnd \
        --nops=$nops --batch.write=$wbatch --mode=$(bits2options $bits)${syncmodes[count%4]}
    done # options
    cases="${subcase}"
  done
}

#------------------------------------------------------------------------------

if [ "$DELAY" != "0" ]; then
  sleep $DELAY
fi

count=0
loop=0
cases='?'
if [[ $SMALL != "yes" ]]; then
  for nops in 10 33 100 333 1000 3333 10000 33333 100000 333333 1000000 3333333 10000000 33333333 100000000 333333333 1000000000; do
    if [ $nops -lt $FROM ]; then continue; fi
    if [ $nops -gt $UPTO ]; then echo "The '--upto $UPTO' limit reached"; break; fi
    if [ -n "$LOOPS" ] && [ $loop -ge "$LOOPS" ]; then echo "The '--loops $LOOPS' limit reached"; break; fi
    echo "======================================================================="
    wbatch=$((nops / 7 + 1))
    speculum=$([ $nops -le 1000 ] && echo '--speculum' || true)
    while true; do
      pass
      loop=$((loop + 1))
      if [ -n "$LOOPS" ] && [ $loop -ge "$LOOPS" ]; then break; fi
      wbatch=$(((wbatch > 7) ? wbatch / 7 : 1))
      if [ $wbatch -eq 1 -o $((nops / wbatch)) -gt 1000 ]; then break; fi
    done # batch (write-ops per txn)
  done # n-ops
else
  for ((wbatch=FROM; wbatch<=UPTO; ++wbatch)); do
    if [ -n "$LOOPS" ] && [ $loop -ge "$LOOPS" ]; then echo "The '--loops $LOOPS' limit reached"; break; fi
    echo "======================================================================="
    speculum=$([ $wbatch -le 1000 ] && echo '--speculum' || true)
    nops=$((wbatch / 7 + 1))
    pass
    loop=$((loop + 1))
  done # wbatch
fi

echo "=== ALL DONE ====================== $(date)"
