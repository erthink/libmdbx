#!/bin/bash
set -euo pipefail
make check
TESTDB_PREFIX=${1:-/dev/shm/mdbx-gc-test}.

function rep9 { printf "%*s" $1 '' | tr ' ' '9'; }
function join { local IFS="$1"; shift; echo "$*"; }
function bit2option { local -n arr=$1; (( ($2&(1<<$3)) != 0 )) && echo -n '+' || echo -n '-'; echo "${arr[$3]}"; }

options=(writemap coalesce lifo)

function bits2list {
	local -n arr=$1
	local i
	local list=()
	for ((i=0; i<${#arr[@]}; ++i)) do
		list[$i]=$(bit2option $1 $2 $i)
	done
	join , "${list[@]}"
}

function probe {
	echo "=============================================== $(date)"
	echo "${caption}: $*"
	rm -f ${TESTDB_PREFIX}* \
		&& ./mdbx_test --pathname=${TESTDB_PREFIX}db "$@" | lz4 > log.lz4 \
		&& ./mdbx_chk -nvvv ${TESTDB_PREFIX}db | tee ${TESTDB_PREFIX}chk \
		|| (echo "FAILED"; exit 1)
}

###############################################################################

caption="Failfast #1" probe \
	--pagesize=min --size=6G --table=+data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
	--nops=99999 --batch.write=9 --mode=-writemap,-coalesce,+lifo --keygen.seed=248240655 basic

caption="Failfast #2" probe \
	--pagesize=min --size=6G --table=-data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
	--nops=999999 --batch.write=999 --mode=+writemap,+coalesce,+lifo --keygen.seed=259083046 basic

caption="Failfast #3" probe \
	--pagesize=min --size=6G --table=-data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
	--nops=999999 --batch.write=999 --mode=+writemap,+coalesce,+lifo --keygen.seed=522365681 basic

caption="Failfast #4" probe \
	--pagesize=min --size=6G --table=-data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
	--nops=999999 --batch.write=9999 --mode=-writemap,+coalesce,+lifo --keygen.seed=866083781 basic

caption="Failfast #5" probe \
	--pagesize=min --size=6G --table=-data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
	--nops=999999 --batch.write=999 --mode=+writemap,-coalesce,+lifo --keygen.seed=246539192 basic

caption="Failfast #6" probe \
	--pagesize=min --size=6G --table=-data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
	--nops=999999 --batch.write=999 --mode=+writemap,+coalesce,+lifo --keygen.seed=540406278 basic

caption="Failfast #7" probe \
	--pagesize=min --size=6G --table=+data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
	--nops=999999 --batch.write=999 --mode=-writemap,+coalesce,+lifo --keygen.seed=619798690 basic

count=0
for nops in {2..7}; do
	for ((wbatch=nops-1; wbatch > 0; --wbatch)); do
		loops=$((1111/nops + 2))
		for ((rep=0; rep++ < loops; )); do
			for ((bits=2**${#options[@]}; --bits >= 0; )); do
				seed=$(date +%N)
				caption="Probe #$((++count)) int-key,w/o-dups, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size=6G --table=+key.integer,-data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
				caption="Probe #$((++count)) int-key,with-dups, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size=6G --table=+key.integer,+data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
				caption="Probe #$((++count)) int-key,int-data, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size=6G --table=+key.integer,+data.integer --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
				caption="Probe #$((++count)) w/o-dups, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size=6G --table=-data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
				caption="Probe #$((++count)) with-dups, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size=6G --table=+data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
			done
		done
	done
done

echo "=== ALL DONE ====================== $(date)"
