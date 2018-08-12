#!/bin/bash
set -euo pipefail
TESTDB_PREFIX=${1:-/dev/shm/mdbx-gc-test}

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

for nops in {1..7}; do
	for ((wbatch=nops; wbatch > 0; --wbatch)); do
		for ((bits=2**${#options[@]}; --bits >= 0; )); do
			echo "=================================== $(date)"
			rm -f ${TESTDB_PREFIX}*
			echo --nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits)
			./mdbx_test --pathname=${TESTDB_PREFIX} --size=8G --keylen.min=1 --keylen.max=250 --datalen.min=1 --datalen.max=500 \
				--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
				--keygen.seed=$(date +%N) --hill | bzip2 -c > ${TESTDB_PREFIX}.log.bz2
			./mdbx_chk -nvv ${TESTDB_PREFIX} | tee ${TESTDB_PREFIX}-chk.log
		done
	done
done

echo "=== ALL DONE ====================== $(date)"
