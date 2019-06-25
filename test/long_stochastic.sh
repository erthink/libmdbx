#!/bin/bash
set -euo pipefail
TESTDB_PREFIX=${1:-/dev/shm/mdbx-gc-test}.

rm -f $(dirname ${TESTDB_PREFIX})/*

if LC_ALL=C free | grep -q -i available; then
	ram_avail_mb=$(($(LC_ALL=C free | grep -i Mem: | tr -s [:blank:] ' ' | cut -d ' ' -f 7) / 1024))
else
	ram_avail_mb=$(($(LC_ALL=C free | grep -i Mem: | tr -s [:blank:] ' ' | cut -d ' ' -f 4) / 1024))
fi

ram_reserve4logs_mb=3333
if [ ${ram_avail_mb} -lt ${ram_reserve4logs_mb} ]; then
	echo "=== At least ${ram_reserve4logs_mb}Mb RAM required"
	exit -2
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
db_size_mb=$(expr '(' ${ram_avail_mb} - ${ram_reserve4logs_mb} ')' / 4)
echo "=== ${ram_avail_mb}M RAM available, use ${db_size_mb}M for DB"

make check
rm -f $(dirname ${TESTDB_PREFIX})/*

###############################################################################

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
		&& ./mdbx_test --ignore-dbfull --repeat=42 --pathname=${TESTDB_PREFIX}db "$@" | lz4 > ${TESTDB_PREFIX}log.lz4 \
		&& ./mdbx_chk -nvvv ${TESTDB_PREFIX}db | tee ${TESTDB_PREFIX}chk \
		&& ([ ! -e ${TESTDB_PREFIX}db-copy ] || ./mdbx_chk -nvvv ${TESTDB_PREFIX}db-copy | tee ${TESTDB_PREFIX}chk-copy) \
		|| (echo "FAILED"; exit 1)
}

###############################################################################

if [ ${db_size_mb} -gt 5555 ]; then
biggest=7
else
biggest=6
fi

count=0
for nops in $(seq ${biggest} -1 2); do
	for ((wbatch=nops-1; wbatch > 0; --wbatch)); do
		loops=$(((99 >> nops) / nops + 3))
		for ((rep=0; rep++ < loops; )); do
			for ((bits=2**${#options[@]}; --bits >= 0; )); do
				seed=$(date +%N)
				caption="Probe #$((++count)) int-key,w/o-dups, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size-upper=${db_size_mb}M --table=+key.integer,-data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
				caption="Probe #$((++count)) int-key,with-dups, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size-upper=${db_size_mb}M --table=+key.integer,+data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
				caption="Probe #$((++count)) int-key,int-data, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size-upper=${db_size_mb}M --table=+key.integer,+data.integer --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
				caption="Probe #$((++count)) w/o-dups, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size-upper=${db_size_mb}M --table=-data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=1111 \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
				caption="Probe #$((++count)) with-dups, repeat ${rep} of ${loops}" probe \
					--pagesize=min --size-upper=${db_size_mb}M --table=+data.dups --keylen.min=min --keylen.max=max --datalen.min=min --datalen.max=max \
					--nops=$( rep9 $nops ) --batch.write=$( rep9 $wbatch ) --mode=$(bits2list options $bits) \
					--keygen.seed=${seed} basic
			done
		done
	done
done

echo "=== ALL DONE ====================== $(date)"
