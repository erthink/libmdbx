#!/usr/bin/env bash

# Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru>
# SPDX-License-Identifier: Apache-2.0

TMUX=tmux
DIR="$(dirname ${BASH_SOURCE[0]})"
TEST="${DIR}/stochastic.sh --skip-make --db-upto-gb 32"
PREFIX="/dev/shm/mdbxtest-"

NUMACTL="$(which numactl 2>-)"
NUMALIST=()
NUMAIDX=0
if [ -n "${NUMACTL}" -a $(${NUMACTL} --hardware | grep 'node [0-9]\+ cpus' | wc -l) -gt 1 ]; then
	NUMALIST=($(${NUMACTL} --hardware | grep 'node [0-9]\+ cpus' | cut -d ' ' -f 2))
fi

function test_numacycle {
	NUMAIDX=$((NUMAIDX + 1))
	if [ ${NUMAIDX} -ge ${#NUMALIST[@]} ]; then
		NUMAIDX=0
	fi
}

function test_numanode {
	if [[ ${#NUMALIST[@]} > 1 ]]; then
		echo "${TEST} --numa ${NUMALIST[$NUMAIDX]}"
	else
		echo "${TEST}"
	fi
}

${TMUX} kill-session -t mdbx
rm -rf ${PREFIX}*
# git clean -x -f -d && make test-assertions
${TMUX} -f "${DIR}/tmux.conf" new-session -d -s mdbx htop

W=0
for ps in min 4k max; do
	for from in 1 30000; do
		for n in 0 1 2 3; do
			CMD="$(test_numanode) --delay $((n * 7)) --page-size ${ps} --from ${from} --dir ${PREFIX}page-${ps}.from-${from}.${n}"
			if [ $n -eq 0 ]; then
				${TMUX} new-window -t mdbx:$((++W)) -n "page-${ps}.from-${from}" -k -d "$CMD"
				${TMUX} select-layout -E tiled
			else
				${TMUX} split-window -t mdbx:$W -l 20% -d $CMD
			fi
			test_numacycle
		done
		for n in 0 1 2 3; do
			CMD="$(test_numanode) --delay $((3 + n * 7)) --extra --page-size ${ps} --from ${from} --dir ${PREFIX}page-${ps}.from-${from}.${n}-extra"
			if [ $n -eq 0 ]; then
				${TMUX} new-window -t mdbx:$((++W)) -n "page-${ps}.from-${from}-extra" -k -d "$CMD"
				${TMUX} select-layout -E tiled
			else
				${TMUX} split-window -t mdbx:$W -l 20% -d $CMD
			fi
			test_numacycle
		done
	done
done

${TMUX} attach -t mdbx
