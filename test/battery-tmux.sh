#!/usr/bin/env bash

# Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru>
# SPDX-License-Identifier: Apache-2.0

TEST="./test/long_stochastic.sh --skip-make"
PREFIX="/dev/shm/mdbxtest-"

tmux kill-session -t mdbx
rm -rf ${PREFIX}*
# git clean -x -f -d && make test-assertions
tmux -f ./test/tmux.conf new-session -d -s mdbx htop

W=0
for ps in min 4k max; do
	for from in 1 30000; do
		for n in 0 1 2 3 4 5 6 7; do
			CMD="${TEST} --delay $((n * 7)) --page-size ${ps} --from ${from} --dir ${PREFIX}page-${ps}.from-${from}.${n}"
			if [ $n -eq 0 ]; then
				tmux new-window -t mdbx:$((++W)) -n "page-${ps}.from-${from}" -k -d "$CMD"
				tmux select-layout -E tiled
			else
				tmux split-window -t mdbx:$W -l 20% -d $CMD
			fi
		done
		for n in 0 1 2 3 4 5 6 7; do
			CMD="${TEST} --delay $((3 + n * 7)) --extra --page-size ${ps} --from ${from} --dir ${PREFIX}page-${ps}.from-${from}.${n}-extra"
			if [ $n -eq 0 ]; then
				tmux new-window -t mdbx:$((++W)) -n "page-${ps}.from-${from}-extra" -k -d "$CMD"
				tmux select-layout -E tiled
			else
				tmux split-window -t mdbx:$W -l 20% -d $CMD
			fi
		done
	done
done

tmux attach -t mdbx
