#!/usr/bin/env bash

echo "------------------------------------------------------------------------------"

if [ -z "$1" ]; then
	echo "No mdbx-db pathname given";
	exit 2
elif [ ! -e "$1" ]; then
	echo "The mdbx-db '$1' don't exists";
	exit 2
else
	echo ">>>>>>>>>> $1"
	RECO="$1.recovered"
	rm -f dump1.txt dump2.txt "$RECO"
	if ./mdbx_chk "$1"; then
		echo ">>>>>>>>>> SOURCE VALID"
		(./mdbx_dump -a "$1" > dump1.txt && \
		./mdbx_load -nf dump1.txt "$RECO" && \
		./mdbx_chk "$RECO" && \
		echo ">>>>>>>>>> DUMP/LOAD/CHK OK") || (echo ">>>>>>>>>> DUMP/LOAD/CHK FAILED"; exit 1)
		REMOVE_RECO=1
	elif ./mdbx_chk -i "$1"; then
		echo ">>>>>>>>>> SOURCE HAS WRONG-ORDER, TRY RECOVERY"
		(./mdbx_dump -a "$1" > dump1.txt && \
		./mdbx_load -anf dump1.txt "$RECO" && \
		./mdbx_chk -i "$RECO" && \
		echo ">>>>>>>>>> DUMP/LOAD/CHK OK") || (echo ">>>>>>>>>> DUMP/LOAD/CHK FAILED"; exit 1)
		REMOVE_RECO=0
	else
		echo ">>>>>>>>>> SOURCE CORRUPTED, TRY RECOVERY"
		(./mdbx_dump -ar "$1" > dump1.txt && \
		./mdbx_load -ranf dump1.txt "$RECO" && \
		./mdbx_chk -i "$RECO" && \
		echo ">>>>>>>>>> DUMP/LOAD/CHK OK") || (echo ">>>>>>>>>> DUMP/LOAD/CHK FAILED"; exit 1)
		REMOVE_RECO=0
	fi
	./mdbx_dump -a "$RECO" > dump2.txt && diff -u dump1.txt dump2.txt && \
	rm -f dump1.txt dump2.txt && [ $REMOVE_RECO -ne 0 ] && rm -f "$RECO"
	exit 0
fi
