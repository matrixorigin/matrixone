#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
EXT_DIR=$DIR

STARTTIME=0
function start 
{
	echo -n $1
	STARTTIME=$(date +%s)
}

function pass 
{
	ENDTIME=$(date +%s)
	ELAPSED=$(($ENDTIME - $STARTTIME))
	echo "[OK $ELAPSED sec]"
}

function fail
{
	echo "[FAILED]"
	exit 1
}


start "decNumber: ... ..."
(cd decNumber && make && make install) >& out/decNumber.out && pass || fail
