#!/bin/bash

# Copyright 2021 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o nounset

if [[ $# != 2 ]]; then
	echo "Usage: $0 CaseType BuildOne"
	echo "	CaseType: Options are BVT | FVT | RT | PT"
	echo "	BuildOne: Options are True|False"
	exit 1
fi

CASE_TYPE=$1
MAKE_ONE=$2

source $HOME/.bash_profile
source ./utilities.sh

if [[ -z $CASE_TYPE ]] || [[ -z $MAKE_ONE ]]; then
	echo "The parameter is NULL"
	exit 1
fi

if [[ ! $CASE_TYPE =~ ^(BVT|FVT|RT|PT)$ ]]; then
	echo "CaseType is incorrect"
	exit 1
fi


if [[ ! $MAKE_ONE =~ ^(True|False)$ ]]; then
	echo 'MakeOne should be "True or "False"'
	exit 1
fi

# This is a workaround before publishing matrixone as an open source project
[[ 'X' == "X$REPO_TOKEN" ]] && (echo 'Not found repo token, please export environment variable "export REPO_TOKEN=<YourRepoToken>"'; exit 1)

TESTER_REPO="$G_STAGE/mysql-tester"
TESTER_COMMIT_ID=""
ONE_REPO="$G_STAGE/matrixone"
ONE_COMMIT_ID=""

URL_TESTER_REPO="https://${REPO_TOKEN}@github.com/matrixorigin/mysql-tester.git"
URL_ONE_REPO="https://${REPO_TOKEN}@matrixone.git"

BIN_NAME="mo-server"
CONFIG_NAME="system_vars_config.toml"
CUBE="cube0"
LOG="$G_TS-$CASE_TYPE.log"
SRV_LOG="$G_WKSP/$G_TS-$CASE_TYPE-SRV.log"
SRS_LOG="$G_WKSP/$G_TS-$CASE_TYPE-SRS.log"
TST_LOG="$G_WKSP/$G_TS-$CASE_TYPE-TST.log"
declare -a RAW_RESULT
SUMMARY_RESULT='PASS'

BUILD_WKSP=$(dirname "$PWD") && cd $BUILD_WKSP

if [[ ! -f $TST_LOG ]]; then touch $TST_LOG; fi
if [[ ! -f $SRS_LOG ]]; then touch $SRS_LOG; fi
if [[ ! -f $SRV_LOG ]]; then touch $SRV_LOG; fi

function logger() {
	local level=$1
	local msg=$2
	local log=$LOG
	logger_base "$level" "$msg" "$log"
}

function node_monitor(){
	nohup dstat -tcmdngspy 2 >> "$SRS_LOG" 2>&1 &
}

function stop_service(){
	if ps -ef | grep -v 'grep' | grep $BIN_NAME > /dev/null; then
		logger "INF" "Stopping service"
		pkill $BIN_NAME
	else
		logger "WRN" "$BIN_NAME does not start"
	fi
}

function start_service(){
	local config_file="system_vars_config.toml"
	local check_point="ClusterStatus is ok now"
	local count=0

	if [[ ! -f $G_STAGE/$BIN_NAME ]] || [[ ! -f $G_STAGE/$config_file ]]; then
		logger "ERR" "Not found mo-server binary or config file"
		exit 1
	fi
	
	cd $G_STAGE 
	logger "INF" "Start MatrixOne service"
	nohup ./$BIN_NAME $config_file > $SRV_LOG 2>&1 &

	sleep 2
	if ps -ef | grep -v 'grep' | grep $BIN_NAME >/dev/null 2>&1; then
		logger "INF" "Service process is running"
	else
		logger "ERR" "Service startup fails"
		exit 1
	fi

	while :; do
		if grep "$check_point" $SRV_LOG > /dev/null 2>&1; then
			logger "INF" "Found \"$check_point\" in log"
			break
		else
			logger "INF" "Service is starting..."
		fi
		sleep 1
		if [ $count -ge 120 ]; then
			logger "ERR" "Server startup timed out" 
			exit 1
		fi
		((count=count+1))
	done
	logger "INF" "MatrixOne service is running"
}


function make_tester(){
	logger "INF" "Build mysql-tester"
	cd $G_STAGE && git clone "$URL_TESTER_REPO"
	cd $TESTER_REPO && TESTER_COMMIT_ID=$(git rev-parse HEAD)
	logger "INF" "mysql-tester commit ID: $TESTER_COMMIT_ID"
	logger "INF" "Build mysql-teser"
	make 
}

function make_one(){
	if [[ $MAKE_ONE == 'False' ]]; then
		logger "INF" "Copying MartixOne binary from $BUILD_WKSP"
		cp -f $BUILD_WKSP/{$BIN_NAME,$CONFIG_NAME} $G_STAGE	
	else	
		logger "INF" "Making MatrixOne binary"
		ping -c 5 github.com
		cd $G_STAGE && git clone $URL_ONE_REPO
		cd $ONE_REPO && ONE_COMMIT_ID=$(git rev-parse HEAD)
		logger "INF" "github.com/matrixorigin/matrixone commit ID: $ONE_COMMIT_ID"
		patch_one
		logger "INF" "Build matrixone"
		make clean && make config && make build
		[[ $? != 0 ]] && exit $?
		cp -f {$BIN_NAME,$CONFIG_NAME} $G_STAGE
	fi
}

function patch_one(){
	local current_pwd=$(pwd)
	cd $G_STAGE
	if [[ -f patch_one.sh ]]; then
		logger "INF" "Patch issue fix"
		./patch_one.sh
	else
		logger "INF" "Not found patch"
	fi
	cd $current_pwd
}

function run_bvt(){
	cd $TESTER_REPO
	logger "INF" "BVT is in progress ..."
	logger "INF" "Refer to $TST_LOG for details"
	local host='127.0.0.1'
	local port='6001'
	local user='dump'
	local pswd='111'
	echo './mysql-tester -host 127.0.0.1 -port 6001 -user dump -passwd 111' >> $TST_LOG
	./mysql-tester -host $host -port $port -user $user -passwd $pswd 2>&1 | tee -a $TST_LOG
	if cat $TST_LOG | egrep "Great, All tests passed" >/dev/null 2>&1; then
		logger "INF" "Great, All tests passed"
	else
		logger "ERR" "Not found assertion from tester"
		SUMMARY_RESULT='FAIL'
	fi
	BVT_CASES=($(cat $TST_LOG | egrep "running tests:\ \[.*\]" | awk -F": " '{print $2}' | sed 's/[]["]//g'))
	local raw=""
	for bc in ${BVT_CASES[*]}; do
		logger "INF" "Check $bc test result"
		if cat $TST_LOG | egrep "\-{3}\ PASS $bc" > /dev/null 2>&1; then
			RAW_RESULT+=("$(cat $TST_LOG | egrep "\-{3}\ PASS $bc")")
		else
			RAW_RESULT+=("--- FAIL $bc")
			SUMMARY_RESULT='FAIL'
		fi
	done
	logger "INF" "BVT done"
}

function clean_up(){
	if ps -ef | grep -v 'grep' | grep mo-server >/dev/null 2>&1; then
		logger "INF" "Stop existing mo service"
		pkill mo-server
	fi
	logger "INF" "Delete repositories"
	if [[ -d $TESTER_REPO ]]; then rm -rf $TESTER_REPO; fi
	if [[ -d $ONE_REPO ]]; then rm -rf $ONE_REPO; fi
	logger "INF" "Delete $G_STAGE/$CUBE"
	if [[ -d $G_STAGE/$CUBE ]]; then rm -rf $G_STAGE/$CUBE; fi
	logger "INF" "Delete $G_STAGE/$BIN_NAME"
	if [[ -f $G_STAGE/$BIN_NAME ]]; then rm -f $G_STAGE/$BIN_NAME; fi
	logger "INF" "Delete $G_STAGE/$CONFIG_NAME"
	if [[ -f $G_STAGE/$CONFIG_NAME ]]; then rm -f $G_STAGE/$CONFIG_NAME; fi
}


horiz_rule
echo "#  OS TYPE:	   $G_OSTYPE"
echo "#  CASE TYPE:    $CASE_TYPE"
echo "#  BUILD BINARY: $MAKE_ONE"
echo "#  GO ROOT:	   $GOROOT"
echo "#  GO PATH:	   $GOPATH"
echo "#  SRV LOG PATH: $SRV_LOG"
echo "#  SRS LOG PATH: $SRS_LOG"
echo "#  TST LOG PATH: $TST_LOG"
horiz_rule

clean_up
node_monitor
make_one
make_tester
start_service
run_bvt
clean_up

horiz_rule
echo "# Build Verification Test Summary"
IFS=$'\n'
printf "%s\n" ${RAW_RESULT[@]} | sort -k3
unset IFS
horiz_rule

if [[ 'PASS' != "$SUMMARY_RESULT" ]]; then
	logger "ERR" "BVT FAILED !!!"
	exit 1
else
	logger "INF" "BVT SUCCEEDED !!!"
fi
exit 0
