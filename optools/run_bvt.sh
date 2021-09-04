#!/bin/bash

#set -euxo pipefail
set -eo pipefail

if [[ $# != 2 ]]; then
	echo "Usage: $0 CaseType BuildOne"
	echo "  CaseType: Options are BVT | FVT | RT | PT"
	echo "  BuildOne: Options are True|False"
	exit 1
fi

CASE_TYPE=$1
MAKE_ONE=$2

source $HOME/.bash_profile

function mlog() {
    local level=$1
    local msg=$2
    if [[ $# != 2 ]]; then
        echo "Wrong parameters passed to mlog"
        exit 1
    fi
    local fn_stack=(${FUNCNAME[*]})
    local fn_name=""
    if [[ ${#fn_stack[*]} == 2 ]] && [[ 'mlog' == ${fn_stack[0]} ]]; then
        fn_name=${fn_stack[1]}
    else
        fn_name="${fn_stack[@]:0-2:1}"
    fi
  
    case $level in
        "ERR") echo -e "[$(date +"%Y-%m-%d %H:%M:%S.%N %z" | cut -b 1-35)] [$fn_name] \e[1;31m ERROR $msg\e[0m" | tee -a $LOG;;
        "WRN") echo -e "[$(date +"%Y-%m-%d %H:%M:%S.%N %z" | cut -b 1-35)] [$fn_name] \e[1;43m WARNING $msg\e[0m" | tee -a $LOG;;
        "INF") echo -e "[$(date +"%Y-%m-%d %H:%M:%S.%N %z" | cut -b 1-35)] [$fn_name] INFO $msg" | tee -a $LOG;;
        "RTN") echo -e "[$(date +"%Y-%m-%d %H:%M:%S.%N %z" | cut -b 1-35)] [$fn_name] RTN $msg" | tee -a $LOG;;
        *) echo "Msg level is incorrect"; exit 1;;
    esac
}

function node_monitor(){
    nohup dstat -tcmdngspy 2 >> "$SRS_LOG" 2>&1 &
}

function stop_service(){
    if ps -ef | grep -v 'grep' | grep $BIN_NAME > /dev/null; then
		mlog "INF" "Stopping service"
		pkill $BIN_NAME
    else
		mlog "WRN" "$BIN_NAME does not start"
    fi
}

function start_service(){
    local config_file="system_vars_config.toml"
    local check_point="ClusterStatus is ok now"
    local count=0

	if [[ ! -f $TST_STAGE/$BIN_NAME ]] || [[ ! -f $TST_STAGE/$config_file ]]; then
	    mlog "ERR" "Not found mo-server binary or config file"
	    exit 1
	fi
	
	cd $TST_STAGE 
    mlog "INF" "Start MatrixOne service"
    nohup ./$BIN_NAME $config_file > $SRV_LOG 2>&1 &

	sleep 2
    while :; do
	    if ps -ef | grep -v 'grep' | grep $BIN_NAME >/dev/null 2>&1; then
            if grep "$check_point" $SRV_LOG > /dev/null 2>&1; then
                mlog "INF" "Found \"$check_point\" in log"
				break
			else
				mlog "INF" "Service is starting..."
            fi
		else
			mlog "ERR" "Service startup fails"
			exit 1
		fi
        sleep 1
        if [ $count -ge 120 ]; then
            mlog "ERR" "Server startup timed out" 
            exit 1
        fi
        ((count=count+1))
    done
	mlog "INF" "MatrixOne service is running"
}


function make_tester(){
	mlog "INF" "Build mysql-tester"
	cd $TST_STAGE && git clone "$URL_TESTER_REPO"
	cd $TESTER_REPO && TESTER_COMMIT_ID=$(git rev-parse HEAD)
	mlog "INF" "mysql-tester commit ID: $TESTER_COMMIT_ID"
	mlog "INF" "Build mysql-teser"
	make 
}

function make_one(){
	if [[ $MAKE_ONE == 'False' ]]; then
		mlog "INF" "Copying MartixOne binary from $SHARE_VOL"
	    cp -f $SHARE_VOL/{$BIN_NAME,$CONFIG_NAME} $TST_STAGE	
	else	
		mlog "INF" "Making MatrixOne binary"
		ping -c 5 github.com
		cd $TST_STAGE && git clone $URL_ONE_REPO
		cd $ONE_REPO && ONE_COMMIT_ID=$(git rev-parse HEAD)
		mlog "INF" "matrixone commit ID: $ONE_COMMIT_ID"
		patch_one
		mlog "INF" "Build matrixone"
		make clean && make config && make build
		[[ $? != 0 ]] && exit $?
		cp -f {$BIN_NAME,$CONFIG_NAME} $TST_STAGE
	fi
}

function patch_one(){
	cd $ONE_REPO
	mlog  "INF" "Patch fix to select max_allowed_packet"
	cp -f pkg/frontend/mysql_cmd_executor.go $TST_STAGE/mysql_cmd_executor.go.bkp
	sed -i '855 r max_allowed_packet.txt' pkg/frontend/mysql_cmd_executor.go
}

function run_bvt(){
	cd $TESTER_REPO
	mlog "INF" "BVT is in progress ..."
	mlog "INF" "Refer to $TST_LOG for details"
	local host='127.0.0.1'
	local port='6001'
	local user='dump'
	local pswd='111'
	echo './mysql-tester -host 127.0.0.1 -port 6001 -user dump -passwd 111' >> $TST_LOG
	./mysql-tester -host $host -port $port -user $user -passwd $pswd 2>&1 | tee -a $TST_LOG
	mlog "INF" "BVT done"
}


function clean_up(){
	if ps -ef | grep -v 'grep' | grep mo-server >/dev/null 2>&1; then
		mlog "INF" "Stop existing mo service"
		pkill mo-server
	fi
	mlog "INF" "Clean up repositories if they exist"
	if [[ -d $TESTER_REPO ]]; then rm -rf $TESTER_REPO; fi
	if [[ -d $ONE_REPO ]]; then rm -rf $ONE_REPO; fi
}


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

# This is a workaround before publishing matrixone as a open source project
[[ -z $REPO_TOKEN ]] && (echo 'Not found repo token, please export environment variable "export REPO_TOKEN=<YourRepoToken>"'; exit 1)

G_TS=`date +%Y%m%d%H%M%S`

TST_STAGE="$HOME/scratch"
TST_VOL="$TST_STAGE/test-data"
SHARE_VOL="$TST_STAGE/share-data"

TESTER_REPO="$TST_STAGE/mysql-tester"
TESTER_COMMIT_ID=""
ONE_REPO="$TST_STAGE/matrixone"
ONE_COMMIT_ID=""

LOG="$TST_VOL/$CASE_TYPE-$G_TS.log"
SRV_LOG="$TST_VOL/$CASE_TYPE-srv-$G_TS.log"
SRS_LOG="$TST_VOL/$CASE_TYPE-srs-$G_TS.log"
TST_LOG="$TST_VOL/$CASE_TYPE-tst-$G_TS.log"

URL_TESTER_REPO="https://${REPO_TOKEN}@github.com/matrixorigin/mysql-tester.git"
URL_ONE_REPO="https://${REPO_TOKEN}@github.com/matrixorigin/matrixone.git"


BIN_NAME="mo-server"
CONFIG_NAME="system_vars_config.toml"

[[ -d $TST_STAGE ]] || mkdir $TST_STAGE
[[ -d $TST_VOL ]] || mkdir $TST_VOL
[[ -d $SHARE_VOL ]] || mkdir $SHARE_VOL

[[ -f $LOG ]] || touch $LOG
[[ -f $TST_LOG ]] || touch $TST_LOG
[[ -f $SRS_LOG ]] || touch $SRS_LOG
[[ -f $SRV_LOG ]] || touch $SRV_LOG

mlog "INF" $(seq -s '#' 50|tr -d '[:digit:]')
mlog "INF" "#  Case Type: $CASE_TYPE"
mlog "INF" "#  Build matrixone: $MAKE_ONE"
mlog "INF" "#  GOROOT: $GOROOT"
mlog "INF" "#  GOPATH: $GOPATH"
mlog "INF" $(seq -s '#' 50|tr -d '[:digit:]')

clean_up
node_monitor
make_one
make_tester
start_service
run_bvt
clean_up


exit 0
