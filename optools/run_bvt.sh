#!/bin/bash
#===============================================================================
#
#          FILE: run_bvt.sh
#   DESCRIPTION: 
#        AUTHOR: Matthew Li, lignay@me.com
#  ORGANIZATION: 
#       CREATED: 09/01/2021
#      REVISION:  ---
#===============================================================================

set -o nounset                                  # Treat unset variables as an error
#set -euxo pipefail

if [[ $# != 2 ]]; then
    echo "Usage: $0 CaseType BuildOne"
    echo "  CaseType: Options are BVT | FVT | RT | PT"
    echo "  BuildOne: Options are True|False"
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

# This is a workaround before publishing matrixone as a open source project
[[ 'X' == "X$REPO_TOKEN" ]] && (echo 'Not found repo token, please export environment variable "export REPO_TOKEN=<YourRepoToken>"'; exit 1)

TESTER_REPO="$G_STAGE/mysql-tester"
TESTER_COMMIT_ID=""
ONE_REPO="$G_STAGE/matrixone"
ONE_COMMIT_ID=""

URL_TESTER_REPO="https://${REPO_TOKEN}@github.com/matrixorigin/mysql-tester.git"
URL_ONE_REPO="https://${REPO_TOKEN}@github.com/matrixorigin/matrixone.git"

BIN_NAME="mo-server"
CONFIG_NAME="system_vars_config.toml"

LOG="$G_TS-$CASE_TYPE.log"
SRV_LOG="$G_WKSP/$G_TS-$CASE_TYPE-SRV.log"
SRS_LOG="$G_WKSP/$G_TS-$CASE_TYPE-SRS.log"
TST_LOG="$G_WKSP/$G_TS-$CASE_TYPE-TST.log"
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
    while :; do
        if ps -ef | grep -v 'grep' | grep $BIN_NAME >/dev/null 2>&1; then
            if grep "$check_point" $SRV_LOG > /dev/null 2>&1; then
                logger "INF" "Found \"$check_point\" in log"
                break
            else
                logger "INF" "Service is starting..."
            fi
        else
            logger "ERR" "Service startup fails"
            exit 1
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
        logger "INF" "Copying MartixOne binary from $SHARE_VOL"
        cp -f $SHARE_VOL/{$BIN_NAME,$CONFIG_NAME} $G_STAGE  
    else    
        logger "INF" "Making MatrixOne binary"
        ping -c 5 github.com
        cd $G_STAGE && git clone $URL_ONE_REPO
        cd $ONE_REPO && ONE_COMMIT_ID=$(git rev-parse HEAD)
        logger "INF" "matrixone commit ID: $ONE_COMMIT_ID"
        patch_one
        logger "INF" "Build matrixone"
        make clean && make config && make build
        [[ $? != 0 ]] && exit $?
        cp -f {$BIN_NAME,$CONFIG_NAME} $G_STAGE
    fi
}

function patch_one(){
    cd $ONE_REPO
    logger  "INF" "Patch fix issue to select max_allowed_packet"
	if [[ ! -f $G_STAGE/patches/max_allowed_packet.txt ]]; then
		logger "ERR" "Not found patch file to fix issue \"select max_allowed_packet\""
		exit 1
	fi
    cp -f pkg/frontend/mysql_cmd_executor.go $G_STAGE/mysql_cmd_executor.go.orig
    sed -i "855 r $G_STAGE/patches/max_allowed_packet.txt" pkg/frontend/mysql_cmd_executor.go
    cp -f pkg/frontend/mysql_cmd_executor.go $G_STAGE/mysql_cmd_executor.go.patched
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
    logger "INF" "BVT done"
}

function clean_up(){
    if ps -ef | grep -v 'grep' | grep mo-server >/dev/null 2>&1; then
        logger "INF" "Stop existing mo service"
        pkill mo-server
    fi
    logger "INF" "Clean up repositories if they exist"
    if [[ -d $TESTER_REPO ]]; then rm -rf $TESTER_REPO; fi
    if [[ -d $ONE_REPO ]]; then rm -rf $ONE_REPO; fi
}


horiz_rule
echo "#  OS TYPE:   $G_OSTYPE"
echo "#  CASE TYPE: $CASE_TYPE"
echo "#  BUILD BIN: $MAKE_ONE"
echo "#  LOG PATH:  $G_WKSP"
echo "#  GO ROOT:   $GOROOT"
echo "#  GO PATH:   $GOPATH"
horiz_rule

clean_up
node_monitor
make_one
make_tester
start_service
run_bvt
clean_up


exit 0
