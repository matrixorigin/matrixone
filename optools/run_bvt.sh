#!/bin/bash

###################################################################
# Title	 : run_bvt.sh
# Desc.  : Executing build verification testing.
# Usage  : run_bvt.sh
# Author : Matthew Li (lignay@me.com)
###################################################################

MO_HOME="$HOME/matrix-origin/"
BUILD_ROOT="$(pwd)/../"
BIN_NAME="mo-server"
SERVER_LOG="$MO_HOME/$BIN_NAME.log"

if [[ ! -f $BUILD_ROOT/$BIN_NAME ]]; then
  echo "Not found mo-server binary"
  exit 1
fi

if [[ ! -d $MO_HOME ]]; then
  mkdir $MO_HOME
  chmod -R 755 $MO_HOME
fi

function stop_service(){
  if ps -ef | grep $BIN_NAME > /dev/null; then
    echo "Stopping service"
    pkill $BIN_NAME
  else
    echo "$BIN_NAME does not start"
  fi
}

function start_service(){
  local config_file="system_vars_config.toml"
  stop_service
  echo "Starting $BIN_NAME"
  cd $MO_HOME
  cp -f $BUILD_ROOT/{$BIN_NAME,$config_file} ./
  nohup ./$BIN_NAME $config_file > $SERVER_LOG 2>&1 &
  local check_point="ClusterStatus is ok now"
  local count=0
  while :; do
    if grep "$check_point" $SERVER_LOG > /dev/null 2>&1; then
      echo "Found \"$check_point\" in $SERVER_LOG"
      stop_service
      echo "BVT PASSED"
      exit 0
    fi
      sleep 2
    if (( $count > 60 )); then
      echo "BVT FAILED"
      echo "Server start timed out. Terminating BVT ..."
      return 1
    fi
    (( count ++ ))
  done
}

function run_tests(){
  echo "Running test cases"
}

start_service

exit 0
