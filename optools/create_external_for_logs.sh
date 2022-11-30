#!/bin/bash
#PATH=/usr/local/cdb_tools:/sbin:/usr/sbin:/usr/local/sbin:/usr/local/bin:/usr/bin:/bin:$PATH

## default
####################################
version="1"
host=127.0.0.1
port=6001
user=dump
password=111
## want create database
database="test"

## param for s3 data
example_endpoint=s3.us-west-2.amazonaws.com
example_region=us-west-2
example_bucket=bucket_name
example_key=I0AM0AWS0KEY0ID00000
example_secret="0IAM0AWS0SECRET0KEY000000000000000000000"
## as key-prefix in cn.toml
example_prefix="mo-data/etl"

## function
####################################
echo_proxy()
{
    echo "[`date '+%F %T'`] $@"
}

test_exitcode()
{
    local exitcode="$?"
    local self_exitcode=$2
    if [ -z "$self_exitcode" ]; then self_exitcode=$exitcode; fi
    if [ $exitcode -ne 0 ]; then
        echo_proxy "[ERROR] $1"
        exit $self_exitcode
    fi
}

check_param_empty()
{
    local key=$1
    eval val='$'"$key"
    if [ -z "$val" ]; then
        echo "param is empty [$key]"
        exit 1
    fi
}

usage()
{
    cat << EOF
Usage: $0 [options]
Brief:
    help to create external tables, which can access others cluster's log/error info, in new cluster
example:
    $0 -h${host} -P${port} -u${user} -p "${password}" --db "${database}" \\
        --endpoint "$example_endpoint" --region "$example_region" --bucket "$example_bucket" \\
        --key "${example_key}" --secret "$example_secret" --prefix "$example_prefix"
Options:
    -h=str          - MO DB host, default: ${host}
    -P=#            - MO DB port, default: ${port}
    -u=str          - MO DB user, default: ${user}
    -p=str          - MO DB password, default: ${password}
    --db=str        - target database in new cluster, default: ${database}
    --endpoint=str  - S3 endpoint
    --region=str    - S3 region
    --bucket=str    - S3 bucket
    --key=tr        - S3 key_id
    --secret=str    - S3 secret_key
    --prefix=str    - S3 prefix path, same value as CN's 'ETL' fileservice
    --clean         - do drop table in target {db} database
    --show          - show all sql will exec
    --help          - show help msg
EOF
}

## check env, NOT support run in mac
getopt --test
if [ "$?" != "4" ]; then
    echo "Sorry, i am support to run in mac."
    echo "Please install gnu-getopt for Mac."
    exit 1
fi


#ARGS=`getopt -o d::,w::,t:: -l way::,type::,start_log_file::,start_log_pos::,end_log_file::,end_log_pos:: -- "$@"`
## PS: diff between v:: and v:
##     v:: request no space bewteen key and val, for example: -v5.6
##     version:: request '=' between key val
##     verions:  request '=' or ' ', between key and val, both is ok
ARGS=`getopt -o h:,P:,u:,p: -l help,show,clean,db:,endpoint:,region:,bucket:,key:,secret:,prefix: -- "$@"`
test_exitcode "parse cmd-args failed" 1

eval set -- "${ARGS}"
test_exitcode "parse cmd-args failed" 1

while true
do
    case "$1" in
        -h)
            host=$2;    shift 2;;
        -P)
            port=$2;    shift 2;;
        -u)
            user=$2;    shift 2;;
        -p)
            password=$2;  shift 2;;
        --db)
            database=$2;    shift 2;;
        --endpoint)
            endpoint=$2;    shift 2;;
        --region)
            region=$2;  shift 2;;
        --bucket)
            bucket=$2;  shift 2;;
        --key)
            key=$2;     shift 2;;
        --secret)
            secret=$2;  shift 2;;
        --prefix)
            prefix=$2;  shift 2;;
        --clean)
            clean=1;    shift 1;;
       --show)
            show=1;    shift 1;;
        --help)
            usage
            exit 1
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Unkown args: $1"
            exit 1
            ;;
    esac
done


## verify config ##
###################
check_param_empty host
check_param_empty port
check_param_empty user
check_param_empty database
check_param_empty endpoint
check_param_empty region
check_param_empty bucket
check_param_empty key
check_param_empty secret

## main logic ##
################
mod="mysql -A -h ${host} -P ${port} -u${user}"
if [ ! -z "$password" ]; then
    mod="mysql -A -h ${host} -P ${port} -u${user} -p${password}"
fi

## check db access
check_access() {
    echo_proxy "try access by cmd: ${mod}"
    echo "select now()" | $mod
    test_exitcode "failed to access mo."
}

drop_table() {
    echo_proxy "drop table ..."
cat << EOF | $mod
drop TABLE IF EXISTS \`$database\`.\`statement_info\`;
drop TABLE IF EXISTS \`$database\`.\`rawlog\`;
drop VIEW  IF EXISTS \`$database\`.\`log_info\`;
drop VIEW  IF EXISTS \`$database\`.\`error_info\`;
drop VIEW  IF EXISTS \`$database\`.\`span_info\`;
EOF
    test_exitcode "failed to drop tables"
}

create_statement() {
    echo_proxy "create statement_info ..."
    cat << EOF | $mod
create database if not exists $database;
use $database;
CREATE EXTERNAL TABLE IF NOT EXISTS \`statement_info\`(
  \`statement_id\` varchar(36) DEFAULT "0" COMMENT "statement uniq id",
  \`transaction_id\` varchar(36) DEFAULT "0" COMMENT "txn uniq id",
  \`session_id\` varchar(36) DEFAULT "0" COMMENT "session uniq id",
  \`account\` varchar(1024) NOT NULL COMMENT "account name",
  \`user\` varchar(1024) NOT NULL COMMENT "user name",
  \`host\` varchar(1024) NOT NULL COMMENT "user client ip",
  \`database\` varchar(1024) NOT NULL COMMENT "what database current session stay in.",
  \`statement\` TEXT NOT NULL COMMENT "sql statement",
  \`statement_tag\` TEXT NOT NULL COMMENT "note tag in statement(Reserved)",
  \`statement_fingerprint\` TEXT NOT NULL COMMENT "note tag in statement(Reserved)",
  \`node_uuid\` varchar(36) DEFAULT "0" COMMENT "node uuid, which node gen this data.",
  \`node_type\` varchar(64) DEFAULT "node" COMMENT "node type in MO, val in [DN, CN, LOG]",
  \`request_at\` datetime(6) NOT NULL COMMENT "request accept datetime",
  \`response_at\` datetime(6) NOT NULL COMMENT "response send datetime",
  \`duration\` bigint unsigned DEFAULT "0" COMMENT "exec time, unit: ns",
  \`status\` varchar(32) DEFAULT "Running" COMMENT "sql statement running, enum: Running, Success, Failed",
  \`err_code\` varchar(1024) DEFAULT "0" COMMENT "",
  \`error\` TEXT NOT NULL COMMENT "error message",
  \`exec_plan\` JSON NOT NULL COMMENT "statement execution plan",
  \`rows_read\` bigint unsigned DEFAULT "0" COMMENT "rows read total",
  \`bytes_scan\` bigint unsigned DEFAULT "0" COMMENT "bytes scan total"
) url s3option { "endpoint"="$endpoint", "access_key_id"='$key', "secret_access_key"='$secret', "region"="$region", "bucket"="$bucket", "filepath" = "${prefix}/*/*/*/*/*/statement_info/*.csv", "compression" = "none" } FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines
EOF
    test_exitcode "failed to create table statement_info"
    echo_proxy "create statement_info done."
}

create_rawlog() {
    echo_proxy "create rawlog ..."
    cat << EOF | $mod
create database if not exists $database;
use $database;
CREATE EXTERNAL TABLE IF NOT EXISTS \`rawlog\`(
  \`raw_item\` varchar(1024) NOT NULL COMMENT "raw log item",
  \`node_uuid\` varchar(36) DEFAULT "0" COMMENT "node uuid, which node gen this data.",
  \`node_type\` varchar(64) DEFAULT "node" COMMENT "node type in MO, val in [DN, CN, LOG]",
  \`span_id\` varchar(16) DEFAULT "0" COMMENT "span uniq id",
  \`trace_id\` varchar(36) DEFAULT "0" COMMENT "trace uniq id",
  \`logger_name\` varchar(1024) NOT NULL COMMENT "logger name",
  \`timestamp\` datetime(6) NOT NULL COMMENT "timestamp of action",
  \`level\` varchar(1024) NOT NULL COMMENT "log level, enum: debug, info, warn, error, panic, fatal",
  \`caller\` varchar(1024) NOT NULL COMMENT "where it log, like: package/file.go:123",
  \`message\` TEXT NOT NULL COMMENT "log message",
  \`extra\` JSON NOT NULL COMMENT "log dynamic fields",
  \`err_code\` varchar(1024) DEFAULT "0" COMMENT "",
  \`error\` TEXT NOT NULL COMMENT "error message",
  \`stack\` varchar(4096) NOT NULL COMMENT "",
  \`span_name\` varchar(1024) NOT NULL COMMENT "span name, for example: step name of execution plan, function name in code, ...",
  \`parent_span_id\` varchar(16) DEFAULT "0" COMMENT "parent span uniq id",
  \`start_time\` datetime(6) NOT NULL COMMENT "",
  \`end_time\` datetime(6) NOT NULL COMMENT "",
  \`duration\` bigint unsigned DEFAULT "0" COMMENT "exec time, unit: ns",
  \`resource\` JSON NOT NULL COMMENT "static resource information",
  \`span_kind\` varchar(32) NOT NULL COMMENT "span kind, enum: internal, statement, remote"
) url s3option { "endpoint"="$endpoint", "access_key_id"='$key', "secret_access_key"='$secret', "region"="$region", "bucket"="$bucket", "filepath" = "$prefix/*/*/*/*/*/rawlog/*.csv", "compression" = "none" } FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines
EOF
    test_exitcode "failed to create table rawlog"
    echo_proxy "create rawlog done."
}



create_view(){
    echo_proxy "create view ..."
    cat << EOF | $mod
USE $database;
CREATE VIEW IF NOT EXISTS \`log_info\` as select \`trace_id\`, \`span_id\`, \`span_kind\`, \`node_uuid\`, \`node_type\`, \`timestamp\`, \`logger_name\`, \`level\`, \`caller\`, \`message\`, \`extra\`, \`stack\` from \`system\`.\`rawlog\` where \`raw_item\` = "log_info";
CREATE VIEW IF NOT EXISTS \`error_info\` as select \`timestamp\`, \`err_code\`, \`error\`, \`trace_id\`, \`span_id\`, \`span_kind\`, \`node_uuid\`, \`node_type\`, \`stack\` from \`system\`.\`rawlog\` where \`raw_item\` = "error_info";
CREATE VIEW IF NOT EXISTS \`span_info\` as select \`trace_id\`, \`span_id\`, \`parent_span_id\`, \`span_kind\`, \`node_uuid\`, \`node_type\`, \`span_name\`, \`start_time\`, \`end_time\`, \`duration\`, \`resource\` from \`system\`.\`rawlog\` where \`raw_item\` = "span_info";
EOF
    test_exitcode "failed to create view"
    echo_proxy "create view Done."
}

## main ########
################

if [ ! -z "$show" ]; then
    mod="cat"
fi

check_access

if [ ! -z "$clean" ]; then
    drop_table
fi
create_statement
create_rawlog
create_view

echo_proxy "done."
