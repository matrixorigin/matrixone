#!/bin/bash
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Starts the jstfu datastream server for the datastream BVT cases
# (test/distributed/cases/datastream).  Build the jar first with `make jstfu`.
#
#   usage: optools/jstfu_bvt.sh [resources_dir] [mo_host:mo_port]
#
# resources_dir defaults to test/distributed/resources (must contain
# datastream/numbers.csv); mo_host:mo_port is where the jdbc datasource dials
# back into MatrixOne (default 127.0.0.1:6001).  Listens on port 4444, the
# port the BVT cases hardcode.  Stop with: pkill -f 'jstfu.jar /tmp/jstfu-bvt'

set -e

cd "$(dirname "$0")/.."

RESOURCES="${1:-$PWD/test/distributed/resources}"
MO_ADDR="${2:-127.0.0.1:6001}"
RESOURCES="$(cd "$RESOURCES" && pwd)"
JAR="$PWD/xtool/jstfu/target/jstfu.jar"

if [ ! -f "$JAR" ]; then
    echo "error: $JAR not found - run 'make jstfu' first" >&2
    exit 1
fi
if [ ! -f "$RESOURCES/datastream/numbers.csv" ]; then
    echo "error: $RESOURCES/datastream/numbers.csv not found" >&2
    exit 1
fi

CONFIG=/tmp/jstfu-bvt.json
# chunksize 4096 (vs the 1MB default) forces the larger fixtures to stream as
# hundreds of chunks, so the BVT exercises multi-chunk record-boundary
# handling end to end, not just single-chunk responses.
cat > "$CONFIG" <<EOF
{
    "port": 4444,
    "chunksize": 4096,
    "datasource": [
        { "name": "file_numbers", "type": "file",
          "path": "$RESOURCES/datastream/numbers.csv" },
        { "name": "bad_file", "type": "file",
          "path": "/nonexistent/jstfu/bvt.csv" },
        { "name": "jdbc_numbers", "type": "jdbc",
          "connectionstring": "jdbc:mysql://$MO_ADDR?useSSL=false&allowPublicKeyRetrieval=true",
          "user": "dump", "password": "111",
          "sql": "select col1, col2, col3, col4 from datastream_bvt.src_numbers where \${FILTER}" },
        { "name": "jdbc_bad_sql", "type": "jdbc",
          "connectionstring": "jdbc:mysql://$MO_ADDR?useSSL=false&allowPublicKeyRetrieval=true",
          "user": "dump", "password": "111",
          "sql": "select * from datastream_bvt.no_such_table where \${FILTER}" },
        { "name": "jdbc_broken_conn", "type": "jdbc",
          "connectionstring": "jdbc:mysql://127.0.0.1:1?useSSL=false&connectTimeout=1000",
          "user": "dump", "password": "111",
          "sql": "select 1 where \${FILTER}" },
        { "name": "file_parallel", "type": "file",
          "path": "$RESOURCES/load_data/test_parallel.csv" },
        { "name": "file_rawlog", "type": "file",
          "path": "$RESOURCES/external_table_file/rawlog_withnull.csv" },
        { "name": "jdbc_parallel", "type": "jdbc",
          "connectionstring": "jdbc:mysql://$MO_ADDR?useSSL=false&allowPublicKeyRetrieval=true",
          "user": "dump", "password": "111",
          "sql": "select col1, col2, col3 from datastream_bvt2.t_parallel where \${FILTER}" },
        { "name": "jdbc_rawlog", "type": "jdbc",
          "connectionstring": "jdbc:mysql://$MO_ADDR?useSSL=false&allowPublicKeyRetrieval=true",
          "user": "dump", "password": "111",
          "sql": "select raw_item, node_uuid, node_type, span_id, statement_id, logger_name, \`timestamp\`, \`level\`, caller, message, extra, err_code, error, stack, span_name, parent_span_id, start_time, end_time, duration, resource from datastream_bvt2.t_rawlog where \${FILTER}" }
    ]
}
EOF

echo "starting jstfu on :4444 (resources=$RESOURCES, mo=$MO_ADDR)"
exec java -jar "$JAR" "$CONFIG"
