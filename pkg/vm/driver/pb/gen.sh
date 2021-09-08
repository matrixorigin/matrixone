#!/bin/bash
#
# Generate all beehive protobuf bindings.
# Run from repository root.
#
set -e

PRJ_PB_PATH=$(cd "$(dirname "$0")";pwd)
cd "${PRJ_PB_PATH}"

protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogo_out=. meta.proto
protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogo_out=. rpc.proto
