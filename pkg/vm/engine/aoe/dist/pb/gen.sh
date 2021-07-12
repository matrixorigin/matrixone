#!/bin/bash
#
# Generate all beehive protobuf bindings.
# Run from repository root.
#
set -e

# directories containing protos to be built
DIRS="./metapb ./rpcpb"

PRJ_PB_PATH="${GOPATH}/src/github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/dist/pb"
# work_path=$(dirname $0)
# cd ~/${work_path}

for dir in ${DIRS}; do
	pushd ${dir}
		protoc  -I=.:"${PRJ_PB_PATH}":"${GOPATH}/src" --gogofaster_out=plugins=grpc:.  *.proto
		goimports -w *.pb.go
	popd
done
