#!/bin/bash
#
# Generate all matrixcube protobuf bindings.
# Run from repository root.
#
set -e

PB_DIR="$PWD/pkg/pb"
PROTOC_DIR="$PWD/proto"

mkdir -p $PB_DIR/plan
protoc --proto_path=$PROTOC_DIR --go_out=paths=source_relative:$PB_DIR/plan  $PROTOC_DIR/plan.proto 
protoc --proto_path=$PROTOC_DIR --go_out=paths=source_relative:$PB_DIR/metric  $PROTOC_DIR/metric.proto 
