#!/bin/bash
#
# Generate all matrixcube protobuf bindings.
# Run from repository root.
#
set -e

VENDOR_DIR="$PWD/vendor"
PB_DIR="$PWD/pkg/pb"
PROTOC_DIR="$PWD/proto"
for file in `ls $PROTOC_DIR/*.proto`
do
	dir=$(basename $file .proto)
	mkdir -p $PB_DIR/$dir
	protoc -I=.:$PROTOC_DIR:$VENDOR_DIR --gogofast_out=paths=source_relative:./pkg/pb/$dir  $file
    goimports -w $PB_DIR/$dir/*pb.go
done
