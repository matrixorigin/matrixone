# kitex-gen
rm -rf ./kitex_gen && mkdir ./kitex_gen
kitex -type protobuf -module github.com/matrixorigin/matrixone ./message.proto
