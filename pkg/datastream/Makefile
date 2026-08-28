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

# Regenerates pkg/datastream/v1 from proto/datastream/v1/datastream.proto.
# Requires protoc, protoc-gen-go (v1.36.x), protoc-gen-go-grpc (v1.5.x):
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
# The same .proto is compiled for Java by xtool/jstfu's Maven build.

.PHONY: generate clean

generate:
	protoc -I ../../proto/datastream/v1 \
		--go_out=./v1 --go_opt=paths=source_relative \
		--go-grpc_out=./v1 --go-grpc_opt=paths=source_relative \
		../../proto/datastream/v1/datastream.proto

clean:
	rm -f v1/datastream.pb.go v1/datastream_grpc.pb.go
