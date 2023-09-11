# Copyright 2023 Matrix Origin
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

.PHONY: clean
clean:
	rm -f udf.pb.go
	rm -f udf_grpc.pb.go
	rm -f pythonservice/pyserver/python_udf_pb2.py
	rm -f pythonservice/pyserver/python_udf_pb2.pyi
	rm -f pythonservice/pyserver/python_udf_pb2_grpc.py

.PHONY: generate
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./udf.proto
	@cd pythonservice/pyserver && python -m grpc_tools.protoc -I ./ --python_out=. --pyi_out=. --grpc_python_out=. ./udf.proto
