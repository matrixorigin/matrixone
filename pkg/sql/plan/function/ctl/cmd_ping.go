// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
)

func handlePing() handleFunc {
	return getDNHandlerFunc(
		pb.CmdMethod_Ping,
		func(parameter string) ([]uint64, error) {
			if len(parameter) > 0 {
				id, err := format.ParseStringUint64(parameter)
				if err != nil {
					return nil, err
				}
				return []uint64{id}, nil
			}
			return nil, nil
		},
		func(dnShardID uint64, parameter string, _ *process.Process) ([]byte, error) {
			return protoc.MustMarshal(&pb.DNPingRequest{Parameter: parameter}), nil
		},
		func(data []byte) (interface{}, error) {
			pong := pb.DNPingResponse{}
			protoc.MustUnmarshal(&pong, data)
			return pong, nil
		})
}
