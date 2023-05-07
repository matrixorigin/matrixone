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
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleCheckpoint() handleFunc {
	return getDNHandlerFunc(
		pb.CmdMethod_Checkpoint,
		func(_ string) ([]uint64, error) {
			return nil, nil
		},
		func(dnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			flushDuration := time.Duration(0)
			var err error
			if parameter != "" {
				flushDuration, err = time.ParseDuration(parameter)
				if err != nil {
					return nil, err
				}
			}
			payload, err := types.Encode((&db.Checkpoint{
				FlushDuration: flushDuration,
			}))
			return payload, err
		},
		func(data []byte) (interface{}, error) {
			resp := pb.DNStringResponse{}
			protoc.MustUnmarshal(&resp, data)
			return resp, nil
		})
}
