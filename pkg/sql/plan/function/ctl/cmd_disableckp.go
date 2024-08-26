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
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleDisableCheckpoint() handleFunc {
	return GetTNHandlerFunc(
		api.OpCode_OpDisableCheckpoint,
		func(string) ([]uint64, error) { return nil, nil },
		func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			var enable bool
			switch parameter {
			case "enable":
				enable = true
			case "disable":
				enable = false
			default:
				return nil, moerr.NewInternalErrorNoCtxf("invalid parameter %v", parameter)
			}
			return types.Encode(&db.Checkpoint{Enable: enable})
		},
		func(data []byte) (any, error) {
			resp := api.TNStringResponse{}
			protoc.MustUnmarshal(&resp, data)
			return resp, nil
		},
	)
}
