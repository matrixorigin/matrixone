// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

func IsValidArg(parameter string, proc *process.Process) (*cmd_util.DiskCleaner, error) {
	parameters := strings.Split(parameter, ".")
	if len(parameters) > 3 || len(parameters) < 1 {
		return nil, moerr.NewInternalError(proc.Ctx, "handleDiskCleaner: invalid argument!")
	}
	op := parameters[0]
	switch op {
	case cmd_util.AddChecker, cmd_util.RemoveChecker:
		break
	case cmd_util.StopGC, cmd_util.StartGC:
		return &cmd_util.DiskCleaner{
			Op: op,
		}, nil
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "handleDiskCleaner: invalid operation!")
	}
	key := parameters[1]
	switch key {
	case cmd_util.CheckerKeyTTL, cmd_util.CheckerKeyMinTS:
		break
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "handleDiskCleaner: invalid key!")
	}
	return &cmd_util.DiskCleaner{
		Op:    op,
		Key:   key,
		Value: parameters[2],
	}, nil
}

func handleDiskCleaner() handleFunc {
	return GetTNHandlerFunc(
		api.OpCode_OpDiskDiskCleaner,
		func(string) ([]uint64, error) { return nil, nil },
		func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			// parameter like "name.freq.action.iarg.sarg"
			diskcleaner, err := IsValidArg(parameter, proc)
			if err != nil {
				return nil, err
			}
			payload, err := types.Encode(diskcleaner)
			if err != nil {
				return nil, moerr.NewInternalError(proc.Ctx, "payload encode err")
			}
			return payload, nil
		},
		func(data []byte) (any, error) {
			resp := api.TNStringResponse{
				ReturnStr: string(data),
			}
			protoc.MustUnmarshal(&resp, data)
			return resp, nil
		})
}
