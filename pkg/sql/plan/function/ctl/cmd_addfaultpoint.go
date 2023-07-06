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
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strconv"
	"strings"
)

func handleAddFaultPoint() handleFunc {
	return getDNHandlerFunc(
		pb.CmdMethod_AddFaultPoint,
		func(_ string) ([]uint64, error) {
			return nil, nil
		},
		func(dnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			// parameter like "name.freq.action.iarg.sarg"
			parameters := strings.Split(parameter, ".")
			if len(parameters) != 5 {
				return nil, moerr.NewInternalError(proc.Ctx, "handleAddFaultPoint: invalid argument!")
			}
			name := parameters[0]
			if name == "" {
				return nil, moerr.NewInternalError(proc.Ctx, "handleAddFaultPoint: fault point's name should not be empty!")
			}
			freq := parameters[1]
			action := parameters[2]
			iarg, err := strconv.Atoi(parameters[3])
			if err != nil {
				return nil, moerr.NewInternalError(proc.Ctx, "handleAddFaultPoint: %s", err.Error())
			}
			sarg := parameters[4]
			payload, err := types.Encode(&db.FaultPoint{
				Name:   name,
				Freq:   freq,
				Action: action,
				Iarg:   int64(iarg),
				Sarg:   sarg,
			})
			if err != nil {
				return nil, moerr.NewInternalError(proc.Ctx, "payload encode err")
			}
			return payload, nil
		},
		func(data []byte) (interface{}, error) {
			resp := pb.DNStringResponse{}
			protoc.MustUnmarshal(&resp, data)
			return resp, nil
		})
}
