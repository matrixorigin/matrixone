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

package debug

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/debug"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

func handleFlush() handleFunc {
	return getDNHandlerFunc(
		pb.CmdMethod_Flush,
		func(_ string) ([]uint64, error) {
			return nil, nil
		},
		func(dnShardID uint64, parameter string, proc *process.Process) []byte {
			// parameter should be "DbName@TableName"
			parameters := strings.Split(parameter, "@")
			payload, err := types.Encode(db.FlushTable{
				DatabaseName: parameters[0],
				TableName:    parameters[1],
				AccessInfo: db.AccessInfo{
					AccountID: proc.SessionInfo.AccountId,
					UserID:    proc.SessionInfo.UserId,
					RoleID:    proc.SessionInfo.RoleId,
				},
			})
			if err != nil {
				panic(any(moerr.NewInternalError("payload encode err")))
			}
			return payload
		},
		func(data []byte) (interface{}, error) {
			if data != nil {
				return pb.DebugResult{Method: pb.CmdMethod_Flush.String(), Data: data}, nil
			}
			return pb.DebugResult{Method: pb.CmdMethod_Flush.String(), Data: "succeed"}, nil
		})
}
