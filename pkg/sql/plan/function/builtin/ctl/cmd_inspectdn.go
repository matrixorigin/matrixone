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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleInspectDN() handleFunc {
	return getDNHandlerFunc(
		pb.CmdMethod_Inspect,
		func(_ string) ([]uint64, error) {
			return nil, nil
		},
		func(dnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			return types.Encode(&db.InspectDN{
				AccessInfo: db.AccessInfo{
					AccountID: proc.SessionInfo.AccountId,
					UserID:    proc.SessionInfo.UserId,
					RoleID:    proc.SessionInfo.RoleId,
				},
				Operation: parameter,
			})
		},
		func(data []byte) (interface{}, error) {
			resp := &db.InspectResp{}
			types.Decode(data, resp)
			return resp.GetResponse(), nil
		})
}
