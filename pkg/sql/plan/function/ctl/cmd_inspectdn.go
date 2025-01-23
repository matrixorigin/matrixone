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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleInspectTN() handleFunc {
	return GetTNHandlerFunc(
		api.OpCode_OpInspect,
		func(string) ([]uint64, error) { return nil, nil },
		func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			//---------------------------------------------------------------------------
			topContext := proc.GetTopContext()
			accountId, err := defines.GetAccountId(topContext)
			if err != nil {
				return nil, err
			}
			useId := defines.GetUserId(topContext)
			roleId := defines.GetRoleId(topContext)
			//---------------------------------------------------------------------------

			return types.Encode(&cmd_util.InspectTN{
				AccessInfo: cmd_util.AccessInfo{
					AccountID: accountId,
					UserID:    useId,
					RoleID:    roleId,
				},
				Operation: parameter,
			})
		},
		func(data []byte) (any, error) {
			resp := &cmd_util.InspectResp{}
			types.Decode(data, resp)
			return resp, nil
		})
}
