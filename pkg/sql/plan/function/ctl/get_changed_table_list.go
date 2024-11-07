package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleGetChangedTableList() handleFunc {
	return GetTNHandlerFunc(
		api.OpCode_OpInspect,
		func(string) ([]uint64, error) { return nil, nil },
		func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			return types.Encode(&cmd_util.InspectTN{
				AccessInfo: cmd_util.AccessInfo{
					AccountID: proc.GetSessionInfo().AccountId,
					UserID:    proc.GetSessionInfo().UserId,
					RoleID:    proc.GetSessionInfo().RoleId,
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
