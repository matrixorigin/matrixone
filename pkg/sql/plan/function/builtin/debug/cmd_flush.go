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
				return pb.DebugResult{Data: data}, nil
			}
			return pb.DebugResult{Data: "true"}, nil
		})
}
