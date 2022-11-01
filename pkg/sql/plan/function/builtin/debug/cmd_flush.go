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
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/debug"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strconv"
	"strings"
)

func handleFlush() handleFunc {
	return getDNHandlerFunc(
		pb.CmdMethod_Flush,
		func(_ string) ([]uint64, error) {
			return nil, nil
		},
		func(dnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			// parameter should be "DbName.TableName"
			parameters := strings.Split(parameter, ".")
			txnOp := proc.TxnOperator
			if proc.TxnOperator == nil {
				v, err := proc.TxnClient.New()
				if err != nil {
					return nil, err
				}
				txnOp = v
				if err = proc.SessionInfo.StorageEngine.New(proc.Ctx, txnOp); err != nil {
					return nil, err
				}
			}
			database, err := proc.SessionInfo.StorageEngine.Database(proc.Ctx, parameters[0], txnOp)
			if err != nil {
				return nil, err
			}
			rel, err := database.Relation(proc.Ctx, parameters[1])
			if err != nil {
				return nil, err
			}
			dId := database.GetDatabaseId(proc.Ctx)
			tId := rel.GetTableID(proc.Ctx)
			err = txnOp.Commit(proc.Ctx)
			if err != nil {
				txnOp = nil
				return nil, err
			}
			dbId, err := strconv.Atoi(dId)
			if err != nil {
				return nil, err
			}
			tableId, err := strconv.Atoi(tId)
			if err != nil {
				return nil, err
			}
			payload, err := types.Encode(db.FlushTable{
				DatabaseID: uint64(dbId),
				TableID:    uint64(tableId),
				AccessInfo: db.AccessInfo{
					AccountID: proc.SessionInfo.AccountId,
					UserID:    proc.SessionInfo.UserId,
					RoleID:    proc.SessionInfo.RoleId,
				},
			})
			if err != nil {
				return nil, moerr.NewInternalError("payload encode err")
			}
			return payload, nil
		},
		func(data []byte) (interface{}, error) {
			resp := pb.DNStringResponse{}
			protoc.MustUnmarshal(&resp, data)
			return resp, nil
		})
}
