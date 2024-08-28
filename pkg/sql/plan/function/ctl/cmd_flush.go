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
	"context"
	"strconv"
	"strings"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleFlush() handleFunc {
	return GetTNHandlerFunc(
		api.OpCode_OpFlush,
		func(_ string) ([]uint64, error) {
			return nil, nil
		},
		func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			// parameter should be "DbName.TableName[.AccountId]" or "TableId"
			parameters := strings.Split(parameter, ".")
			if len(parameters) == 0 || len(parameters) > 3 {
				return nil, moerr.NewInternalErrorNoCtx("handleFlush: expected \"TableId\" or \"DbName.TableName[.AccountId]\" ")
			}
			txnOp := proc.GetTxnOperator()
			if txnOp == nil {
				return nil, moerr.NewInternalError(proc.Ctx, "handleFlush: txn operator is nil")
			}

			if len(parameters) == 1 {
				tblId, err := strconv.ParseUint(parameters[0], 10, 64)
				if err != nil {
					return nil, moerr.NewInternalError(proc.Ctx, "handleFlush: table id parse fail")
				}
				payload, err := types.Encode(&db.FlushTable{TableID: tblId})
				if err != nil {
					return nil, moerr.NewInternalError(proc.Ctx, "payload encode err")
				}
				return payload, nil
			}

			ctx := proc.Ctx
			if len(parameters) == 3 {
				accId, err := strconv.ParseUint(parameters[2], 0, 32)
				if err != nil {
					return nil, moerr.NewInternalError(proc.Ctx, "handleFlush: invalid account id")
				}
				ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(accId))
			}

			database, err := proc.GetSessionInfo().StorageEngine.Database(ctx, parameters[0], txnOp)
			if err != nil {
				return nil, err
			}
			rel, err := database.Relation(ctx, parameters[1], nil)
			if err != nil {
				return nil, err
			}
			payload, err := types.Encode(&db.FlushTable{TableID: rel.GetTableID(proc.Ctx)})
			if err != nil {
				return nil, moerr.NewInternalError(ctx, "payload encode err")
			}
			return payload, nil
		},
		func(data []byte) (any, error) {
			resp := api.TNStringResponse{}
			protoc.MustUnmarshal(&resp, data)
			return resp, nil
		})
}
