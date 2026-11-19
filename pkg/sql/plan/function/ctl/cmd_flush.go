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
	"unicode/utf8"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
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
			if len(parameters) > 3 {
				return nil, invalidFlushTarget(parameter)
			}
			txnOp := proc.GetTxnOperator()
			if txnOp == nil {
				return nil, moerr.NewInternalError(proc.Ctx, "handleFlush: txn operator is nil")
			}

			if len(parameters) == 1 {
				tblId, err := strconv.ParseUint(parameters[0], 10, 64)
				if err != nil {
					return nil, invalidFlushTarget(parameter)
				}
				payload, err := types.Encode(&cmd_util.FlushTable{TableID: tblId})
				if err != nil {
					return nil, moerr.NewInternalError(proc.Ctx, "payload encode err")
				}
				return payload, nil
			}

			ctx := proc.Ctx
			if parameters[0] == "" || parameters[1] == "" {
				return nil, invalidFlushTarget(parameter)
			}
			if len(parameters) == 3 {
				accId, err := strconv.ParseUint(parameters[2], 0, 32)
				if err != nil {
					return nil, invalidFlushTarget(parameter)
				}
				ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(accId))
			}

			database, err := proc.GetSessionInfo().StorageEngine.Database(ctx, parameters[0], txnOp)
			if err != nil {
				// The storage engine reports a database missing from this
				// transaction's snapshot as the internal OkExpectedEOB sentinel
				// (disttae Engine.Database). Translate it to the typed
				// unknown-database error the way the DDL paths do, so the
				// errno-0 sentinel never leaks to SQL clients.
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					return nil, moerr.NewBadDB(ctx, parameters[0])
				}
				return nil, err
			}
			rel, err := database.Relation(ctx, parameters[1], nil)
			if err != nil {
				return nil, err
			}
			payload, err := types.Encode(&cmd_util.FlushTable{TableID: rel.GetTableID(proc.Ctx)})
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

// quotedFlushEchoLimit bounds how many bytes of the malformed target are
// echoed back in the error message; long garbage must not grow the reporting
// prefix without limit.
const quotedFlushEchoLimit = 64

// invalidFlushTarget reports a malformed flush target as bounded invalid input
// naming the accepted target shapes, mirroring the reader handler's usage
// hint. The echoed target is cut to a fixed byte budget with an explicit
// truncation mark, so the message stays bounded for any input; short targets
// keep the exact full echo.
func invalidFlushTarget(parameter string) error {
	return moerr.NewInvalidInputNoCtxf(
		"invalid flush ctl parameter %q, expect \"TableId\" or \"DbName.TableName[.AccountId]\", e.g. 2725 or db1.t1",
		truncateFlushEcho(parameter))
}

// truncateFlushEcho cuts the echo to the byte budget, backing up to a rune
// boundary so the quoted prefix stays one printable token; the mark records
// the cut.
func truncateFlushEcho(parameter string) string {
	if len(parameter) <= quotedFlushEchoLimit {
		return parameter
	}
	prefix := parameter[:quotedFlushEchoLimit]
	for len(prefix) > 0 && !utf8.ValidString(prefix) {
		prefix = prefix[:len(prefix)-1]
	}
	return prefix + "..."
}
