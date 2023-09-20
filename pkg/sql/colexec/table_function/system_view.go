// Copyright 2023 Matrix Origin
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

package table_function

import (
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

const (
	lockStatusWait     = "wait"     // nobody holds the lock but somebody waits on it
	lockStatusAcquired = "acquired" // somebody holds the lock
	lockStatusNone     = "none"     // nobody waits and holds the lock
)

func getLockStatus(li *query.LockInfo) string {
	hasHolders := len(li.GetHolders()) != 0
	hasWaiters := len(li.GetWaiters()) != 0
	if !hasHolders && !hasWaiters {
		return lockStatusNone
	} else if hasHolders {
		return lockStatusAcquired
	} else {
		return lockStatusWait
	}
}

func getRangeKeys(li *query.LockInfo) ([]byte, []byte) {
	keys := li.GetKeys()
	llen := len(keys)
	if llen >= 2 {
		return keys[0], keys[1]
	} else if llen >= 1 {
		return keys[0], []byte{}
	} else {
		return []byte{}, []byte{}
	}
}

func getPointKey(li *query.LockInfo) []byte {
	keys := li.GetKeys()
	llen := len(keys)
	if llen >= 1 {
		return keys[0]
	}
	return []byte{}
}

func moLocksPrepare(proc *process.Process, arg *Argument) error {
	arg.ctr.state = dataProducing
	if len(arg.Args) > 0 {
		return moerr.NewInvalidInput(proc.Ctx, "moConfigurations: no argument is required")
	}
	for i := range arg.Attrs {
		arg.Attrs[i] = strings.ToUpper(arg.Attrs[i])
	}
	return nil
}

func moLocksCall(_ int, proc *process.Process, arg *Argument) (bool, error) {
	switch arg.ctr.state {
	case dataProducing:

		rsps, err := getLocks(proc)
		if err != nil {
			return false, err
		}

		//alloc batch
		bat := batch.NewWithSize(len(arg.Attrs))
		for i, col := range arg.Attrs {
			col = strings.ToLower(col)
			idx, ok := plan2.MoLocksColName2Index[col]
			if !ok {
				return false, moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", col)
			}

			tp := plan2.MoLocksColTypes[idx]
			bat.Vecs[i] = vector.NewVec(tp)
		}
		bat.Attrs = arg.Attrs

		//fill batch from lock info
		for _, rsp := range rsps {
			if rsp == nil || len(rsp.LockInfoList) == 0 {
				continue
			}
			for _, lock := range rsp.LockInfoList {
				if lock == nil {
					continue
				}
				cnId := rsp.GetCnId()
				//sessionId := ""
				txnId := ""
				tableId := fmt.Sprintf("%d", lock.GetTableId())

				//table name
				//tableName := ""
				//lock key
				lockKey := "point"
				if lock.GetIsRangeLock() {
					lockKey = "range"
				}

				//lock content
				lockContent := ""
				if lock.GetIsRangeLock() {
					k1, k2 := getRangeKeys(lock)
					lockContent = hex.EncodeToString(k1) + "," + hex.EncodeToString(k2)
				} else {
					lockContent = hex.EncodeToString(getPointKey(lock))
				}

				//lock mode
				lockMode := lock.GetLockMode().String()
				//lock status
				lockStatus := getLockStatus(lock)
				//lock wait
				lockWait := ""

				hList := lock.GetHolders()
				hLen := len(hList)
				wList := lock.GetWaiters()
				wLen := len(wList)

				record := make([][]byte, len(plan2.MoLocksColNames))
				record[plan2.MoLocksColTypeCnId] = []byte(cnId)
				//record[plan2.MoLocksColTypeSessionId] = []byte(sessionId)
				record[plan2.MoLocksColTypeTxnId] = []byte(txnId)
				record[plan2.MoLocksColTypeTableId] = []byte(tableId)
				//record[plan2.MoLocksColTypeTableName] = []byte(tableName)
				record[plan2.MoLocksColTypeLockKey] = []byte(lockKey)
				record[plan2.MoLocksColTypeLockContent] = []byte(lockContent)
				record[plan2.MoLocksColTypeLockMode] = []byte(lockMode)
				record[plan2.MoLocksColTypeLockStatus] = []byte(lockStatus)
				record[plan2.MoLocksColTypeLockWait] = []byte(lockWait)

				if hLen == 0 && wLen == 0 {
					//one record
					if err := fillRecord(proc, bat, record); err != nil {
						return false, err
					}
				} else if hLen == 0 && wLen != 0 {
					//wLen records
					for j := 0; j < wLen; j++ {
						record[plan2.MoLocksColTypeLockWait] = []byte(hex.EncodeToString(wList[j].GetTxnID()))
						if err := fillRecord(proc, bat, record); err != nil {
							return false, err
						}
					}
				} else if hLen != 0 && wLen == 0 {
					//hLen records
					for j := 0; j < hLen; j++ {
						record[plan2.MoLocksColTypeTxnId] = []byte(hex.EncodeToString(hList[j].GetTxnID()))
						if err := fillRecord(proc, bat, record); err != nil {
							return false, err
						}
					}
				} else {
					//hLen * wLen records
					for j := 0; j < hLen; j++ {
						for k := 0; k < wLen; k++ {
							record[plan2.MoLocksColTypeTxnId] = []byte(hex.EncodeToString(hList[j].GetTxnID()))
							record[plan2.MoLocksColTypeLockWait] = []byte(hex.EncodeToString(wList[k].GetTxnID()))
							if err := fillRecord(proc, bat, record); err != nil {
								return false, err
							}
						}
					}
				}
			}
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		proc.SetInputBatch(bat)
		arg.ctr.state = dataFinished
		return false, nil

	case dataFinished:
		proc.SetInputBatch(nil)
		return true, nil
	default:
		return false, moerr.NewInternalError(proc.Ctx, "unknown state %v", arg.ctr.state)
	}
}

func fillRecord(proc *process.Process, bat *batch.Batch, record [][]byte) error {
	for colIdx, colData := range record {
		if err := vector.AppendBytes(bat.Vecs[colIdx], colData, false, proc.GetMPool()); err != nil {
			return err
		}
	}
	return nil
}

// getLocks get lock info from all cn
func getLocks(proc *process.Process) ([]*query.GetLockInfoResponse, error) {
	var err error
	var nodes []string

	disttae.SelectForSuperTenant(clusterservice.NewSelector(), "root", nil,
		func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		})

	genRequest := func() *query.Request {
		req := proc.QueryService.NewRequest(query.CmdMethod_GetLockInfo)
		req.GetLockInfoRequest = &query.GetLockInfoRequest{}
		return req
	}

	rsps := make([]*query.GetLockInfoResponse, 0)

	handleValidResponse := func(nodeAddr string, rsp *query.Response) {
		if rsp != nil && rsp.GetLockInfoResponse != nil {
			rsps = append(rsps, rsp.GetLockInfoResponse)
		}
	}

	err = queryservice.RequestMultipleCn(proc.Ctx, nodes, proc.QueryService, genRequest, handleValidResponse, nil)
	return rsps, err
}