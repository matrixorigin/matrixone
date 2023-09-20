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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

// lockInfo holds all info from lockservice
type lockInfo struct {
	tableId     uint64
	keys        [][]byte
	lockMode    pblock.LockMode
	isRangeLock bool
	holders     []pblock.WaitTxn
	waiters     []pblock.WaitTxn
}

const (
	lockStatusWait     = "wait"     // nobody holds the lock but somebody waits on it
	lockStatusAcquired = "acquired" // somebody holds the lock
	lockStatusNone     = "none"     // nobody waits and holds the lock
)

func (li *lockInfo) isRange() bool {
	return li.isRangeLock
}

func (li *lockInfo) rangeKeys() ([]byte, []byte) {
	llen := len(li.keys)
	if llen >= 2 {
		return li.keys[0], li.keys[1]
	} else if llen >= 1 {
		return li.keys[0], []byte{}
	} else {
		return []byte{}, []byte{}
	}
}

func (li *lockInfo) pointKey() []byte {
	llen := len(li.keys)
	if llen >= 1 {
		return li.keys[0]
	}
	return []byte{}
}

func (li *lockInfo) status() string {
	hasHolders := len(li.holders) != 0
	hasWaiters := len(li.waiters) != 0
	if !hasHolders && !hasWaiters {
		return lockStatusNone
	} else if hasHolders {
		return lockStatusAcquired
	} else {
		return lockStatusWait
	}
}

func (li *lockInfo) holderList() []pblock.WaitTxn {
	return li.holders
}

func (li *lockInfo) waiterList() []pblock.WaitTxn {
	return li.waiters
}

func copyKeys(src [][]byte) [][]byte {
	dst := make([][]byte, 0, len(src))
	for _, s := range src {
		d := make([]byte, len(s))
		copy(d, s)
		dst = append(dst, s)
	}
	return dst
}

func copyWaitTxn(src pblock.WaitTxn) pblock.WaitTxn {
	dst := pblock.WaitTxn{}
	dst.TxnID = make([]byte, len(src.TxnID))
	copy(dst.TxnID, src.GetTxnID())
	dst.CreatedOn = src.GetCreatedOn()
	return dst
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

		locks := make([]*lockInfo, 0)

		getAllLocks := func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			//need copy keys
			info := &lockInfo{
				tableId:     tableID,
				keys:        copyKeys(keys),
				lockMode:    lock.GetLockMode(),
				isRangeLock: lock.IsRangeLock(),
			}

			lock.IterHolders(func(holder pblock.WaitTxn) bool {
				info.holders = append(info.holders, copyWaitTxn(holder))
				return true
			})

			lock.IterWaiters(func(waiter pblock.WaitTxn) bool {
				info.waiters = append(info.waiters, copyWaitTxn(waiter))
				return true
			})

			locks = append(locks, info)
			return true
		}

		proc.LockService.IterLocks(getAllLocks)

		//fill batch
		for _, lock := range locks {
			if lock == nil {
				continue
			}
			//cnId := ""
			//sessionId := ""
			txnId := ""
			tableId := fmt.Sprintf("%d", lock.tableId)

			//table name
			//tableName := ""
			//lock key
			lockKey := "point"
			if lock.isRange() {
				lockKey = "range"
			}

			//lock content
			lockContent := ""
			if lock.isRange() {
				k1, k2 := lock.rangeKeys()
				lockContent = hex.EncodeToString(k1) + "," + hex.EncodeToString(k2)
			} else {
				lockContent = hex.EncodeToString(lock.pointKey())
			}

			//lock mode
			lockMode := lock.lockMode.String()
			//lock status
			lockStatus := lock.status()
			//lock wait
			lockWait := ""

			hList := lock.holderList()
			hLen := len(hList)
			wList := lock.waiterList()
			wLen := len(wList)

			record := make([][]byte, len(plan2.MoLocksColNames))
			//record[plan2.MoLocksColTypeCnId] = []byte(cnId)
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
