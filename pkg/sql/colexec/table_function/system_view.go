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
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func moLocksCall(_ int, proc *process.Process, arg *Argument, result *vm.CallResult) (bool, error) {
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
			bat.Vecs[i] = proc.GetVector(tp)
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
					if err := fillLockRecord(proc, arg.Attrs, bat, record); err != nil {
						return false, err
					}
				} else if hLen == 0 && wLen != 0 {
					//wLen records
					for j := 0; j < wLen; j++ {
						record[plan2.MoLocksColTypeLockWait] = []byte(hex.EncodeToString(wList[j].GetTxnID()))
						if err := fillLockRecord(proc, arg.Attrs, bat, record); err != nil {
							return false, err
						}
					}
				} else if hLen != 0 && wLen == 0 {
					//hLen records
					for j := 0; j < hLen; j++ {
						record[plan2.MoLocksColTypeTxnId] = []byte(hex.EncodeToString(hList[j].GetTxnID()))
						if err := fillLockRecord(proc, arg.Attrs, bat, record); err != nil {
							return false, err
						}
					}
				} else {
					//hLen * wLen records
					for j := 0; j < hLen; j++ {
						for k := 0; k < wLen; k++ {
							record[plan2.MoLocksColTypeTxnId] = []byte(hex.EncodeToString(hList[j].GetTxnID()))
							record[plan2.MoLocksColTypeLockWait] = []byte(hex.EncodeToString(wList[k].GetTxnID()))
							if err := fillLockRecord(proc, arg.Attrs, bat, record); err != nil {
								return false, err
							}
						}
					}
				}
			}
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		result.Batch = bat
		arg.ctr.state = dataFinished
		return false, nil

	case dataFinished:
		result.Batch = nil
		return true, nil
	default:
		return false, moerr.NewInternalError(proc.Ctx, "unknown state %v", arg.ctr.state)
	}
}

func fillLockRecord(proc *process.Process, attrs []string, bat *batch.Batch, record [][]byte) error {
	for colIdx, attr := range attrs {
		realColIdx := plan2.MoLocksColName2Index[strings.ToLower(attr)]
		if err := vector.AppendBytes(bat.Vecs[colIdx], record[realColIdx], false, proc.GetMPool()); err != nil {
			return err
		}
	}
	return nil
}

// getLocks get lock info from all cn
func getLocks(proc *process.Process) ([]*query.GetLockInfoResponse, error) {
	var err error
	var nodes []string

	selectSuperTenant(clusterservice.NewSelector(), "root", nil,
		func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		})

	genRequest := func() *query.Request {
		req := proc.GetQueryClient().NewRequest(query.CmdMethod_GetLockInfo)
		req.GetLockInfoRequest = &query.GetLockInfoRequest{}
		return req
	}

	rsps := make([]*query.GetLockInfoResponse, 0)

	handleValidResponse := func(nodeAddr string, rsp *query.Response) {
		if rsp != nil && rsp.GetLockInfoResponse != nil {
			rsps = append(rsps, rsp.GetLockInfoResponse)
		}
	}

	err = requestMultipleCn(proc.Ctx, nodes, proc.Base.QueryClient, genRequest, handleValidResponse, nil)
	return rsps, err
}

func moConfigurationsPrepare(proc *process.Process, arg *Argument) error {
	arg.ctr.state = dataProducing
	if len(arg.Args) > 0 {
		return moerr.NewInvalidInput(proc.Ctx, "moConfigurations: no argument is required")
	}
	for i := range arg.Attrs {
		arg.Attrs[i] = strings.ToUpper(arg.Attrs[i])
	}
	return nil
}

func moConfigurationsCall(_ int, proc *process.Process, arg *Argument, result *vm.CallResult) (bool, error) {
	switch arg.ctr.state {
	case dataProducing:

		if proc.Base.Hakeeper == nil {
			return false, moerr.NewInternalError(proc.Ctx, "hakeeper is nil")
		}

		//get cluster details
		details, err := proc.Base.Hakeeper.GetClusterDetails(proc.Ctx)
		if err != nil {
			return false, err
		}

		//alloc batch
		bat := batch.NewWithSize(len(arg.Attrs))
		for i, col := range arg.Attrs {
			col = strings.ToLower(col)
			idx, ok := plan2.MoConfigColName2Index[col]
			if !ok {
				return false, moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", col)
			}

			tp := plan2.MoConfigColTypes[idx]
			bat.Vecs[i] = proc.GetVector(tp)
		}
		bat.Attrs = arg.Attrs

		mp := proc.GetMPool()

		//fill batch for cn
		for _, cnStore := range details.GetCNStores() {
			if cnStore.GetConfigData() != nil {
				err = fillMapToBatch("cn", cnStore.GetUUID(), arg.Attrs, cnStore.GetConfigData().GetContent(), bat, mp)
				if err != nil {
					return false, err
				}
			}
		}

		//fill batch for tn
		for _, tnStore := range details.GetTNStores() {
			if tnStore.GetConfigData() != nil {
				err = fillMapToBatch("tn", tnStore.GetUUID(), arg.Attrs, tnStore.GetConfigData().GetContent(), bat, mp)
				if err != nil {
					return false, err
				}
			}
		}

		//fill batch for log
		for _, logStore := range details.GetLogStores() {
			if logStore.GetConfigData() != nil {
				err = fillMapToBatch("log", logStore.GetUUID(), arg.Attrs, logStore.GetConfigData().GetContent(), bat, mp)
				if err != nil {
					return false, err
				}
			}
		}

		// fill batch for proxy
		for _, proxyStore := range details.GetProxyStores() {
			if proxyStore.GetConfigData() != nil {
				err = fillMapToBatch(
					"proxy",
					proxyStore.GetUUID(),
					arg.Attrs,
					proxyStore.GetConfigData().GetContent(),
					bat,
					mp,
				)
				if err != nil {
					return false, err
				}
			}
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		result.Batch = bat
		arg.ctr.state = dataFinished
		return false, nil

	case dataFinished:
		result.Batch = nil
		return true, nil
	default:
		return false, moerr.NewInternalError(proc.Ctx, "unknown state %v", arg.ctr.state)
	}
}

func fillMapToBatch(nodeType, nodeId string, attrs []string, kvs map[string]*logservicepb.ConfigItem, bat *batch.Batch, mp *mpool.MPool) error {
	var err error
	for _, value := range kvs {
		for i, col := range attrs {
			col = strings.ToLower(col)
			switch plan2.MoConfigColType(plan2.MoConfigColName2Index[col]) {
			case plan2.MoConfigColTypeNodeType:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(nodeType), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeNodeId:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(nodeId), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeName:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(value.GetName()), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeCurrentValue:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(value.GetCurrentValue()), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeDefaultValue:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(value.GetDefaultValue()), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeInternal:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(value.GetInternal()), false, mp); err != nil {
					return err
				}
			}
		}
	}
	return err
}

func moTransactionsPrepare(proc *process.Process, arg *Argument) error {
	arg.ctr.state = dataProducing
	if len(arg.Args) > 0 {
		return moerr.NewInvalidInput(proc.Ctx, "moTransactions: no argument is required")
	}
	for i := range arg.Attrs {
		arg.Attrs[i] = strings.ToUpper(arg.Attrs[i])
	}
	return nil
}

func getRangeContent(li *query.TxnLockInfo) ([]byte, []byte) {
	keys := li.GetRows()
	llen := len(keys)
	if llen >= 2 {
		return keys[0], keys[1]
	} else if llen >= 1 {
		return keys[0], []byte{}
	} else {
		return []byte{}, []byte{}
	}
}

func getPointContent(li *query.TxnLockInfo) []byte {
	keys := li.GetRows()
	llen := len(keys)
	if llen >= 1 {
		return keys[0]
	}
	return []byte{}
}

func moTransactionsCall(_ int, proc *process.Process, arg *Argument, result *vm.CallResult) (bool, error) {
	switch arg.ctr.state {
	case dataProducing:

		rsps, err := getTxns(proc)
		if err != nil {
			return false, err
		}

		//alloc batch
		bat := batch.NewWithSize(len(arg.Attrs))
		for i, col := range arg.Attrs {
			col = strings.ToLower(col)
			idx, ok := plan2.MoTransactionsColName2Index[col]
			if !ok {
				return false, moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", col)
			}

			tp := plan2.MoTransactionsColTypes[idx]
			bat.Vecs[i] = proc.GetVector(tp)
		}
		bat.Attrs = arg.Attrs
		for _, rsp := range rsps {
			if rsp == nil || len(rsp.TxnInfoList) == 0 {
				continue
			}

			for _, txn := range rsp.TxnInfoList {
				if txn == nil {
					continue
				}

				cnId := rsp.GetCnId()
				txnId := ""
				if txn.GetMeta() != nil {
					txnId = hex.EncodeToString(txn.GetMeta().GetID())
				}
				createTs := txn.GetCreateAt().Format(time.RFC3339Nano)
				snapshotTs := ""
				if txn.GetMeta() != nil {
					snapshotTs = txn.GetMeta().GetSnapshotTS().DebugString()
				}
				preparedTs := ""
				if txn.GetMeta() != nil {
					preparedTs = txn.GetMeta().GetPreparedTS().DebugString()
				}
				commitTs := ""
				if txn.GetMeta() != nil {
					commitTs = txn.GetMeta().GetCommitTS().DebugString()
				}
				txnMode := ""
				if txn.GetMeta() != nil {
					txnMode = txn.GetMeta().GetMode().String()
				}
				isolation := ""
				if txn.GetMeta() != nil {
					isolation = txn.GetMeta().GetIsolation().String()
				}
				userTxn := strconv.FormatBool(txn.GetUserTxn())
				txnStatus := ""
				if txn.GetMeta() != nil {
					txnStatus = txn.GetMeta().GetStatus().String()
				}

				waitLocksCnt := len(txn.GetWaitLocks())
				record := make([][]byte, len(plan2.MoTransactionsColNames))
				record[plan2.MoTransactionsColTypeCnId] = []byte(cnId)
				record[plan2.MoTransactionsColTypeTxnId] = []byte(txnId)
				record[plan2.MoTransactionsColTypeCreateTs] = []byte(createTs)
				record[plan2.MoTransactionsColTypeSnapshotTs] = []byte(snapshotTs)
				record[plan2.MoTransactionsColTypePreparedTs] = []byte(preparedTs)
				record[plan2.MoTransactionsColTypeCommitTs] = []byte(commitTs)
				record[plan2.MoTransactionsColTypeTxnMode] = []byte(txnMode)
				record[plan2.MoTransactionsColTypeIsolation] = []byte(isolation)
				record[plan2.MoTransactionsColTypeUserTxn] = []byte(userTxn)
				record[plan2.MoTransactionsColTypeTxnStatus] = []byte(txnStatus)

				if waitLocksCnt == 0 {
					//one record
					record[plan2.MoTransactionsColTypeTableId] = []byte{}
					record[plan2.MoTransactionsColTypeLockKey] = []byte{}
					record[plan2.MoTransactionsColTypeLockContent] = []byte{}
					record[plan2.MoTransactionsColTypeLockMode] = []byte{}

					if err := fillTxnRecord(proc, arg.Attrs, bat, record); err != nil {
						return false, err
					}
				} else {
					//multiple records

					for _, lock := range txn.GetWaitLocks() {
						options := lock.GetOptions()
						if options == nil {
							continue
						}

						//table id
						tableId := fmt.Sprintf("%d", lock.GetTableId())
						record[plan2.MoTransactionsColTypeTableId] = []byte(tableId)

						//lock key
						lockKey := "point"
						if options.GetGranularity() == pblock.Granularity_Range {
							lockKey = "range"
						}
						record[plan2.MoTransactionsColTypeLockKey] = []byte(lockKey)

						//lock content
						lockContent := ""
						if options.GetGranularity() == pblock.Granularity_Range {
							//first range
							k1, k2 := getRangeContent(lock)
							lockContent = hex.EncodeToString(k1) + "," + hex.EncodeToString(k2)
						} else {
							lockContent = hex.EncodeToString(getPointContent(lock))
						}
						record[plan2.MoTransactionsColTypeLockContent] = []byte(lockContent)

						//lock mode
						lockMode := options.GetMode().String()
						record[plan2.MoTransactionsColTypeLockMode] = []byte(lockMode)

						if err := fillTxnRecord(proc, arg.Attrs, bat, record); err != nil {
							return false, err
						}
					}
				}

			}
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		result.Batch = bat
		arg.ctr.state = dataFinished
		return false, nil

	case dataFinished:
		return true, nil
	default:
		return false, moerr.NewInternalError(proc.Ctx, "unknown state %v", arg.ctr.state)
	}
}

func fillTxnRecord(proc *process.Process, attrs []string, bat *batch.Batch, record [][]byte) error {
	for colIdx, attr := range attrs {
		realColIdx := plan2.MoTransactionsColName2Index[strings.ToLower(attr)]
		if err := vector.AppendBytes(bat.Vecs[colIdx], record[realColIdx], false, proc.GetMPool()); err != nil {
			return err
		}
	}
	return nil
}

// getTxns get txn info from all cn
func getTxns(proc *process.Process) ([]*query.GetTxnInfoResponse, error) {
	var err error
	var nodes []string

	selectSuperTenant(clusterservice.NewSelector(), "root", nil,
		func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		})

	genRequest := func() *query.Request {
		req := proc.Base.QueryClient.NewRequest(query.CmdMethod_GetTxnInfo)
		req.GetTxnInfoRequest = &query.GetTxnInfoRequest{}
		return req
	}

	rsps := make([]*query.GetTxnInfoResponse, 0)

	handleValidResponse := func(nodeAddr string, rsp *query.Response) {
		if rsp != nil && rsp.GetTxnInfoResponse != nil {
			rsps = append(rsps, rsp.GetTxnInfoResponse)
		}
	}

	err = requestMultipleCn(proc.Ctx, nodes, proc.Base.QueryClient, genRequest, handleValidResponse, nil)
	return rsps, err
}

func moCachePrepare(proc *process.Process, arg *Argument) error {
	arg.ctr.state = dataProducing
	if len(arg.Args) > 0 {
		return moerr.NewInvalidInput(proc.Ctx, "moCache: no argument is required")
	}
	for i := range arg.Attrs {
		arg.Attrs[i] = strings.ToUpper(arg.Attrs[i])
	}
	return nil
}

func moCacheCall(_ int, proc *process.Process, arg *Argument, result *vm.CallResult) (bool, error) {
	switch arg.ctr.state {
	case dataProducing:

		rsps, err := getCacheStats(proc)
		if err != nil {
			return false, err
		}

		//alloc batch
		bat := batch.NewWithSize(len(arg.Attrs))
		for i, col := range arg.Attrs {
			col = strings.ToLower(col)
			idx, ok := plan2.MoCacheColName2Index[col]
			if !ok {
				return false, moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", col)
			}

			tp := plan2.MoCacheColTypes[idx]
			bat.Vecs[i] = proc.GetVector(tp)
		}
		bat.Attrs = arg.Attrs
		for _, rsp := range rsps {
			if rsp == nil || len(rsp.CacheInfoList) == 0 {
				continue
			}

			for _, cache := range rsp.CacheInfoList {
				if cache == nil {
					continue
				}

				if err = fillCacheRecord(proc, arg.Attrs, bat, cache); err != nil {
					return false, err
				}
			}
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		result.Batch = bat
		arg.ctr.state = dataFinished
		return false, nil

	case dataFinished:
		result.Batch = nil
		return true, nil
	default:
		return false, moerr.NewInternalError(proc.Ctx, "unknown state %v", arg.ctr.state)
	}
}

func fillCacheRecord(proc *process.Process, attrs []string, bat *batch.Batch, cache *query.CacheInfo) error {
	var err error
	for colIdx, attr := range attrs {
		switch plan2.MoCacheColType(plan2.MoCacheColName2Index[strings.ToLower(attr)]) {
		case plan2.MoCacheColTypeNodeType:
			if err = vector.AppendBytes(bat.Vecs[colIdx], []byte(cache.GetNodeType()), false, proc.GetMPool()); err != nil {
				return err
			}
		case plan2.MoCacheColTypeNodeId:
			if err = vector.AppendBytes(bat.Vecs[colIdx], []byte(cache.GetNodeId()), false, proc.GetMPool()); err != nil {
				return err
			}
		case plan2.MoCacheColTypeType:
			if err = vector.AppendBytes(bat.Vecs[colIdx], []byte(cache.GetCacheType()), false, proc.GetMPool()); err != nil {
				return err
			}
		case plan2.MoCacheColTypeUsed:
			if err = vector.AppendFixed(bat.Vecs[colIdx], cache.GetUsed(), false, proc.GetMPool()); err != nil {
				return err
			}
		case plan2.MoCacheColTypeFree:
			if err = vector.AppendFixed(bat.Vecs[colIdx], cache.GetFree(), false, proc.GetMPool()); err != nil {
				return err
			}
		case plan2.MoCacheColTypeHitRatio:
			if err = vector.AppendFixed(bat.Vecs[colIdx], cache.GetHitRatio(), false, proc.GetMPool()); err != nil {
				return err
			}
		}
	}

	return err
}

// getCacheStats get txn info from all cn, tn
func getCacheStats(proc *process.Process) ([]*query.GetCacheInfoResponse, error) {
	var err error
	var nodes []string

	selectSuperTenant(clusterservice.NewSelector(), "root", nil,
		func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		})

	listTnService(func(s *metadata.TNService) {
		nodes = append(nodes, s.QueryAddress)
	})

	genRequest := func() *query.Request {
		req := proc.Base.QueryClient.NewRequest(query.CmdMethod_GetCacheInfo)
		req.GetCacheInfoRequest = &query.GetCacheInfoRequest{}
		return req
	}

	rsps := make([]*query.GetCacheInfoResponse, 0)

	handleValidResponse := func(nodeAddr string, rsp *query.Response) {
		if rsp != nil && rsp.GetCacheInfoResponse != nil {
			rsps = append(rsps, rsp.GetCacheInfoResponse)
		}
	}

	err = requestMultipleCn(proc.Ctx, nodes, proc.Base.QueryClient, genRequest, handleValidResponse, nil)
	return rsps, err
}

var selectSuperTenant = func(selector clusterservice.Selector,
	username string,
	filter func(string) bool,
	appendFn func(service *metadata.CNService)) {
	clusterservice.GetMOCluster().GetCNService(
		clusterservice.NewSelectAll(), func(s metadata.CNService) bool {
			appendFn(&s)
			return true
		})
}

var listTnService = func(appendFn func(service *metadata.TNService)) {
	disttae.ListTnService(appendFn)
}

var requestMultipleCn = func(ctx context.Context, nodes []string, qc qclient.QueryClient, genRequest func() *query.Request, handleValidResponse func(string, *query.Response), handleInvalidResponse func(string)) error {
	return queryservice.RequestMultipleCn(ctx, nodes, qc, genRequest, handleValidResponse, handleInvalidResponse)
}
