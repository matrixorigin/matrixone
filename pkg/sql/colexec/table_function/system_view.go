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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
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

type moLocksState struct {
	simpleOneBatchState
}

func moLocksPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	if len(tf.ctr.retSchema) == 0 {
		tf.ctr.retSchema = make([]types.Type, len(tf.Attrs))
		for i, col := range tf.Attrs {
			col = strings.ToLower(col)
			idx, ok := plan2.MoLocksColName2Index[col]
			if !ok {
				return nil, moerr.NewInternalError(proc.Ctx, "invalid column name %s", col)
			}
			tf.ctr.retSchema[i] = plan2.MoLocksColTypes[idx]
		}
	}
	if len(tf.ctr.retSchema) != len(tf.Attrs) {
		return nil, moerr.NewInternalError(proc.Ctx, "invalid column count")
	}

	return &moLocksState{}, nil
}

func (s *moLocksState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	s.startPreamble(tf, proc, nthRow)
	bat := s.batch

	rsps, err := getLocks(proc)
	if err != nil {
		return err
	}

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
				if err := fillLockRecord(proc, tf.Attrs, bat, record); err != nil {
					return err
				}
			} else if hLen == 0 && wLen != 0 {
				//wLen records
				for j := 0; j < wLen; j++ {
					record[plan2.MoLocksColTypeLockWait] = []byte(hex.EncodeToString(wList[j].GetTxnID()))
					if err := fillLockRecord(proc, tf.Attrs, bat, record); err != nil {
						return err
					}
				}
			} else if hLen != 0 && wLen == 0 {
				//hLen records
				for j := 0; j < hLen; j++ {
					record[plan2.MoLocksColTypeTxnId] = []byte(hex.EncodeToString(hList[j].GetTxnID()))
					if err := fillLockRecord(proc, tf.Attrs, bat, record); err != nil {
						return err
					}
				}
			} else {
				//hLen * wLen records
				for j := 0; j < hLen; j++ {
					for k := 0; k < wLen; k++ {
						record[plan2.MoLocksColTypeTxnId] = []byte(hex.EncodeToString(hList[j].GetTxnID()))
						record[plan2.MoLocksColTypeLockWait] = []byte(hex.EncodeToString(wList[k].GetTxnID()))
						if err := fillLockRecord(proc, tf.Attrs, bat, record); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
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

	selectSuperTenant(
		proc.GetService(),
		clusterservice.NewSelector(),
		"root",
		nil,
		func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		},
	)

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

type moConfigurationState struct {
	simpleOneBatchState
}

func moConfigurationsPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	if len(tf.ctr.retSchema) == 0 {
		tf.ctr.retSchema = make([]types.Type, len(tf.Attrs))
		for i, col := range tf.Attrs {
			col = strings.ToLower(col)
			idx, ok := plan2.MoConfigColName2Index[col]
			if !ok {
				return nil, moerr.NewInternalError(proc.Ctx, "invalid column name %s", col)
			}
			tf.ctr.retSchema[i] = plan2.MoConfigColTypes[idx]
		}
	}
	if len(tf.ctr.retSchema) != len(tf.Attrs) {
		return nil, moerr.NewInternalError(proc.Ctx, "invalid column count")
	}
	return &moConfigurationState{}, nil
}

func (s *moConfigurationState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	s.startPreamble(tf, proc, nthRow)
	bat := s.batch
	mp := proc.GetMPool()

	if proc.Base.Hakeeper == nil {
		return moerr.NewInternalError(proc.Ctx, "hakeeper is nil")
	}
	//get cluster details
	details, err := proc.Base.Hakeeper.GetClusterDetails(proc.Ctx)
	if err != nil {
		return err
	}

	//fill batch for cn
	for _, cnStore := range details.GetCNStores() {
		if cnStore.GetConfigData() != nil {
			err = fillMapToBatch("cn", cnStore.GetUUID(), tf.Attrs, cnStore.GetConfigData().GetContent(), bat, mp)
			if err != nil {
				return err
			}
		}
	}

	//fill batch for tn
	for _, tnStore := range details.GetTNStores() {
		if tnStore.GetConfigData() != nil {
			err = fillMapToBatch("tn", tnStore.GetUUID(), tf.Attrs, tnStore.GetConfigData().GetContent(), bat, mp)
			if err != nil {
				return err
			}
		}
	}

	//fill batch for log
	for _, logStore := range details.GetLogStores() {
		if logStore.GetConfigData() != nil {
			err = fillMapToBatch("log", logStore.GetUUID(), tf.Attrs, logStore.GetConfigData().GetContent(), bat, mp)
			if err != nil {
				return err
			}
		}
	}

	// fill batch for proxy
	for _, proxyStore := range details.GetProxyStores() {
		if proxyStore.GetConfigData() != nil {
			err = fillMapToBatch(
				"proxy",
				proxyStore.GetUUID(),
				tf.Attrs,
				proxyStore.GetConfigData().GetContent(),
				bat,
				mp,
			)
			if err != nil {
				return err
			}
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
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

type moTransactionsState struct {
	simpleOneBatchState
}

func moTransactionsPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	if len(tf.ctr.retSchema) == 0 {
		tf.ctr.retSchema = make([]types.Type, len(tf.Attrs))
		for i, col := range tf.Attrs {
			col = strings.ToLower(col)
			idx, ok := plan2.MoTransactionsColName2Index[col]
			if !ok {
				return nil, moerr.NewInternalError(proc.Ctx, "invalid column name %s", col)
			}
			tf.ctr.retSchema[i] = plan2.MoTransactionsColTypes[idx]
		}
	}
	if len(tf.ctr.retSchema) != len(tf.Attrs) {
		return nil, moerr.NewInternalError(proc.Ctx, "invalid column count")
	}

	return &moTransactionsState{}, nil
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

func (s *moTransactionsState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	s.startPreamble(tf, proc, nthRow)
	bat := s.batch

	rsps, err := getTxns(proc)
	if err != nil {
		return err
	}

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

				if err := fillTxnRecord(proc, tf.Attrs, bat, record); err != nil {
					return err
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

					if err := fillTxnRecord(proc, tf.Attrs, bat, record); err != nil {
						return err
					}
				}
			}
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
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

	selectSuperTenant(
		proc.GetService(),
		clusterservice.NewSelector(),
		"root",
		nil,
		func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		},
	)

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

type moCacheState struct {
	simpleOneBatchState
}

func moCachePrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	tf.ctr.retSchema = make([]types.Type, len(tf.Attrs))
	for i, col := range tf.Attrs {
		col = strings.ToLower(col)
		idx, ok := plan2.MoCacheColName2Index[col]
		if !ok {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid column name %s", col)
		}
		tf.ctr.retSchema[i] = plan2.MoCacheColTypes[idx]
	}
	return &moCacheState{}, nil
}

func (s *moCacheState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	s.startPreamble(tf, proc, nthRow)
	bat := s.batch

	rsps, err := getCacheStats(proc)
	if err != nil {
		return err
	}

	for _, rsp := range rsps {
		if rsp == nil || len(rsp.CacheInfoList) == 0 {
			continue
		}

		for _, cache := range rsp.CacheInfoList {
			if cache == nil {
				continue
			}

			if err = fillCacheRecord(proc, tf.Attrs, bat, cache); err != nil {
				return err
			}
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
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

	selectSuperTenant(
		proc.GetService(),
		clusterservice.NewSelector(),
		"root",
		nil,
		func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		},
	)

	listTnService(
		proc.GetService(),
		func(s *metadata.TNService) {
			nodes = append(nodes, s.QueryAddress)
		},
	)

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

var selectSuperTenant = func(
	sid string,
	selector clusterservice.Selector,
	username string,
	filter func(string) bool,
	appendFn func(service *metadata.CNService),
) {
	clusterservice.GetMOCluster(sid).GetCNService(
		clusterservice.NewSelectAll(), func(s metadata.CNService) bool {
			appendFn(&s)
			return true
		})
}

var listTnService = func(
	sid string,
	appendFn func(service *metadata.TNService)) {
	disttae.ListTnService(sid, appendFn)
}

var requestMultipleCn = func(ctx context.Context, nodes []string, qc qclient.QueryClient, genRequest func() *query.Request, handleValidResponse func(string, *query.Response), handleInvalidResponse func(string)) error {
	return queryservice.RequestMultipleCn(ctx, nodes, qc, genRequest, handleValidResponse, handleInvalidResponse)
}
