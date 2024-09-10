// Copyright 2022 Matrix Origin
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

package disttae

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"go.uber.org/zap"
)

func LinearSearchOffsetByValFactory(pk *vector.Vector) func(*vector.Vector) []int64 {
	mp := make(map[any]bool)
	switch pk.GetType().Oid {
	case types.T_bool:
		vs := vector.MustFixedColNoTypeCheck[bool](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_bit:
		vs := vector.MustFixedColNoTypeCheck[uint64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int8:
		vs := vector.MustFixedColNoTypeCheck[int8](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int16:
		vs := vector.MustFixedColNoTypeCheck[int16](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int32:
		vs := vector.MustFixedColNoTypeCheck[int32](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int64:
		vs := vector.MustFixedColNoTypeCheck[int64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint8:
		vs := vector.MustFixedColNoTypeCheck[uint8](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint16:
		vs := vector.MustFixedColNoTypeCheck[uint16](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint32:
		vs := vector.MustFixedColNoTypeCheck[uint32](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint64:
		vs := vector.MustFixedColNoTypeCheck[uint64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_decimal64:
		vs := vector.MustFixedColNoTypeCheck[types.Decimal64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_decimal128:
		vs := vector.MustFixedColNoTypeCheck[types.Decimal128](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uuid:
		vs := vector.MustFixedColNoTypeCheck[types.Uuid](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_float32:
		vs := vector.MustFixedColNoTypeCheck[float32](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_float64:
		vs := vector.MustFixedColNoTypeCheck[float64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_date:
		vs := vector.MustFixedColNoTypeCheck[types.Date](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_timestamp:
		vs := vector.MustFixedColNoTypeCheck[types.Timestamp](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_time:
		vs := vector.MustFixedColNoTypeCheck[types.Time](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_datetime:
		vs := vector.MustFixedColNoTypeCheck[types.Datetime](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_enum:
		vs := vector.MustFixedColNoTypeCheck[types.Enum](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_TS:
		vs := vector.MustFixedColNoTypeCheck[types.TS](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_Rowid:
		vs := vector.MustFixedColNoTypeCheck[types.Rowid](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_Blockid:
		vs := vector.MustFixedColNoTypeCheck[types.Blockid](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		if pk.IsConst() {
			for i := 0; i < pk.Length(); i++ {
				v := pk.UnsafeGetStringAt(i)
				mp[v] = true
			}
		} else {
			vs := vector.MustFixedColNoTypeCheck[types.Varlena](pk)
			area := pk.GetArea()
			for i := 0; i < len(vs); i++ {
				v := vs[i].UnsafeGetString(area)
				mp[v] = true
			}
		}
	case types.T_array_float32:
		for i := 0; i < pk.Length(); i++ {
			v := types.ArrayToString[float32](vector.GetArrayAt[float32](pk, i))
			mp[v] = true
		}
	case types.T_array_float64:
		for i := 0; i < pk.Length(); i++ {
			v := types.ArrayToString[float64](vector.GetArrayAt[float64](pk, i))
			mp[v] = true
		}
	default:
		panic(moerr.NewInternalErrorNoCtxf("%s not supported", pk.GetType().String()))
	}

	return func(vec *vector.Vector) []int64 {
		var sels []int64
		switch vec.GetType().Oid {
		case types.T_bool:
			vs := vector.MustFixedColNoTypeCheck[bool](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_bit:
			vs := vector.MustFixedColNoTypeCheck[uint64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_int8:
			vs := vector.MustFixedColNoTypeCheck[int8](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_int16:
			vs := vector.MustFixedColNoTypeCheck[int16](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_int32:
			vs := vector.MustFixedColNoTypeCheck[int32](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_int64:
			vs := vector.MustFixedColNoTypeCheck[int64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_uint8:
			vs := vector.MustFixedColNoTypeCheck[uint8](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_uint16:
			vs := vector.MustFixedColNoTypeCheck[uint16](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_uint32:
			vs := vector.MustFixedColNoTypeCheck[uint32](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_uint64:
			vs := vector.MustFixedColNoTypeCheck[uint64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_decimal64:
			vs := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_decimal128:
			vs := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_uuid:
			vs := vector.MustFixedColNoTypeCheck[types.Uuid](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_float32:
			vs := vector.MustFixedColNoTypeCheck[float32](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_float64:
			vs := vector.MustFixedColNoTypeCheck[float64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_date:
			vs := vector.MustFixedColNoTypeCheck[types.Date](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_timestamp:
			vs := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_time:
			vs := vector.MustFixedColNoTypeCheck[types.Time](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_datetime:
			vs := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_enum:
			vs := vector.MustFixedColNoTypeCheck[types.Enum](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_TS:
			vs := vector.MustFixedColNoTypeCheck[types.TS](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_Rowid:
			vs := vector.MustFixedColNoTypeCheck[types.Rowid](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_Blockid:
			vs := vector.MustFixedColNoTypeCheck[types.Blockid](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_char, types.T_varchar, types.T_json,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
			if pk.IsConst() {
				for i := 0; i < pk.Length(); i++ {
					v := pk.UnsafeGetStringAt(i)
					if mp[v] {
						sels = append(sels, int64(i))
					}
				}
			} else {
				vs := vector.MustFixedColNoTypeCheck[types.Varlena](pk)
				area := pk.GetArea()
				for i := 0; i < len(vs); i++ {
					v := vs[i].UnsafeGetString(area)
					if mp[v] {
						sels = append(sels, int64(i))
					}
				}
			}
		case types.T_array_float32:
			for i := 0; i < vec.Length(); i++ {
				v := types.ArrayToString[float32](vector.GetArrayAt[float32](vec, i))
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		case types.T_array_float64:
			for i := 0; i < vec.Length(); i++ {
				v := types.ArrayToString[float64](vector.GetArrayAt[float64](vec, i))
				if mp[v] {
					sels = append(sels, int64(i))
				}
			}
		default:
			panic(moerr.NewInternalErrorNoCtxf("%s not supported", vec.GetType().String()))
		}
		return sels
	}
}

func getNonSortedPKSearchFuncByPKVec(
	vec *vector.Vector,
) objectio.ReadFilterSearchFuncType {

	searchPKFunc := LinearSearchOffsetByValFactory(vec)

	if searchPKFunc != nil {
		return func(vecs []*vector.Vector) []int64 {
			return searchPKFunc(vecs[0])
		}
	}
	return nil
}

// ListTnService gets all tn service in the cluster
func ListTnService(
	service string,
	appendFn func(service *metadata.TNService),
) {
	mc := clusterservice.GetMOCluster(service)
	mc.GetTNService(clusterservice.NewSelector(), func(tn metadata.TNService) bool {
		if appendFn != nil {
			appendFn(&tn)
		}
		return true
	})
}

func ForeachBlkInObjStatsList(
	next bool,
	dataMeta objectio.ObjectDataMeta,
	onBlock func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool,
	objects ...objectio.ObjectStats,
) {
	stop := false
	objCnt := len(objects)

	for idx := 0; idx < objCnt && !stop; idx++ {
		iter := NewStatsBlkIter(&objects[idx], dataMeta)
		pos := uint32(0)
		for iter.Next() {
			blk := iter.Entry()
			var meta objectio.BlockObject
			if !dataMeta.IsEmpty() {
				meta = dataMeta.GetBlockMeta(pos)
			}
			pos++
			if !onBlock(blk, meta) {
				stop = true
				break
			}
		}

		if stop && next {
			stop = false
		}
	}
}

type StatsBlkIter struct {
	name       objectio.ObjectName
	extent     objectio.Extent
	blkCnt     uint16
	totalRows  uint32
	cur        int
	accRows    uint32
	curBlkRows uint32
	meta       objectio.ObjectDataMeta
}

func NewStatsBlkIter(stats *objectio.ObjectStats, meta objectio.ObjectDataMeta) *StatsBlkIter {
	return &StatsBlkIter{
		name:       stats.ObjectName(),
		blkCnt:     uint16(stats.BlkCnt()),
		extent:     stats.Extent(),
		cur:        -1,
		accRows:    0,
		totalRows:  stats.Rows(),
		curBlkRows: objectio.BlockMaxRows,
		meta:       meta,
	}
}

func (i *StatsBlkIter) Next() bool {
	if i.cur >= 0 {
		i.accRows += i.curBlkRows
	}
	i.cur++
	return i.cur < int(i.blkCnt)
}

func (i *StatsBlkIter) Entry() objectio.BlockInfo {
	if i.cur == -1 {
		i.cur = 0
	}

	// assume that all blks have BlockMaxRows, except the last one
	if i.meta.IsEmpty() {
		if i.cur == int(i.blkCnt-1) {
			i.curBlkRows = i.totalRows - i.accRows
		}
	} else {
		i.curBlkRows = i.meta.GetBlockMeta(uint32(i.cur)).GetRows()
	}

	loc := objectio.BuildLocation(i.name, i.extent, i.curBlkRows, uint16(i.cur))
	blk := objectio.BlockInfo{
		BlockID: *objectio.BuildObjectBlockid(i.name, uint16(i.cur)),
		MetaLoc: objectio.ObjectLocation(loc),
	}
	return blk
}

func ForeachCommittedObjects(
	createObjs map[objectio.ObjectNameShort]struct{},
	delObjs map[objectio.ObjectNameShort]struct{},
	p *logtailreplay.PartitionState,
	onObj func(info logtailreplay.ObjectInfo) error) (err error) {
	for obj := range createObjs {
		if objInfo, ok := p.GetObject(obj); ok {
			if err = onObj(objInfo); err != nil {
				return
			}
		}
	}
	for obj := range delObjs {
		if objInfo, ok := p.GetObject(obj); ok {
			if err = onObj(objInfo); err != nil {
				return
			}
		}
	}
	return nil

}

func ForeachTombstoneObject(
	ts types.TS,
	onTombstone func(tombstone logtailreplay.ObjectEntry) (next bool, err error),
	pState *logtailreplay.PartitionState,
) error {
	iter, err := pState.NewObjectsIter(ts, true, true)
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Next() {
		obj := iter.Entry()
		if next, err := onTombstone(obj); !next || err != nil {
			return err
		}
	}

	return nil
}

func ForeachSnapshotObjects(
	ts timestamp.Timestamp,
	onObject func(obj logtailreplay.ObjectInfo, isCommitted bool) error,
	tableSnapshot *logtailreplay.PartitionState,
	extraCommitted []objectio.ObjectStats,
	uncommitted ...objectio.ObjectStats,
) (err error) {
	// process all uncommitted objects first
	for _, obj := range uncommitted {
		info := logtailreplay.ObjectInfo{
			ObjectStats: obj,
		}
		if err = onObject(info, false); err != nil {
			return
		}
	}
	// process all uncommitted objects first
	for _, obj := range extraCommitted {
		info := logtailreplay.ObjectInfo{
			ObjectStats: obj,
		}
		if err = onObject(info, true); err != nil {
			return
		}
	}

	// process all committed objects
	if tableSnapshot == nil {
		return
	}

	iter, err := tableSnapshot.NewObjectsIter(types.TimestampToTS(ts), true, false)
	if err != nil {
		return
	}
	defer iter.Close()
	for iter.Next() {
		obj := iter.Entry()
		if err = onObject(obj.ObjectInfo, true); err != nil {
			return
		}
	}
	return
}

func ConstructObjStatsByLoadObjMeta(
	ctx context.Context,
	metaLoc objectio.Location,
	fs fileservice.FileService,
) (stats objectio.ObjectStats, dataMeta objectio.ObjectDataMeta, err error) {

	// 1. load object meta
	var meta objectio.ObjectMeta
	if meta, err = objectio.FastLoadObjectMeta(ctx, &metaLoc, false, fs); err != nil {
		logutil.Error("fast load object meta failed when split object stats. ", zap.Error(err))
		return
	}
	dataMeta = meta.MustDataMeta()

	// 2. construct an object stats
	objectio.SetObjectStatsObjectName(&stats, metaLoc.Name())
	objectio.SetObjectStatsExtent(&stats, metaLoc.Extent())
	objectio.SetObjectStatsBlkCnt(&stats, dataMeta.BlockCount())

	sortKeyIdx := dataMeta.BlockHeader().SortKey()
	objectio.SetObjectStatsSortKeyZoneMap(&stats, dataMeta.MustGetColumn(sortKeyIdx).ZoneMap())

	totalRows := uint32(0)
	for idx := uint32(0); idx < dataMeta.BlockCount(); idx++ {
		totalRows += dataMeta.GetBlockMeta(idx).GetRows()
	}

	objectio.SetObjectStatsRowCnt(&stats, totalRows)

	return
}

// txnIsValid
// if the workspace is nil or txnOp is aborted, it returns error
func txnIsValid(txnOp client.TxnOperator) (*Transaction, error) {
	if txnOp == nil {
		return nil, moerr.NewInternalErrorNoCtx("txnOp is nil")
	}
	ws := txnOp.GetWorkspace()
	if ws == nil {
		return nil, moerr.NewInternalErrorNoCtx("txn workspace is nil")
	}
	var wsTxn *Transaction
	var ok bool
	if wsTxn, ok = ws.(*Transaction); ok {
		if wsTxn == nil {
			return nil, moerr.NewTxnClosedNoCtx(txnOp.Txn().ID)
		}
	}
	//if it is not the Transaction instance, only check the txnOp
	if txnOp.Status() == txn.TxnStatus_Aborted {
		return nil, moerr.NewTxnClosedNoCtx(txnOp.Txn().ID)
	}
	return wsTxn, nil
}

func CheckTxnIsValid(txnOp client.TxnOperator) (err error) {
	_, err = txnIsValid(txnOp)
	return err
}

// concurrentTask is the task that runs in the concurrent executor.
type concurrentTask func() error

// ConcurrentExecutor is an interface that runs tasks concurrently.
type ConcurrentExecutor interface {
	// AppendTask append the concurrent task to the exuecutor.
	AppendTask(concurrentTask)
	// Run starts receive task to execute.
	Run(context.Context)
	// GetConcurrency returns the concurrency of this executor.
	GetConcurrency() int
}

type concurrentExecutor struct {
	// concurrency is the concurrency to run the tasks at the same time.
	concurrency int
	// task contains all the tasks needed to run.
	tasks chan concurrentTask
}

func newConcurrentExecutor(concurrency int) ConcurrentExecutor {
	return &concurrentExecutor{
		concurrency: concurrency,
		tasks:       make(chan concurrentTask, 2048),
	}
}

// AppendTask implements the ConcurrentExecutor interface.
func (e *concurrentExecutor) AppendTask(t concurrentTask) {
	e.tasks <- t
}

// Run implements the ConcurrentExecutor interface.
func (e *concurrentExecutor) Run(ctx context.Context) {
	for i := 0; i < e.concurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return

				case t := <-e.tasks:
					if err := t(); err != nil {
						logutil.Errorf("failed to execute task: %v", err)
					}
				}
			}
		}()
	}
}

// GetConcurrency implements the ConcurrentExecutor interface.
func (e *concurrentExecutor) GetConcurrency() int {
	return e.concurrency
}

// removeIf removes the elements that pred is true.
func removeIf[T any](data []T, pred func(t T) bool) []T {
	if len(data) == 0 {
		return data
	}
	res := 0
	for i := 0; i < len(data); i++ {
		if !pred(data[i]) {
			if res != i {
				data[res] = data[i]
			}
			res++
		}
	}
	return data[:res]
}

func stringifySlice(req any, f func(any) string) string {
	buf := &bytes.Buffer{}
	v := reflect.ValueOf(req)
	buf.WriteRune('[')
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				buf.WriteRune(',')
			}
			buf.WriteString(f(v.Index(i).Interface()))
		}
	}
	buf.WriteRune(']')
	buf.WriteString(fmt.Sprintf("[%d]", v.Len()))
	return buf.String()
}

func stringifyMap(req any, f func(any, any) string) string {
	buf := &bytes.Buffer{}
	v := reflect.ValueOf(req)
	buf.WriteRune('{')
	if v.Kind() == reflect.Map {
		keys := v.MapKeys()
		for i, key := range keys {
			if i > 0 {
				buf.WriteRune(',')
			}
			buf.WriteString(f(key.Interface(), v.MapIndex(key).Interface()))
		}
	}
	buf.WriteRune('}')
	buf.WriteString(fmt.Sprintf("[%d]", v.Len()))
	return buf.String()
}

func execReadSql(ctx context.Context, op client.TxnOperator, sql string, disableLog bool) (executor.Result, error) {
	// copy from compile.go runSqlWithResult
	service := op.GetWorkspace().(*Transaction).proc.GetService()
	v, ok := moruntime.ServiceRuntime(service).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic(fmt.Sprintf("missing sql executor in service %q", service))
	}
	exec := v.(executor.SQLExecutor)
	proc := op.GetWorkspace().(*Transaction).proc
	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(op).
		WithTimeZone(proc.GetSessionInfo().TimeZone)
	if disableLog {
		opts = opts.WithStatementOption(executor.StatementOption{}.WithDisableLog())
	}
	return exec.Exec(ctx, sql, opts)
}

func fillTsVecForSysTableQueryBatch(bat *batch.Batch, ts types.TS, m *mpool.MPool) error {
	tsvec := vector.NewVec(types.T_TS.ToType())
	for i := 0; i < bat.RowCount(); i++ {
		if err := vector.AppendFixed(tsvec, ts, false, m); err != nil {
			tsvec.Free(m)
			return err
		}
	}
	bat.Vecs = append([]*vector.Vector{bat.Vecs[0] /*rowid*/, tsvec}, bat.Vecs[1:]...)
	return nil
}

func isColumnsBatchPerfectlySplitted(bs []*batch.Batch) bool {
	tidIdx := cache.MO_OFF + catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX
	if len(bs) == 1 {
		return true
	}
	prevTableId := vector.GetFixedAtNoTypeCheck[uint64](bs[0].Vecs[tidIdx], bs[0].RowCount()-1)
	for _, b := range bs[1:] {
		firstId := vector.GetFixedAtNoTypeCheck[uint64](b.Vecs[tidIdx], 0)
		if firstId == prevTableId {
			return false
		}
		prevTableId = vector.GetFixedAtNoTypeCheck[uint64](b.Vecs[tidIdx], b.RowCount()-1)
	}
	return true
}
