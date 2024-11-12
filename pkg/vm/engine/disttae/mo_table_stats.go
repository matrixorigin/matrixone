// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// considerations:
// 1. task restart from very old lastUpdateTS causing huge number of subscribe/update stats.
//		a. getChangeTableList request carries a Limit parameter. this requires strongly the changed table list
//     		collecting according their ts.
// 2. get changed table list: [from, now], but the [from, truncate] deleted from memory.
// 		a. collect from checkpoints
// 3. user can stop this task or force update some tables' stats.
//

const (
	TableStatsTableSize = iota
	TableStatsTableRows

	TableStatsTObjectCnt
	TableStatsDObjectCnt

	TableStatsTBlockCnt
	TableStatsDBlockCnt

	TableStatsCnt
)

const (
	insertOrUpdateSQL = `
				insert into 
				    %s.%s (account_id, database_id, table_id, database_name, table_name, table_stats, update_time, takes)
					values(%d, %d, %d, '%s', '%s', '%s', '%s', %d)
				on duplicate key update
					table_stats = '%s', update_time = '%s', takes = %d;`

	lastUpdateTSSQL = `select update_time from %s.%s where account_id=-1 and database_id=-1 and table_id=-1;`

	getTableStatsSQL = `
				select 
    				table_id, update_time, COALESCE(table_stats, CAST('{}' AS JSON)) from  %s.%s
              	where 
                	account_id in (%v) and 
                	database_id in (%v) and 
                  	table_id in (%v) 
			  	order by table_id asc;`
)

const (
	defaultAlphaCycleDur = time.Minute

	defaultGetTableListLimit = 100
	defaultForceUpdateGap    = time.Minute * 5

	updateTSAccountId  = -1
	updateTSDatabaseId = -1
	updateTSTableId    = -1
)

var TableStatsName = [TableStatsCnt]string{
	"table_size",
	"table_rows",
	"tobject_cnt",
	"dobject_cnt",
	"tblock_cnt",
	"dblock_cnt",
}

type Config struct {
	UpdateDuration    time.Duration `json:"update_duration"`
	GetTableListLimit int           `json:"get_table_list_limit"`
	ForceUpdateGap    time.Duration `json:"force_update_gap"`
}

var dynamicConfig struct {
	tblQueue chan *tablePair

	de                   *Engine
	service              string
	alphaCycleDur        time.Duration
	forceUpdateGap       time.Duration
	initialized          bool
	objIdsPool           sync.Pool
	sqlExecFactory       func() ie.InternalExecutor
	changeTableListLimit int

	tableStock struct {
		tbls   []*tablePair
		newest types.TS
	}
}

////////////////// MoTableStats Interface //////////////////

func intsJoin(items []uint64, delimiter string) string {
	str := ""
	for i := range items {
		str += fmt.Sprintf("%d", items[i])
		if i < len(items)-1 {
			str += delimiter
		}
	}
	return str
}

func queryStats(
	ctx context.Context,
	statsIdx int,
	defaultVal any,
	accs, dbs, tbls []uint64,
) (statsVals []any, err error) {

	execSQL := func(inputAccId, inputDbIds, inputTblIds []uint64) ie.InternalExecResult {
		opts := ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()

		sql := fmt.Sprintf(getTableStatsSQL,
			catalog.MO_CATALOG,
			catalog.MO_TABLE_STATS,
			intsJoin(inputAccId, ","),
			intsJoin(inputDbIds, ","),
			intsJoin(inputTblIds, ","))

		// tricky here, only sys can visit this table,
		// but there should have not any privilege leak, since the [acc, dbs, tbls] already checked.
		sysCtx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)

		return dynamicConfig.sqlExecFactory().Query(sysCtx, sql, opts)
	}

	ret := execSQL(accs, dbs, tbls)
	if err = ret.Error(); err != nil {
		return
	}

	var (
		idxes   []uint64
		gotTIds []int64
		stats   map[string]any
		val     interface{}

		tblIdColIdx  = uint64(0)
		updateColIdx = uint64(1)
		statsColIdx  = uint64(2)

		now     = time.Now()
		updates []time.Time
	)

	for i := range ret.RowCount() {
		if val, err = ret.Value(ctx, i, tblIdColIdx); err != nil {
			return
		}

		idxes = append(idxes, i)
		gotTIds = append(gotTIds, val.(int64))
	}

	// scan update time and stats val
	for i := range tbls {
		idx, found := sort.Find(len(gotTIds), func(j int) int { return int(tbls[i]) - int(gotTIds[j]) })

		if !found {
			updates = append(updates, now)
			statsVals = append(statsVals, defaultVal)
			continue
		}

		if val, err = ret.Value(ctx, idxes[idx], statsColIdx); err != nil {
			return
		}

		if err = json.Unmarshal([]byte(val.(bytejson.ByteJson).String()), &stats); err != nil {
			return
		}

		statsVals = append(statsVals, stats[TableStatsName[statsIdx]])

		if val, err = ret.Value(ctx, idxes[idx], updateColIdx); err != nil {
			return
		}

		var ud time.Time
		ud, err = time.Parse("2006-01-02 15:04:05.000000", val.(string))
		if err != nil {
			return
		}

		updates = append(updates, ud)
	}

	recordForceUpdateErr := func(tp *tablePair, err error) {
		logutil.Info("force update old stats failed",
			zap.Error(err),
			zap.String("table", fmt.Sprintf("%s(%d)-%s(%d)", tp.dbName, tp.db, tp.tblName, tp.tbl)))
	}

	var forceUpdatedCnt int
	// for too old stats, we force launch an update,
	// the number of force update also subject to Limit.
	for i := range tbls {
		if now.Sub(updates[i]) > dynamicConfig.forceUpdateGap {
			if forceUpdatedCnt >= dynamicConfig.changeTableListLimit {
				logutil.Info("force update stats reached limit",
					zap.Int("limit", dynamicConfig.changeTableListLimit))
				break
			}

			// force update
			tblItem := dynamicConfig.de.
				GetLatestCatalogCache().
				GetTableById(uint32(accs[i]), dbs[i], tbls[i])

			tp := allocateTablePair()
			tp.acc = int64(accs[i])
			tp.db = int64(dbs[i])
			tp.tbl = int64(tbls[i])
			tp.snapshot = types.BuildTS(now.UnixNano(), 0)
			tp.tblName = tblItem.Name
			tp.dbName = tblItem.DatabaseName
			tp.pkSequence = tblItem.PrimarySeqnum

			tp.pState, err = subscribeTable(ctx, dynamicConfig.service, dynamicConfig.de, tp)
			if err != nil {
				recordForceUpdateErr(tp, err)
				continue
			}

			dynamicConfig.tblQueue <- tp

			if err = tp.Wait(); err != nil {
				recordForceUpdateErr(tp, err)
				continue
			}

			// reread from table
			ret = execSQL([]uint64{accs[i]}, []uint64{dbs[i]}, []uint64{tbls[i]})
			if err = ret.Error(); err != nil {
				recordForceUpdateErr(tp, err)
				continue
			}

			if val, err = ret.Value(ctx, 0, statsColIdx); err != nil {
				recordForceUpdateErr(tp, err)
				continue
			}

			if err = json.Unmarshal([]byte(val.(bytejson.ByteJson).String()), &stats); err != nil {
				recordForceUpdateErr(tp, err)
				continue
			}

			forceUpdatedCnt++
			statsVals[i] = stats[TableStatsName[statsIdx]]
		}
	}

	return statsVals, nil
}

func MTSTableSize(
	ctx context.Context, server string,
	accs, dbs, tbls []uint64,
) (sizes []uint64, err error) {

	fmt.Println("mo_table_size request", len(tbls))

	statsVals, err := queryStats(ctx, TableStatsTableSize, float64(0), accs, dbs, tbls)
	if err != nil {
		return nil, err
	}

	for i := range statsVals {
		sizes = append(sizes, uint64(statsVals[i].(float64)))
	}

	return
}

func MTSTableRows(
	ctx context.Context, server string,
	accs, dbs, tbls []uint64,
) (sizes []uint64, err error) {

	statsVals, err := queryStats(ctx, TableStatsTableRows, float64(0), accs, dbs, tbls)
	if err != nil {
		return nil, err
	}

	for i := range statsVals {
		sizes = append(sizes, uint64(statsVals[i].(float64)))
	}

	return
}

/////////////// MoTableStats Implementation ///////////////

type tablePair struct {
	waiter struct {
		cond     sync.Cond
		err      error
		errQueue chan error
	}

	pkSequence int

	acc, db, tbl    int64
	dbName, tblName string

	snapshot types.TS
	pState   *logtailreplay.PartitionState
}

func allocateTablePair() *tablePair {
	tp := tablePair{}
	tp.waiter.cond = *sync.NewCond(new(sync.Mutex))
	return &tp
}

func (tp *tablePair) String() string {
	return fmt.Sprintf("%d-%s(%d)-%s(%d)",
		tp.acc, tp.dbName, tp.db, tp.tblName, tp.tbl)
}

func (tp *tablePair) Done(err error) {
	tp.waiter.err = err
	if tp.waiter.errQueue != nil {
		select {
		// un blocking
		// full or closed
		case tp.waiter.errQueue <- err:
		}
	}

	tp.waiter.cond.Broadcast()
}

func (tp *tablePair) Wait() error {
	tp.waiter.cond.L.Lock()
	defer tp.waiter.cond.L.Unlock()

	tp.waiter.cond.Wait()
	if tp.waiter.errQueue != nil && len(tp.waiter.errQueue) != 0 {
		return <-tp.waiter.errQueue
	}
	return tp.waiter.err
}

type statsList struct {
	took  time.Duration
	stats map[string]any
}

type betaCycleStash struct {
	snapshot types.TS

	born time.Time

	dataObjIds []types.Objectid

	totalSize   float64
	totalRows   float64
	deletedRows float64

	tblockCnt, dblockCnt   int
	tobjectCnt, dobjectCnt int
}

func stashToStats(bcs betaCycleStash) statsList {
	var (
		sl statsList
	)

	sl.stats = make(map[string]any)

	// table rows
	{
		leftRows := bcs.totalRows - bcs.deletedRows
		sl.stats[TableStatsName[TableStatsTableRows]] = leftRows
	}

	// table size
	{
		deletedSize := float64(0)
		if bcs.totalRows > 0 && bcs.deletedRows > 0 {
			deletedSize = bcs.totalSize / bcs.totalRows * bcs.deletedRows
		}

		leftSize := math.Round((bcs.totalSize-deletedSize)*1000) / 1000
		sl.stats[TableStatsName[TableStatsTableSize]] = leftSize
	}

	// data object, block count
	// tombstone object, block count
	{
		sl.stats[TableStatsName[TableStatsTObjectCnt]] = bcs.tobjectCnt
		sl.stats[TableStatsName[TableStatsTBlockCnt]] = bcs.tblockCnt
		sl.stats[TableStatsName[TableStatsDObjectCnt]] = bcs.dobjectCnt
		sl.stats[TableStatsName[TableStatsDBlockCnt]] = bcs.dblockCnt
	}

	sl.took = time.Since(bcs.born)

	return sl
}

func initialize(
	ctx context.Context,
	server string,
	conf Config,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {

	if dynamicConfig.initialized {
		return
	}

	go func() {
		if err = betaTask(ctx, server, eng, sqlExecutor); err != nil {
			return
		}
	}()

	if err != nil {
		return
	}

	dynamicConfig.de = eng.(*Engine)
	dynamicConfig.initialized = true
	dynamicConfig.sqlExecFactory = sqlExecutor
	if conf.GetTableListLimit <= 0 {
		dynamicConfig.changeTableListLimit = defaultGetTableListLimit
	} else {
		dynamicConfig.changeTableListLimit = conf.GetTableListLimit
	}

	if conf.ForceUpdateGap <= 0 {
		dynamicConfig.forceUpdateGap = defaultForceUpdateGap
	} else {
		dynamicConfig.forceUpdateGap = conf.ForceUpdateGap
	}

	if conf.UpdateDuration <= 0 {
		dynamicConfig.alphaCycleDur = defaultAlphaCycleDur
	} else {
		dynamicConfig.alphaCycleDur = conf.UpdateDuration
	}

	dynamicConfig.objIdsPool = sync.Pool{
		New: func() interface{} {
			return make([]types.Objectid, 0)
		},
	}

	// the queue length also decides the parallelism of subscription
	dynamicConfig.tblQueue = make(chan *tablePair,
		min(10, dynamicConfig.changeTableListLimit/5))

	{
		ff1 := func() func(
			context.Context, string, []uint64, []uint64, []uint64) ([]uint64, error) {
			return MTSTableSize
		}
		function.GetMoTableSizeFunc.Store(&ff1)

		ff2 := func() func(
			context.Context, string, []uint64, []uint64, []uint64) ([]uint64, error) {
			return MTSTableRows
		}
		function.GetMoTableRowsFunc.Store(&ff2)
	}

	return nil
}

func GetMOTableStatsExecutor(
	service string,
	conf Config,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) func(ctx context.Context, task task.Task) error {
	return func(ctx context.Context, task task.Task) error {
		return tableStatsExecutor(ctx, service, conf, eng, sqlExecutor)
	}
}

func tableStatsExecutor(
	ctx context.Context,
	service string,
	conf Config,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {

	if val := ctx.Value(defines.TenantIDKey{}); val == nil {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}

	if err = initialize(ctx, service, conf, eng, sqlExecutor); err != nil {
		return
	}

	defer func() {
		for len(dynamicConfig.tblQueue) > 0 {
			tbl := <-dynamicConfig.tblQueue
			tbl.Done(nil)
		}
	}()

	tickerDur := time.Second
	executeTicker := time.NewTicker(tickerDur)

	for {
		select {
		case <-ctx.Done():
			logutil.Info("table stats executor exit by ctx.Done", zap.Error(ctx.Err()))
			err = ctx.Err()
			break

		case <-executeTicker.C:
			if err = alphaTask(ctx, service, eng, sqlExecutor); err != nil {
				logutil.Info("table stats alpha exit by err", zap.Error(err))
				break
			}

			executeTicker.Reset(dynamicConfig.alphaCycleDur / 10)
		}

		if err != nil {
			break
		}
	}

	return err
}

func alphaTask(
	ctx context.Context,
	service string,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {

	var (
		newCtx context.Context
		cancel context.CancelFunc

		ticker    *time.Ticker
		processed int

		lastUpdateTS types.TS
		pState       *logtailreplay.PartitionState
	)

	if _, ok := ctx.Deadline(); !ok {
		newCtx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
	}

	if lastUpdateTS, err = getLastUpdateTS(newCtx, sqlExecutor); err != nil {
		return err
	}

	err = getChangedTableList(newCtx, service, eng, lastUpdateTS)
	defer func() {
		if len(dynamicConfig.tableStock.tbls) == 0 {
			err = updateLastUpdateTS(ctx, dynamicConfig.tableStock.newest, service, sqlExecutor)
			return
		}
	}()

	if err != nil {
		return err
	}

	errQueue := make(chan error, 10)
	ticker = time.NewTicker(time.Millisecond * 10)

	for range ticker.C {
		select {
		case <-ctx.Done():
			return nil

		case err = <-errQueue:
			return err

		default:
			if processed >= dynamicConfig.changeTableListLimit ||
				len(dynamicConfig.tableStock.tbls) == 0 {
				return
			}

			if pState, err = subscribeTable(ctx, service, eng, dynamicConfig.tableStock.tbls[0]); err != nil {
				return err
			} else if pState == nil {
				continue
			}
			pState = pState.Copy()

			dynamicConfig.tableStock.tbls[0].pState = pState
			dynamicConfig.tableStock.tbls[0].snapshot = dynamicConfig.tableStock.newest
			dynamicConfig.tableStock.tbls[0].waiter.errQueue = errQueue
			dynamicConfig.tblQueue <- dynamicConfig.tableStock.tbls[0]

			processed++
			dynamicConfig.tableStock.tbls = dynamicConfig.tableStock.tbls[1:]
			ticker.Reset(time.Millisecond * 10)
		}
	}

	return nil
}

func betaTask(
	ctx context.Context,
	service string,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {

	var (
		de       = eng.(*Engine)
		taskPool *ants.Pool
	)

	if taskPool, err = ants.NewPool(2); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return

		case tbl := <-dynamicConfig.tblQueue:
			op := func() {
				defer func() {
					tbl.Done(err)
				}()

				bcs := betaCycleStash{
					born:       time.Now(),
					snapshot:   tbl.snapshot,
					dataObjIds: dynamicConfig.objIdsPool.Get().([]types.Objectid),
				}

				defer func() {
					bcs.dataObjIds = bcs.dataObjIds[:0]
					dynamicConfig.objIdsPool.Put(bcs.dataObjIds)
				}()

				if err = collectVisibleData(&bcs, tbl.pState); err != nil {
					return
				}

				if err = applyTombstones(ctx, &bcs, de.fs, tbl.pState); err != nil {
					return
				}

				sl := stashToStats(bcs)
				if err = updateStatsTable(ctx, service, tbl, sl, sqlExecutor); err != nil {
					return
				}
			}

			if err = taskPool.Submit(op); err != nil {
				logutil.Info("stats beta task exist", zap.Error(err))
				return
			}
		}
	}
}

func getChangedTableList(
	ctx context.Context,
	service string,
	eng engine.Engine,
	from types.TS,
) (err error) {

	if len(dynamicConfig.tableStock.tbls) > 0 {
		return
	}

	defer func() {
		logutil.Info("get changed table list",
			zap.Time("from", from.ToTimestamp().ToStdTime()),
			zap.Int("cnt", len(dynamicConfig.tableStock.tbls)))
	}()

	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payload := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		req := cmd_util.GetChangedTableListReq{}
		ts := from.ToTimestamp()
		req.From = &ts
		req.Limit = defaultGetTableListLimit
		return req.Marshal()
	}

	responseUnmarshaler := func(payload []byte) (any, error) {
		list := &cmd_util.GetChangedTableListResp{}
		if err = list.Unmarshal(payload); err != nil {
			return nil, err
		}
		return list, nil
	}

	de := eng.(*Engine)
	txnOperator, err := de.cli.New(ctx, timestamp.Timestamp{})
	if err != nil {
		return err
	}

	proc := process.NewTopProcess(ctx,
		de.mp, de.cli, txnOperator,
		de.fs, de.ls,
		de.qc, de.hakeeper,
		de.us, nil,
	)

	fmt.Println("get table request")
	handler := ctl.GetTNHandlerFunc(api.OpCode_OpGetChangedTableList, whichTN, payload, responseUnmarshaler)
	ret, err := handler(proc, "DN", "", ctl.MoCtlTNCmdSender)
	if err != nil {
		return err
	}

	cc := de.GetLatestCatalogCache()

	resp := ret.Data.([]any)[0].(*cmd_util.GetChangedTableListResp)

	fmt.Println("get table stats", len(resp.AccIds))

	if len(resp.AccIds) == 0 {
		return nil
	}

	for i := range resp.AccIds {
		item := cc.GetTableById(uint32(resp.AccIds[i]), resp.DatabaseIds[i], resp.TableIds[i])
		if item == nil {
			continue
		}

		tp := allocateTablePair()
		tp.pkSequence = item.PrimarySeqnum
		tp.acc = int64(item.AccountId)
		tp.db = int64(item.DatabaseId)
		tp.tbl = int64(item.Id)
		tp.tblName = item.Name
		tp.dbName = item.DatabaseName

		//fmt.Println("get table stats", tp.String())
		dynamicConfig.tableStock.tbls = append(dynamicConfig.tableStock.tbls, tp)
	}

	dynamicConfig.tableStock.newest = types.TimestampToTS(*resp.Newest)

	return nil
}

func subscribeTable(
	ctx context.Context,
	service string,
	eng engine.Engine,
	tbl *tablePair,
) (pState *logtailreplay.PartitionState, err error) {

	txnTbl := txnTable{}
	txnTbl.tableId = uint64(tbl.tbl)
	txnTbl.tableName = tbl.tblName
	txnTbl.accountId = uint32(tbl.acc)
	txnTbl.db = &txnDatabase{
		databaseId:   uint64(tbl.db),
		databaseName: tbl.dbName,
	}

	txnTbl.primarySeqnum = tbl.pkSequence

	if pState, err = eng.(*Engine).PushClient().toSubscribeTable(ctx, &txnTbl); err != nil {
		return nil, err
	}

	return pState, nil
}

func getLastUpdateTS(
	ctx context.Context,
	sqlExecutor func() ie.InternalExecutor,
) (types.TS, error) {

	// force update logic may distribute the continuity of the update time,
	// so a special record needed to remember which step the normal routine arrived.

	opts := ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()
	ret := sqlExecutor().Query(ctx, fmt.Sprintf(lastUpdateTSSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS), opts)
	if ret.Error() != nil {
		return types.TS{}, ret.Error()
	}

	if ret.RowCount() == 0 {
		return types.TS{}, nil
	}

	val, err := ret.Value(ctx, 0, 0)
	if err != nil {
		return types.TS{}, err
	}

	if val == nil {
		return types.TS{}, nil
	}

	tt, err := time.Parse("2006-01-02 15:04:05.000000", val.(string))
	if err != nil {
		return types.TS{}, err
	}

	lastUpdateTS := types.BuildTS(tt.UnixNano(), 0)
	return lastUpdateTS, nil
}

func updateLastUpdateTS(
	ctx context.Context,
	snapshot types.TS,
	service string,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {
	tbl := tablePair{
		tbl:      updateTSTableId,
		db:       updateTSDatabaseId,
		acc:      updateTSAccountId,
		snapshot: snapshot,
	}

	if err = updateStatsTable(
		ctx, service, &tbl, statsList{}, sqlExecutor); err != nil {
		return err
	}
	return nil
}

// O(m+n)
func getDeletedRows(
	objIds []types.Objectid,
	rowIds []types.Rowid,
) (deletedCnt int) {

	var (
		i int
		j int
	)

	for i < len(objIds) && j < len(rowIds) {
		if j > 0 && rowIds[j-1].EQ(&rowIds[j]) {
			j++
			continue
		}

		cmp := rowIds[j].BorrowObjectID().Compare(&objIds[i])

		if cmp == 0 {
			deletedCnt++
			j++
		} else if cmp > 0 {
			i++
		} else {
			// cmp < 0
			j++
		}
	}

	return deletedCnt
}

func collectVisibleData(
	bcs *betaCycleStash,
	pState *logtailreplay.PartitionState,
) (err error) {

	var (
		dRowIter logtailreplay.RowsIter
		dObjIter logtailreplay.ObjectsIter

		estimatedOneRowSize float64
	)

	defer func() {
		if dRowIter != nil {
			err = dRowIter.Close()
		}

		if dObjIter != nil {
			err = dObjIter.Close()
		}
	}()

	dObjIter, err = pState.NewObjectsIter(bcs.snapshot, true, false)
	if err != nil {
		return err
	}

	dRowIter = pState.NewRowsIter(bcs.snapshot, nil, false)

	// there won't exist visible appendable obj
	for dObjIter.Next() {
		obj := dObjIter.Entry()

		bcs.dobjectCnt++
		bcs.dblockCnt += int(obj.BlkCnt())

		bcs.totalSize += float64(obj.Size())
		bcs.totalRows += float64(obj.Rows())
		bcs.dataObjIds = append(
			bcs.dataObjIds, *obj.ObjectStats.ObjectName().ObjectId())
	}

	if bcs.totalRows != 0 {
		estimatedOneRowSize = bcs.totalSize / bcs.totalRows
	}

	// 1. inserts on appendable object
	for dRowIter.Next() {
		entry := dRowIter.Entry()

		idx := slices.IndexFunc(bcs.dataObjIds, func(objId types.Objectid) bool {
			return objId.EQ(entry.BlockID.Object())
		})

		if idx == -1 {
			bcs.dataObjIds = append(bcs.dataObjIds, *entry.BlockID.Object())
		}

		bcs.totalRows += float64(1)
		if estimatedOneRowSize > 0 {
			bcs.totalSize += estimatedOneRowSize
		} else {
			bcs.totalSize += float64(entry.Batch.Size()) / float64(entry.Batch.RowCount())
		}
	}

	return
}

func applyTombstones(
	ctx context.Context,
	bcs *betaCycleStash,
	fs fileservice.FileService,
	pState *logtailreplay.PartitionState,
) (err error) {
	var (
		tObjIter logtailreplay.ObjectsIter

		hidden  objectio.HiddenColumnSelection
		release func()
	)

	tObjIter, err = pState.NewObjectsIter(bcs.snapshot, true, true)
	if err != nil {
		return err
	}

	// 1. non-appendable tombstone obj
	// 2. appendable tombstone obj
	for tObjIter.Next() {
		tombstone := tObjIter.Entry()

		bcs.tobjectCnt++
		bcs.tblockCnt += int(tombstone.BlkCnt())

		attrs := objectio.GetTombstoneAttrs(hidden)
		persistedDeletes := containers.NewVectors(len(attrs))

		ForeachBlkInObjStatsList(true, nil,
			func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {

				if _, release, err = blockio.ReadDeletes(
					ctx, blk.MetaLoc[:], fs, tombstone.GetCNCreated(), persistedDeletes,
				); err != nil {
					return false
				}
				defer release()

				rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](&persistedDeletes[0])
				cnt := getDeletedRows(bcs.dataObjIds, rowIds)

				bcs.deletedRows += float64(cnt)
				//deletedRows += float64(cnt)
				return true
			}, tombstone.ObjectStats)

		//loaded += int(tombstone.BlkCnt())

		if err != nil {
			return
		}
	}

	// if appendable tombstone persisted, deletes on it already eliminated
	// 1. deletes on appendable object
	// 2. deletes on non-appendable objects
	//
	// here, we only collect the deletes that have not paired inserts
	// according to its LESS function, the deletes come first
	var lastInsert logtailreplay.RowEntry
	err = pState.ScanRows(true, func(entry logtailreplay.RowEntry) (bool, error) {
		if !entry.Deleted {
			lastInsert = entry
			return true, nil
		}

		if entry.RowID.EQ(&lastInsert.RowID) {
			return true, nil
		}

		bcs.deletedRows += float64(getDeletedRows(bcs.dataObjIds, []types.Rowid{entry.RowID}))
		//deletedRows += float64(getDeletedRows(moTableStats.bcs.dataObjIds, []types.Rowid{entry.RowID}))

		return true, nil
	})

	//return deletedRows, loaded, err
	return nil
}

func updateStatsTable(
	ctx context.Context,
	service string,
	tbl *tablePair,
	sl statsList,
	executor func() ie.InternalExecutor,
) (err error) {

	var (
		val []byte
		ret ie.InternalExecResult
	)

	opts := ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()

	if val, err = json.Marshal(sl.stats); err != nil {
		return err
	}

	utcTime := tbl.snapshot.
		ToTimestamp().
		ToStdTime().
		Format("2006-01-02 15:04:05.000000")

	sql := fmt.Sprintf(insertOrUpdateSQL,
		catalog.MO_CATALOG,
		catalog.MO_TABLE_STATS,
		tbl.acc, tbl.db, tbl.tbl,
		tbl.dbName, tbl.tblName,
		string(val), utcTime, sl.took.Microseconds(),
		string(val), utcTime, sl.took.Microseconds())

	ret = executor().Query(ctx, sql, opts)

	if err = ret.Error(); err != nil {
		return err
	}

	return nil
}
