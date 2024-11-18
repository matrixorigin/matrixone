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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"math"
	"runtime"
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
	//"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
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
	insertOrUpdateStatsListSQL = `
				insert into 
				    %s.%s (account_id, database_id, table_id, database_name, table_name, table_stats, update_time, takes)
					values(%d, %d, %d, '%s', '%s', '%s', '%s', %d)
				on duplicate key update
					table_stats = '%s', update_time = '%s', takes = %d;`

	UpdateOnlyTSSQL = `
                update 
					%s.%s
				set 
				    update_time = '%s'
				where
				    account_id = %d and 
				    database_id = %d and 
				    table_id = %d;`

	getTableStatsSQL = `
				select 
    				table_id, update_time, COALESCE(table_stats, CAST('{}' AS JSON)) from  %s.%s
              	where 
                	account_id in (%v) and 
                	database_id in (%v) and 
                  	table_id in (%v) 
			  	order by table_id asc;`

	getCandidatesSQL = `
				select
					account_id, database_id, table_id, update_time 
				from 
				    %s.%s
				order by 
				    update_time asc
				limit %d;`

	getUpdateTSSQL = `
				select
					table_id, update_time
				from 
					%s.%s
				where 
					account_id in (%v) and
				    database_id in (%v) and
				    table_id in (%v);`

	getNewTablesSQL = `
                select 
					account_id, reldatabase, reldatabase_id, relname, rel_id 
				from 
					%s.%s
				where
					created_time >= '%s'
				group by
				    account_id, reldatabase, relname;`

	insertNewTablesSQL = `
				insert into 
				    %s.%s (account_id, database_id, table_id, database_name, table_name, table_stats, update_time, takes)
					values(%d, %d, %d, '%s', '%s', '{}', '1970-01-01 23:59:59.999999', 0)
				on duplicate key update
					0=1;`
)

const (
	defaultAlphaCycleDur = time.Minute

	defaultGetTableListLimit = 1000
)

var TableStatsName = [TableStatsCnt]string{
	"table_size",
	"table_rows",
	"tobject_cnt",
	"dobject_cnt",
	"tblock_cnt",
	"dblock_cnt",
}

func initMoTableStatsConfig(
	ctx context.Context,
	eng *Engine,
) (err error) {

	if val := ctx.Value(defines.TenantIDKey{}); val == nil {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}

	defer func() {
		if err != nil {
			logutil.Error("init mo table stats config failed", zap.Error(err))
		}
	}()

	go func() {
		if err = betaTask(ctx, eng.service, eng); err != nil {
			return
		}
	}()

	if dynamicConfig.alphaTaskPool, err = ants.NewPool(
		runtime.NumCPU(),
		ants.WithNonblocking(false)); err != nil {
		return
	}

	if dynamicConfig.betaTaskPool, err = ants.NewPool(
		runtime.NumCPU(),
		ants.WithNonblocking(false)); err != nil {
		return
	}

	statsConf := eng.config.statsConf

	dynamicConfig.sqlExecFunc = eng.config.sqlExecFunc
	dynamicConfig.sqlOpts = ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()

	if statsConf.GetTableListLimit <= 0 {
		dynamicConfig.changeTableListLimit = defaultGetTableListLimit
	} else {
		dynamicConfig.changeTableListLimit = statsConf.GetTableListLimit
	}

	if statsConf.UpdateDuration <= 0 {
		dynamicConfig.alphaCycleDur = defaultAlphaCycleDur
	} else {
		dynamicConfig.alphaCycleDur = statsConf.UpdateDuration
	}

	dynamicConfig.objIdsPool = sync.Pool{
		New: func() interface{} {
			return make([]types.Objectid, 0)
		},
	}

	// the queue length also decides the parallelism of subscription
	dynamicConfig.tblQueue = make(chan *tablePair,
		min(10, dynamicConfig.changeTableListLimit/5))

	// registerMoTableSizeRows
	{
		ff1 := func() func(
			context.Context,
			[]uint64, []uint64, []uint64,
			engine.Engine, bool) ([]uint64, error) {
			return MTSTableSize
		}
		function.GetMoTableSizeFunc.Store(&ff1)

		ff2 := func() func(
			context.Context,
			[]uint64, []uint64, []uint64,
			engine.Engine, bool) ([]uint64, error) {
			return MTSTableRows
		}
		function.GetMoTableRowsFunc.Store(&ff2)
	}

	dynamicConfig.tableStock.tbls = make([]*tablePair, 0, 1)
	return nil
}

type MoTableStatsConfig struct {
	UpdateDuration    time.Duration `toml:"update-duration"`
	GetTableListLimit int           `toml:"get-table-list-limit"`
	ForceUpdateGap    time.Duration `toml:"force-update-gap"`
	StatsUsingOldImpl bool          `toml:"stats-using-old-impl"`
}

var dynamicConfig struct {
	tblQueue chan *tablePair

	de            *Engine
	service       string
	alphaCycleDur time.Duration
	objIdsPool    sync.Pool

	changeTableListLimit int

	tableStock struct {
		tbls   []*tablePair
		newest types.TS
	}

	lastCheckNewTables types.TS

	betaTaskPool  *ants.Pool
	alphaTaskPool *ants.Pool

	sqlOpts     ie.SessionOverrideOptions
	sqlExecFunc func(context.Context, string, ie.SessionOverrideOptions) ie.InternalExecResult
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

func forceUpdateQuery(
	ctx context.Context,
	statsIdx int,
	defaultVal any,
	accs, dbs, tbls []uint64,
	eng *Engine,
) (statsVals []any, err error) {

	var (
		to      types.TS
		val     any
		stdTime time.Time
		pairs   []*tablePair           = make([]*tablePair, 0, len(tbls))
		oldTS   []*timestamp.Timestamp = make([]*timestamp.Timestamp, len(tbls))
	)

	sql := fmt.Sprintf(getUpdateTSSQL,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
		intsJoin(accs, ","),
		intsJoin(dbs, ","),
		intsJoin(tbls, ","))

	sqlRet := dynamicConfig.sqlExecFunc(ctx, sql, dynamicConfig.sqlOpts)
	if sqlRet.Error() != nil {
		return nil, sqlRet.Error()
	}

	for i := range sqlRet.RowCount() {
		if val, err = sqlRet.Value(ctx, i, 0); err != nil {
			return
		}

		tblId := val.(int64)

		if val, err = sqlRet.Value(ctx, i, 1); err != nil {
			return nil, err
		}

		if stdTime, err = time.Parse("2006-01-02 15:04:05.000000", val.(string)); err != nil {
			return
		}

		idx := slices.Index(tbls, uint64(tblId))
		oldTS[idx] = &timestamp.Timestamp{PhysicalTime: stdTime.UnixNano()}
	}

	if err = getChangedTableList(
		ctx, eng.service, eng, accs, dbs, tbls, oldTS, &pairs, &to); err != nil {
		return
	}

	if err = alphaTask(ctx, eng.service, eng, pairs, to); err != nil {
		return nil, err
	}

	if statsVals, err = normalQuery(
		ctx, statsIdx, defaultVal,
		accs, dbs, tbls); err != nil {
		return nil, err
	}

	return
}

func normalQuery(
	ctx context.Context,
	statsIdx int,
	defaultVal any,
	accs, dbs, tbls []uint64,
) (statsVals []any, err error) {

	sql := fmt.Sprintf(getTableStatsSQL,
		catalog.MO_CATALOG,
		catalog.MO_TABLE_STATS,
		intsJoin(accs, ","),
		intsJoin(dbs, ","),
		intsJoin(tbls, ","))

	sqlRet := dynamicConfig.sqlExecFunc(ctx, sql, dynamicConfig.sqlOpts)
	if sqlRet.Error() != nil {
		return nil, sqlRet.Error()
	}

	var (
		val any

		idxes   []int
		gotTIds []int64
		stats   map[string]any

		tblIdColIdx  = uint64(0)
		updateColIdx = uint64(1)
		statsColIdx  = uint64(2)

		now     = time.Now()
		updates []time.Time
	)

	for i := range sqlRet.RowCount() {
		idxes = append(idxes, int(i))

		if val, err = sqlRet.Value(ctx, i, tblIdColIdx); err != nil {
			return nil, err
		}

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

		if val, err = sqlRet.Value(ctx, uint64(idxes[idx]), statsColIdx); err != nil {
			return nil, err
		}

		if err = json.Unmarshal([]byte(val.(bytejson.ByteJson).String()), &stats); err != nil {
			return
		}

		statsVals = append(statsVals, stats[TableStatsName[statsIdx]])

		if val, err = sqlRet.Value(ctx, uint64(idxes[idx]), updateColIdx); err != nil {
			return nil, err
		}

		var ud time.Time
		if ud, err = time.Parse("2006-01-02 15:04:05.000000", val.(string)); err != nil {
			return
		}

		updates = append(updates, ud)
	}

	return statsVals, nil
}

func queryStats(
	ctx context.Context,
	statsIdx int,
	defaultVal any,
	accs, dbs, tbls []uint64,
	forceUpdate bool,
	eng engine.Engine,
) (statsVals []any, err error) {

	if val := ctx.Value(defines.TenantIDKey{}); val == nil {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}

	if forceUpdate {
		if len(tbls) >= defaultGetTableListLimit {
			logutil.Warn("query stats with force update", zap.Int("tbl-list-length", len(tbls)))
		}
		return forceUpdateQuery(
			ctx, statsIdx, defaultVal,
			accs, dbs, tbls,
			eng.(*engine.EntireEngine).Engine.(*Engine))
	}

	return normalQuery(ctx, statsIdx, defaultVal, accs, dbs, tbls)
}

func MTSTableSize(
	ctx context.Context,
	accs, dbs, tbls []uint64,
	eng engine.Engine,
	forceUpdate bool,
) (sizes []uint64, err error) {

	statsVals, err := queryStats(
		ctx, TableStatsTableSize, float64(0),
		accs, dbs, tbls,
		forceUpdate, eng)
	if err != nil {
		return nil, err
	}

	for i := range statsVals {
		sizes = append(sizes, uint64(statsVals[i].(float64)))
	}

	return
}

func MTSTableRows(
	ctx context.Context,
	accs, dbs, tbls []uint64,
	eng engine.Engine,
	forceUpdate bool,
) (sizes []uint64, err error) {

	statsVals, err := queryStats(
		ctx, TableStatsTableRows, float64(0),
		accs, dbs, tbls,
		forceUpdate, eng)
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

	relKind    string
	pkSequence int

	acc, db, tbl    int64
	dbName, tblName string

	onlyUpdateTS bool

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
		tp.waiter.errQueue <- err
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

func GetMOTableStatsExecutor(
	service string,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) func(ctx context.Context, task task.Task) error {
	return func(ctx context.Context, task task.Task) error {
		return tableStatsExecutor(ctx, service, eng)
	}
}

func tableStatsExecutor(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	if val := ctx.Value(defines.TenantIDKey{}); val == nil {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}

	tickerDur := time.Second
	executeTicker := time.NewTicker(tickerDur)

	for {
		select {
		case <-ctx.Done():
			logutil.Info("table stats executor exit by ctx.Done", zap.Error(ctx.Err()))
			err = ctx.Err()
			break

		case <-executeTicker.C:
			if err = prepare(ctx, service, eng); err != nil {
				break
			}

			if err = alphaTask(
				ctx, service, eng,
				dynamicConfig.tableStock.tbls,
				dynamicConfig.tableStock.newest,
			); err != nil {
				logutil.Info("table stats alpha exit by err", zap.Error(err))
				break
			}

			//if err = updateLastUpdateTS(
			//	ctx, dynamicConfig.tableStock.newest,
			//	service); err != nil {
			//	break
			//}

			dynamicConfig.tableStock.tbls = dynamicConfig.tableStock.tbls[:0]
			executeTicker.Reset(dynamicConfig.alphaCycleDur)
		}

		if err != nil {
			break
		}
	}

	return err
}

func prepare(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	var (
		//lastUpdateTS types.TS

		newCtx context.Context
		cancel context.CancelFunc
	)

	if _, ok := ctx.Deadline(); !ok {
		newCtx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
	}

	accs, dbs, tbls, ts, err := getCandidates(ctx, service, eng)
	if err != nil {
		return
	}

	err = getChangedTableList(
		newCtx, service, eng, accs, dbs, tbls, ts,
		&dynamicConfig.tableStock.tbls,
		&dynamicConfig.tableStock.newest)

	return err
}

func alphaTask(
	ctx context.Context,
	service string,
	eng engine.Engine,
	tbls []*tablePair,
	to types.TS,
) (err error) {

	if len(tbls) == 0 {
		return
	}

	var (
		errWaitToReceive = len(tbls)
		ticker           *time.Ticker
		processed        int
	)

	//fmt.Println("alpha received", len(tbls))

	now := time.Now()
	defer func() {
		if len(tbls) == 0 {
			logutil.Info("alpha processed",
				zap.Int("processed", processed),
				zap.Duration("takes", time.Since(now)))
			//fmt.Printf("procced: %d/%d, left: %d, spent: %v\n",
			//	processed, dynamicConfig.changeTableListLimit, len(dynamicConfig.tableStock.tbls), time.Since(now))
		}
	}()

	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	batCnt := max(len(tbls)/100, 100)

	errQueue := make(chan error, len(tbls)*2)
	ticker = time.NewTicker(time.Millisecond * 10)

	for {
		select {
		case <-ctx.Done():
			return nil

		case err = <-errQueue:
			if err != nil {
				for len(errQueue) != 0 {
					<-errQueue
				}
				return err
			}
			errWaitToReceive--
			if errWaitToReceive <= 0 {
				// all processed
				return nil
			}

		case <-ticker.C:
			if len(tbls) == 0 {
				// all submitted
				//fmt.Println("all submitted, waiting err")
				ticker.Reset(time.Second)
				continue
			}

			start := time.Now()
			submitted := 0
			for i := 0; i < batCnt && i < len(tbls); i++ {

				submitted++
				wg.Add(1)

				dynamicConfig.alphaTaskPool.Submit(func() {
					defer wg.Done()

					var pState *logtailreplay.PartitionState
					if pState, err = subscribeTable(ctx, service, eng, tbls[i]); err != nil {
						return
					}

					tbls[i].pState = pState
					tbls[i].snapshot = to
					tbls[i].waiter.errQueue = errQueue
					dynamicConfig.tblQueue <- tbls[i]
				})
			}

			if submitted == 0 {
				continue
			}

			wg.Wait()

			dur := time.Since(start)
			// the longer the update takes, the longer we would pause,
			// but waiting for 1s at most, 10ms at least.
			ticker.Reset(max(min(dur, time.Millisecond*50), time.Millisecond*5))

			processed += submitted
			tbls = tbls[submitted:]

			//fmt.Printf("wait done, processed: %d/%d, left: %d, spent: %v\n",
			//	processed,
			//	dynamicConfig.changeTableListLimit,
			//	len(tbls),
			//	dur)
		}
	}
}

func betaTask(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	var (
		de = eng.(*Engine)
	)

	for {
		select {
		case <-ctx.Done():
			return

		case tbl := <-dynamicConfig.tblQueue:
			if tbl == nil || tbl.pState == nil {
				tbl.Done(nil)
				continue
			}

			if err = dynamicConfig.betaTaskPool.Submit(func() {
				if tbl.onlyUpdateTS {
					err = updateTableOnlyTS(ctx, service, tbl)
				} else {
					_, err = statsCalculateOp(ctx, service, de.fs, tbl)
				}

				tbl.Done(err)
			}); err != nil {
				tbl.Done(err)
				logutil.Info("stats beta task exist", zap.Error(err))
				return
			}
		}

	}
}

func statsCalculateOp(
	ctx context.Context,
	service string,
	fs fileservice.FileService,
	tbl *tablePair,
) (sl statsList, err error) {

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

	if err = applyTombstones(ctx, &bcs, fs, tbl.pState); err != nil {
		return
	}

	sl = stashToStats(bcs)
	if err = updateTableStatsList(ctx, service, tbl, sl); err != nil {
		return
	}

	return sl, nil
}

func getCandidates(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (accs, dbs, tbls []uint64, ts []*timestamp.Timestamp, err error) {

	sql := fmt.Sprintf(getCandidatesSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, dynamicConfig.changeTableListLimit)
	sqlRet := dynamicConfig.sqlExecFunc(ctx, sql, dynamicConfig.sqlOpts)
	if err = sqlRet.Error(); err != nil {
		return
	}

	var (
		val any
		tm  time.Time
	)

	for i := range sqlRet.RowCount() {
		if val, err = sqlRet.Value(ctx, i, 0); err != nil {
			return
		}
		accs = append(accs, uint64(val.(int64)))

		if val, err = sqlRet.Value(ctx, i, 1); err != nil {
			return
		}
		dbs = append(dbs, uint64(val.(int64)))

		if val, err = sqlRet.Value(ctx, i, 2); err != nil {
			return
		}
		tbls = append(tbls, uint64(val.(int64)))

		if val, err = sqlRet.Value(ctx, i, 3); err != nil {
			return
		}
		if tm, err = time.Parse("2006-01-02 15:04:05.000000", val.(string)); err != nil {
			return
		}

		tt := types.BuildTS(tm.UnixNano(), 0).ToTimestamp()
		ts = append(ts, &tt)
	}

	return
}

func getChangedTableList(
	ctx context.Context,
	service string,
	eng engine.Engine,
	accs []uint64,
	dbs []uint64,
	tbls []uint64,
	ts []*timestamp.Timestamp,
	pairs *[]*tablePair,
	to *types.TS,
) (err error) {

	req := &cmd_util.GetChangedTableListReq{}

	if len(tbls) == 0 {
		return
	}

	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payload := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		for i := range tbls {
			if ts[i] == nil {
				continue
			}

			req.AccIds = append(req.AccIds, accs[i])
			req.DatabaseIds = append(req.DatabaseIds, dbs[i])
			req.TableIds = append(req.TableIds, tbls[i])
			req.From = append(req.From, ts[i])
		}
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
		return
	}

	proc := process.NewTopProcess(ctx,
		de.mp, de.cli, txnOperator,
		de.fs, de.ls,
		de.qc, de.hakeeper,
		de.us, nil,
	)

	//fmt.Println("get table request")
	handler := ctl.GetTNHandlerFunc(api.OpCode_OpGetChangedTableList, whichTN, payload, responseUnmarshaler)
	ret, err := handler(proc, "DN", "", ctl.MoCtlTNCmdSender)
	if err != nil {
		return err
	}

	cc := de.GetLatestCatalogCache()

	resp := ret.Data.([]any)[0].(*cmd_util.GetChangedTableListResp)

	if len(resp.AccIds) == 0 {
		return
	}

	getTablePair := func(item *cache.TableItem) *tablePair {
		tp := allocateTablePair()
		tp.pkSequence = item.PrimarySeqnum
		tp.acc = int64(item.AccountId)
		tp.db = int64(item.DatabaseId)
		tp.tbl = int64(item.Id)
		tp.tblName = item.Name
		tp.relKind = item.Kind
		tp.dbName = item.DatabaseName

		return tp
	}

	for i := range resp.AccIds {
		item := cc.GetTableById(uint32(resp.AccIds[i]), resp.DatabaseIds[i], resp.TableIds[i])
		if item == nil {
			continue
		}

		tp := getTablePair(item)
		*pairs = append(*pairs, tp)
	}

	for i := range req.AccIds {
		if idx := slices.Index(resp.TableIds, req.AccIds[i]); idx != -1 {
			// need calculate, already in it
			continue
		}

		// has no changes, only update TS
		item := cc.GetTableById(uint32(req.AccIds[i]), req.DatabaseIds[i], req.TableIds[i])
		if item == nil {
			continue
		}

		tp := getTablePair(item)
		tp.onlyUpdateTS = true
		*pairs = append(*pairs, tp)
	}

	*to = types.TimestampToTS(*resp.Newest)

	return
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

	txnTbl.relKind = tbl.relKind
	txnTbl.primarySeqnum = tbl.pkSequence

	if pState, err = eng.(*Engine).PushClient().toSubscribeTable(ctx, &txnTbl); err != nil {
		return nil, err
	}

	return pState, nil
}

//func getLastUpdateTS(
//	ctx context.Context,
//) (types.TS, error) {
//
//	// force update logic may distribute the continuity of the update time,
//	// so a special record needed to remember which step the normal routine arrived.
//
//	sqlRet := dynamicConfig.sqlExecFunc(
//		ctx,
//		fmt.Sprintf(lastUpdateTSSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS),
//		dynamicConfig.sqlOpts,
//	)
//
//	ret := sqlRet.(ie.InternalExecResult)
//
//	if ret.Error() != nil {
//		return types.TS{}, ret.Error()
//	}
//
//	if ret.RowCount() == 0 {
//		return types.TS{}, nil
//	}
//
//	val, err := ret.Value(ctx, 0, 0)
//	if err != nil {
//		return types.TS{}, err
//	}
//
//	if val == nil {
//		return types.TS{}, nil
//	}
//
//	tt, err := time.Parse("2006-01-02 15:04:05.000000", val.(string))
//	if err != nil {
//		return types.TS{}, err
//	}
//
//	lastUpdateTS := types.BuildTS(tt.UnixNano(), 0)
//	return lastUpdateTS, nil
//}
//
//func updateLastUpdateTS(
//	ctx context.Context,
//	snapshot types.TS,
//	service string,
//) (err error) {
//	tbl := tablePair{
//		tbl:      updateTSTableId,
//		db:       updateTSDatabaseId,
//		acc:      updateTSAccountId,
//		snapshot: snapshot,
//	}
//
//	if err = updateTableByStats(
//		ctx, service, &tbl, statsList{}); err != nil {
//
//		return err
//	}
//
//	return nil
//}

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

func updateTableStatsList(
	ctx context.Context,
	service string,
	tbl *tablePair,
	sl statsList,
) (err error) {

	//defer func() {
	//	fmt.Println("update Stats table", err, tbl.String(), sl.stats)
	//}()
	var (
		val []byte
	)

	//opts := ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()

	if val, err = json.Marshal(sl.stats); err != nil {
		return err
	}

	utcTime := tbl.snapshot.
		ToTimestamp().
		ToStdTime().
		Format("2006-01-02 15:04:05.000000")

	sql := fmt.Sprintf(insertOrUpdateStatsListSQL,
		catalog.MO_CATALOG,
		catalog.MO_TABLE_STATS,
		tbl.acc, tbl.db, tbl.tbl,
		tbl.dbName, tbl.tblName,
		string(val), utcTime, sl.took.Microseconds(),
		string(val), utcTime, sl.took.Microseconds())

	ret := dynamicConfig.sqlExecFunc(ctx, sql, dynamicConfig.sqlOpts)

	return ret.Error()
}

func updateTableOnlyTS(
	ctx context.Context,
	service string,
	tbl *tablePair,
) (err error) {

	utcTime := tbl.snapshot.
		ToTimestamp().
		ToStdTime().
		Format("2006-01-02 15:04:05.000000")

	sql := fmt.Sprintf(UpdateOnlyTSSQL,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
		utcTime,
		tbl.acc, tbl.db, tbl.tbl,
	)

	ret := dynamicConfig.sqlExecFunc(ctx, sql, dynamicConfig.sqlOpts)
	return ret.Error()
}
