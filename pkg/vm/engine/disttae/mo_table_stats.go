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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"math"
	"runtime"
	"slices"
	"sort"
	"strings"
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
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
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

// session variable: (only valid with a session)
//    // force update from last updated time
// 1. set mo_table_stats.force_update = yes
// 2. set mo_table_stats.use_old_impl = yes
//    // force update as a new table
// 3. set mo_table_stats.reset_update_time = yes
//
//
// mo ctl
// 	mo_ctl("cn", "MoTableStats", "use_old_impl:false|true");
//	mo_ctl("cn", "MoTableStats", "force_update:false|true");
//	mo_ctl("cn", "MoTableStats", "move_on:false|true");
//	mo_ctl("cn", "MoTableStats", "restore_default_setting:true|false");
//
// bootstrap config

type MoTableStatsConfig struct {
	UpdateDuration     time.Duration `toml:"update-duration"`
	ForceUpdate        bool          `toml:"force-update"`
	GetTableListLimit  int           `toml:"get-table-list-limit"`
	StatsUsingOldImpl  bool          `toml:"stats-using-old-impl"`
	DisableStatsTask   bool          `toml:"disable-stats-task"`
	CorrectionDuration time.Duration `toml:"correction-duration"`
}

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
	bulkInsertOrUpdateStatsListSQL = `
				insert into 
				    %s.%s (account_id, database_id, table_id, database_name, table_name, table_stats, update_time, takes)
					values %s 
				on duplicate key update
					table_stats = values(table_stats), update_time = values(update_time), takes = values(takes);`

	bulkInsertOrUpdateOnlyTSSQL = `
				insert into 
				    %s.%s (account_id, database_id, table_id, database_name, table_name, table_stats, update_time, takes)
					values %s 
				on duplicate key update
					update_time = values(update_time);`

	getTableStatsSQL = `
				select 
    				table_id, update_time, COALESCE(table_stats, CAST('{}' AS JSON)) from  %s.%s
              	where 
                	account_id in (%v) and 
                	database_id in (%v) and 
                  	table_id in (%v) 
			  	order by table_id asc;`

	getNextReadyListSQL = `
				select
					account_id, database_id, table_id, update_time 
				from 
				    %s.%s 
				%s 
				order by 
				    update_time asc
				limit %d;`

	getNextCheckAliveListSQL = `
				select
					%s  
				from 
				    %s.%s 
				order by 
				    update_time asc
				group by
				    %s 
				limit %d;`

	getCheckAliveSQL = `
				select 
					%s
				from 
					%s.%s
				where
				    %s in (%v);`

	getDeleteFromStatsSQL = `
				delete from 
				    %s.%s
				where 
					%s in (%v);`

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
					created_time >= '%s' and relkind != "v" 
				group by
				    account_id, reldatabase, reldatabase_id, relname, rel_id;`

	insertNewTablesSQL = `
				insert ignore into 
				    %s.%s (account_id, database_id, table_id, database_name, table_name, table_stats, update_time, takes)
					values %s;`

	getMinTSSQL = `
				select
					min(update_time) 
				from
					%s.%s;`

	getNullStatsSQL = `
				select 
					account_id, database_id, table_id 
				from 
				    %s.%s
				where 
				    table_stats = "{}"
				limit
					%d;`
)

const (
	defaultAlphaCycleDur     = time.Minute
	defaultGamaCycleDur      = time.Minute * 30
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

	dynamicCtx.once.Do(func() {

		newCtx := turn2SysCtx(ctx)

		defer func() {
			dynamicCtx.defaultConf = dynamicCtx.conf
			if err != nil {
				logutil.Error("init mo table stats config failed", zap.Error(err))
			}
		}()

		if dynamicCtx.alphaTaskPool, err = ants.NewPool(
			runtime.NumCPU(),
			ants.WithNonblocking(false)); err != nil {
			return
		}

		if dynamicCtx.betaTaskPool, err = ants.NewPool(
			runtime.NumCPU(),
			ants.WithNonblocking(false)); err != nil {
			return
		}

		dynamicCtx.conf = eng.config.statsConf

		function.MoTableRowsSizeUseOldImpl.Store(dynamicCtx.conf.DisableStatsTask)

		dynamicCtx.executorPool = sync.Pool{
			New: func() interface{} {
				return eng.config.ieFactory()
			},
		}

		dynamicCtx.tablePairPool = sync.Pool{
			New: func() interface{} {
				return &tablePair{}
			},
		}

		//dynamicCtx.db, err = sql.Open("mo_table_stats")
		dynamicCtx.sqlOpts = ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()

		if dynamicCtx.conf.GetTableListLimit <= 0 {
			dynamicCtx.conf.GetTableListLimit = defaultGetTableListLimit
		}

		if dynamicCtx.conf.UpdateDuration <= 0 {
			dynamicCtx.conf.UpdateDuration = defaultAlphaCycleDur
		}

		if dynamicCtx.conf.CorrectionDuration <= 0 {
			dynamicCtx.conf.CorrectionDuration = defaultGamaCycleDur
		}

		dynamicCtx.objIdsPool = sync.Pool{
			New: func() interface{} {
				objIds := make([]types.Objectid, 0)
				return &objIds
			},
		}

		dynamicCtx.tblQueue = make(chan *tablePair,
			min(100, dynamicCtx.conf.GetTableListLimit/5))

		// registerMoTableSizeRows
		{
			ff1 := func() func(
				context.Context,
				[]uint64, []uint64, []uint64,
				engine.Engine, bool, bool) ([]uint64, error) {
				return MTSTableSize
			}
			function.GetMoTableSizeFunc.Store(&ff1)

			ff2 := func() func(
				context.Context,
				[]uint64, []uint64, []uint64,
				engine.Engine, bool, bool) ([]uint64, error) {
				return MTSTableRows
			}
			function.GetMoTableRowsFunc.Store(&ff2)
		}

		dynamicCtx.tableStock.tbls = make([]*tablePair, 0, 1)

		// start sub tasks
		{
			go func() {
				_ = betaTask(newCtx, eng.service, eng)
			}()

			go func() {
				_ = gamaTask(newCtx, eng.service, eng)
			}()
		}

	})

	return err
}

var dynamicCtx struct {
	sync.RWMutex

	once sync.Once

	defaultConf MoTableStatsConfig
	conf        MoTableStatsConfig

	tblQueue chan *tablePair

	de         *Engine
	service    string
	objIdsPool sync.Pool

	tableStock struct {
		tbls   []*tablePair
		newest types.TS
	}

	lastCheckNewTables types.TS

	betaTaskPool  *ants.Pool
	alphaTaskPool *ants.Pool

	tablePairPool sync.Pool
	executorPool  sync.Pool

	sqlOpts ie.SessionOverrideOptions
}

////////////////// MoTableStats Interface //////////////////

func HandleMoTableStatsCtl(cmd string) string {
	cmds := strings.Split(cmd, ":")
	typ, val := cmds[0], cmds[1]

	typ = strings.TrimSpace(typ)
	val = strings.TrimSpace(val)

	if val != "false" && val != "true" {
		return "failed, cmd invalid"
	}

	switch typ {
	case "use_old_impl":
		return setUseOldImpl(val == "true")
	case "force_update":
		return setForceUpdate(val == "true")
	case "move_on":
		return setMoveOnTask(val == "true")

	case "restore_default_setting":
		return restoreDefaultSetting(val == "true")

	default:
		return "failed, cmd invalid"
	}
}

func checkMoveOnTask() bool {
	dynamicCtx.Lock()
	defer dynamicCtx.Unlock()

	disable := dynamicCtx.conf.DisableStatsTask

	if disable {
		logutil.Info("mo table stats task disabled")
	}

	return disable
}

func restoreDefaultSetting(ok bool) string {
	if !ok {
		return "noop"
	}

	dynamicCtx.Lock()
	defer dynamicCtx.Unlock()

	dynamicCtx.conf = dynamicCtx.defaultConf
	function.MoTableRowsSizeUseOldImpl.Store(dynamicCtx.conf.StatsUsingOldImpl)
	function.MoTableRowsSizeForceUpdate.Store(dynamicCtx.conf.ForceUpdate)

	return fmt.Sprintf("move_on(%v), use_old_impl(%v), force_update(%v)",
		!dynamicCtx.conf.DisableStatsTask,
		dynamicCtx.conf.StatsUsingOldImpl,
		dynamicCtx.conf.ForceUpdate)
}

func setMoveOnTask(newVal bool) string {
	dynamicCtx.Lock()
	defer dynamicCtx.Unlock()

	oldState := !dynamicCtx.conf.DisableStatsTask
	dynamicCtx.conf.DisableStatsTask = !newVal

	ret := fmt.Sprintf("move on state, %v to %v", oldState, newVal)
	logutil.Info(ret)

	return ret
}

func setUseOldImpl(newVal bool) string {
	dynamicCtx.Lock()
	defer dynamicCtx.Unlock()

	oldState := dynamicCtx.conf.StatsUsingOldImpl
	function.MoTableRowsSizeUseOldImpl.Store(newVal)
	dynamicCtx.conf.StatsUsingOldImpl = newVal

	ret := fmt.Sprintf("use old impl, %v to %v", oldState, newVal)
	logutil.Info(ret)

	return ret
}

func setForceUpdate(newVal bool) string {
	dynamicCtx.Lock()
	defer dynamicCtx.Unlock()

	oldState := dynamicCtx.conf.ForceUpdate
	function.MoTableRowsSizeForceUpdate.Store(newVal)
	dynamicCtx.conf.ForceUpdate = newVal

	ret := fmt.Sprintf("force update, %v to %v", oldState, newVal)
	logutil.Info(ret)

	return ret
}

func executeSQL(ctx context.Context, sql string, hint string) ie.InternalExecResult {
	exec := dynamicCtx.executorPool.Get()
	defer dynamicCtx.executorPool.Put(exec)

	ret := exec.(ie.InternalExecutor).Query(ctx, sql, dynamicCtx.sqlOpts)
	if ret.Error() != nil {
		logutil.Info("stats task exec sql failed",
			zap.Error(ret.Error()),
			zap.String("hint", hint),
			zap.String("sql", sql))
	}

	return ret
}

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
	resetUpdateTime bool,
	eng *Engine,
) (statsVals []any, err error) {

	if len(tbls) == 0 {
		return
	}

	var (
		to      types.TS
		val     any
		stdTime time.Time
		pairs   []*tablePair           = make([]*tablePair, 0, len(tbls))
		oldTS   []*timestamp.Timestamp = make([]*timestamp.Timestamp, len(tbls))
	)

	if !resetUpdateTime {

		sql := fmt.Sprintf(getUpdateTSSQL,
			catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
			intsJoin(accs, ","),
			intsJoin(dbs, ","),
			intsJoin(tbls, ","))

		sqlRet := executeSQL(ctx, sql, "force update query")
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
	} else {
		to = types.BuildTS(time.Now().UnixNano(), 0)
		cc := eng.GetLatestCatalogCache()

		for i := range tbls {
			tbl := allocateTablePair()
			item := cc.GetTableById(uint32(accs[i]), dbs[i], tbls[i])
			if item == nil {
				// account, db, tbl may delete already
				// the `update_time` not change anymore
				continue
			}

			if item.Kind == "v" {
				continue
			}

			tbl.acc = int64(accs[i])
			tbl.db = int64(dbs[i])
			tbl.tbl = int64(tbls[i])
			tbl.tblName = item.Name
			tbl.dbName = item.DatabaseName
			tbl.relKind = item.Kind
			tbl.pkSequence = item.PrimarySeqnum
			tbl.onlyUpdateTS = false
			tbl.snapshot = to

			pairs = append(pairs, tbl)
		}
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

	if len(tbls) == 0 {
		return
	}

	sql := fmt.Sprintf(getTableStatsSQL,
		catalog.MO_CATALOG,
		catalog.MO_TABLE_STATS,
		intsJoin(accs, ","),
		intsJoin(dbs, ","),
		intsJoin(tbls, ","))

	sqlRet := executeSQL(ctx, sql, "normal query")
	if sqlRet.Error() != nil {
		return nil, sqlRet.Error()
	}

	var (
		val any

		idxes   = make([]int, 0, sqlRet.RowCount())
		gotTIds = make([]int64, 0, sqlRet.RowCount())

		stats map[string]any

		tblIdColIdx = uint64(0)
		//updateColIdx = uint64(1)
		statsColIdx = uint64(2)

		//now     = time.Now()
		//updates []time.Time
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
			//updates = append(updates, now)
			statsVals = append(statsVals, defaultVal)
			continue
		}

		if val, err = sqlRet.Value(ctx, uint64(idxes[idx]), statsColIdx); err != nil {
			return nil, err
		}

		if err = json.Unmarshal([]byte(val.(bytejson.ByteJson).String()), &stats); err != nil {
			return
		}

		ss := stats[TableStatsName[statsIdx]]
		if ss == nil {
			ss = defaultVal
		}
		statsVals = append(statsVals, ss)

		//if val, err = sqlRet.Value(ctx, uint64(idxes[idx]), updateColIdx); err != nil {
		//	return nil, err
		//}

		//var ud time.Time
		//if ud, err = time.Parse("2006-01-02 15:04:05.000000", val.(string)); err != nil {
		//	return
		//}

		//updates = append(updates, ud)
	}

	return statsVals, nil
}

func queryStats(
	ctx context.Context,
	statsIdx int,
	defaultVal any,
	accs, dbs, tbls []uint64,
	forceUpdate bool,
	resetUpdateTime bool,
	eng engine.Engine,
) (statsVals []any, err error) {

	var now = time.Now()
	defer func() {
		if resetUpdateTime {
			v2.MoTableSizeRowsResetUpdateTimeCountingHistogram.Observe(float64(len(tbls)))
			v2.MoTableSizeRowsResetUpdateTimeDurationHistogram.Observe(time.Since(now).Seconds())
		} else if forceUpdate {
			v2.MoTableSizeRowsForceUpdateCountingHistogram.Observe(float64(len(tbls)))
			v2.MoTableSizeRowsForceUpdateDurationHistogram.Observe(time.Since(now).Seconds())
		} else {
			v2.MoTableSizeRowsNormalDurationHistogram.Observe(time.Since(now).Seconds())
			v2.MoTableSizeRowsNormalCountingHistogram.Observe(float64(len(tbls)))
		}
	}()

	newCtx := turn2SysCtx(ctx)

	if forceUpdate || resetUpdateTime {
		if len(tbls) >= defaultGetTableListLimit {
			logutil.Warn("query stats with force update", zap.Int("tbl-list-length", len(tbls)))
		}
		return forceUpdateQuery(
			newCtx, statsIdx, defaultVal,
			accs, dbs, tbls,
			resetUpdateTime,
			eng.(*engine.EntireEngine).Engine.(*Engine))
	}

	return normalQuery(newCtx, statsIdx, defaultVal, accs, dbs, tbls)
}

func MTSTableSize(
	ctx context.Context,
	accs, dbs, tbls []uint64,
	eng engine.Engine,
	forceUpdate bool,
	resetUpdateTime bool,
) (sizes []uint64, err error) {

	statsVals, err := queryStats(
		ctx, TableStatsTableSize, float64(0),
		accs, dbs, tbls,
		forceUpdate, resetUpdateTime, eng)
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
	resetUpdateTime bool,
) (sizes []uint64, err error) {

	statsVals, err := queryStats(
		ctx, TableStatsTableRows, float64(0),
		accs, dbs, tbls,
		forceUpdate, resetUpdateTime, eng)
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
		errQueue chan error
	}

	relKind    string
	pkSequence int

	acc, db, tbl    int64
	dbName, tblName string

	onlyUpdateTS bool

	snapshot types.TS
	pState   *logtailreplay.PartitionState

	reuse func()
}

func allocateTablePair() *tablePair {
	pair := dynamicCtx.tablePairPool.Get().(*tablePair)
	pair.reuse = func() {
		pair.tblName = ""
		pair.dbName = ""
		pair.pState = nil
		pair.onlyUpdateTS = false
		// pair can not close this channel
		pair.waiter.errQueue = nil
		dynamicCtx.tablePairPool.Put(pair)
	}
	return pair
}

func (tp *tablePair) String() string {
	return fmt.Sprintf("%d-%s(%d)-%s(%d)",
		tp.acc, tp.dbName, tp.db, tp.tblName, tp.tbl)
}

func (tp *tablePair) Done(err error) {
	if tp.waiter.errQueue != nil {
		tp.waiter.errQueue <- err
	}

	tp.reuse()
}

type statsList struct {
	took  time.Duration
	stats map[string]any
}

type betaCycleStash struct {
	snapshot types.TS

	born time.Time

	dataObjIds *[]types.Objectid

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

func turn2SysCtx(ctx context.Context) context.Context {
	newCtx := ctx
	if val := ctx.Value(defines.TenantIDKey{}); val == nil || val.(uint32) != catalog.System_Account {
		newCtx = context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
	}

	return newCtx
}

func tableStatsExecutor(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	newCtx := turn2SysCtx(ctx)

	tickerDur := time.Minute
	executeTicker := time.NewTicker(tickerDur)

	for {
		select {
		case <-newCtx.Done():
			logutil.Info("table stats executor exit by ctx.Done", zap.Error(newCtx.Err()))
			return newCtx.Err()

		case <-executeTicker.C:
			if checkMoveOnTask() {
				continue
			}

			if err = prepare(newCtx, service, eng); err != nil {
				return err
			}

			if err = alphaTask(
				newCtx, service, eng,
				dynamicCtx.tableStock.tbls,
				dynamicCtx.tableStock.newest,
			); err != nil {
				logutil.Info("table stats alpha exit by err", zap.Error(err))
				return err
			}

			dynamicCtx.tableStock.tbls = dynamicCtx.tableStock.tbls[:0]

			dynamicCtx.Lock()
			executeTicker.Reset(dynamicCtx.conf.UpdateDuration)
			dynamicCtx.Unlock()
		}
	}
}

func insertNewTables(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	var (
		val    any
		tm     time.Time
		sql    string
		sqlRet ie.InternalExecResult

		dbName, tblName    string
		accId, dbId, tblId uint64
	)

	//if dynamicCtx.lastCheckNewTables.IsEmpty() {
	sql = fmt.Sprintf(getMinTSSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS)
	sqlRet = executeSQL(ctx, sql, "insert new table-0: get min ts")
	if err = sqlRet.Error(); err != nil {
		return err
	}

	if val, err = sqlRet.Value(ctx, 0, 0); err != nil {
		return err
	}

	if val != nil {
		if tm, err = time.Parse("2006-01-02 15:04:05.000000", val.(string)); err != nil {
			return
		}

		dynamicCtx.lastCheckNewTables = types.BuildTS(tm.UnixNano(), 0)
	}

	//}

	sql = fmt.Sprintf(getNewTablesSQL,
		catalog.MO_CATALOG, catalog.MO_TABLES,
		dynamicCtx.lastCheckNewTables.
			ToTimestamp().
			ToStdTime().
			Format("2006-01-02 15:04:05"),
	)

	sqlRet = executeSQL(ctx, sql, "insert new table-1: get new tables")
	if err = sqlRet.Error(); err != nil {
		return err
	}

	valFmt := "(%d,%d,%d,'%s','%s','{}','%s',0)"

	values := make([]string, 0, sqlRet.RowCount())
	for i := range sqlRet.RowCount() {
		if val, err = sqlRet.Value(ctx, i, 0); err != nil {
			return err
		}
		accId = uint64(val.(uint32))

		if val, err = sqlRet.Value(ctx, i, 1); err != nil {
			return err
		}
		dbName = string(val.([]uint8))

		if val, err = sqlRet.Value(ctx, i, 2); err != nil {
			return err
		}
		dbId = val.(uint64)

		if val, err = sqlRet.Value(ctx, i, 3); err != nil {
			return err
		}
		tblName = string(val.([]uint8))

		if val, err = sqlRet.Value(ctx, i, 4); err != nil {
			return err
		}
		tblId = val.(uint64)

		values = append(values, fmt.Sprintf(valFmt,
			accId, dbId, tblId, dbName, tblName,
			timestamp.Timestamp{}.ToStdTime().
				Format("2006-01-02 15:04:05.000000")))
	}

	sql = fmt.Sprintf(insertNewTablesSQL,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
		strings.Join(values, ","))

	sqlRet = executeSQL(ctx, sql, "insert new table-2: insert new tables")
	return sqlRet.Error()
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

	if err = insertNewTables(ctx, service, eng); err != nil {
		return
	}

	offsetTS := types.TS{}
	for len(dynamicCtx.tableStock.tbls) == 0 {
		accs, dbs, tbls, ts, err := getCandidates(ctx, service, eng, offsetTS)
		if err != nil {
			return err
		}

		if len(ts) == 0 {
			break
		}

		err = getChangedTableList(
			newCtx, service, eng, accs, dbs, tbls, ts,
			&dynamicCtx.tableStock.tbls,
			&dynamicCtx.tableStock.newest)

		if err != nil {
			return err
		}

		// in case of all candidates have been deleted.
		offsetTS = types.TimestampToTS(*ts[len(ts)-1])
	}

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

	now := time.Now()
	defer func() {
		if len(tbls) == 0 {
			dur := time.Since(now)
			logutil.Info("alpha processed",
				zap.Int("processed", processed), zap.Duration("takes", dur))

			v2.AlphaTaskDurationHistogram.Observe(dur.Seconds())
			v2.AlphaTaskCountingHistogram.Observe(float64(processed))
		}
	}()

	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	dynamicCtx.Lock()
	limit := dynamicCtx.conf.GetTableListLimit
	dynamicCtx.Unlock()

	batCnt := max(max(len(tbls)/5, limit/5), 200)

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
				close(errQueue)
				return err
			}

			errWaitToReceive--
			if errWaitToReceive <= 0 {
				// all processed
				close(errQueue)
				return nil
			}

		case <-ticker.C:
			if len(tbls) == 0 {
				// all submitted
				logutil.Info("alpha send all, wait err", zap.Int("err left", errWaitToReceive))
				dynamicCtx.tblQueue <- nil
				ticker.Reset(time.Second)
				continue
			}

			start := time.Now()
			submitted := 0
			for i := 0; i < batCnt && i < len(tbls); i++ {
				if tbls[i] == nil {
					panic("table pair expected not nil")
				}

				submitted++
				wg.Add(1)

				curTbl := tbls[i]
				dynamicCtx.alphaTaskPool.Submit(func() {
					defer wg.Done()

					var pState *logtailreplay.PartitionState
					if !tbls[i].onlyUpdateTS {
						if pState, err = subscribeTable(ctx, service, eng, curTbl); err != nil {
							return
						}
					}

					curTbl.pState = pState
					curTbl.snapshot = to
					curTbl.waiter.errQueue = errQueue
					dynamicCtx.tblQueue <- curTbl
				})
			}

			if submitted == 0 {
				continue
			}

			wg.Wait()

			// let beta know that a batch done
			dynamicCtx.tblQueue <- nil

			dur := time.Since(start)
			// the longer the update takes, the longer we would pause,
			// but waiting for 1s at most, 50ms at least.
			ticker.Reset(max(min(dur, time.Second), time.Millisecond*50))

			processed += submitted
			tbls = tbls[submitted:]
		}
	}
}

func betaTask(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	defer func() {
		logutil.Info("beta task exist", zap.Error(err))
	}()

	var (
		sl        statsList
		de        = eng.(*Engine)
		slBat     sync.Map
		onlyTSBat []*tablePair

		bulkWait sync.WaitGroup
	)

	for {
		select {
		case <-ctx.Done():
			return

		case tbl := <-dynamicCtx.tblQueue:
			if tbl == nil {
				// an alpha batch transmit done
				bulkUpdateTableOnlyTS(ctx, service, onlyTSBat)

				bulkWait.Wait()
				_ = bulkUpdateTableStatsList(ctx, service, &slBat)

				slBat.Clear()
				onlyTSBat = onlyTSBat[:0]

				continue
			}

			if tbl.pState == nil {
				// 1. view
				// 2. no update
				//err = updateTableOnlyTS(ctx, service, tbl)
				onlyTSBat = append(onlyTSBat, tbl)
				continue
			}

			bulkWait.Add(1)
			if err = dynamicCtx.betaTaskPool.Submit(func() {
				defer bulkWait.Done()

				sl, err = statsCalculateOp(ctx, service, de.fs, tbl.snapshot, tbl.pState)
				slBat.Store(tbl, sl)

			}); err != nil {
				tbl.Done(err)
				logutil.Info("stats beta task exist", zap.Error(err))
				return
			}
		}
	}
}

func gamaTask(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	var (
		val  any
		de   = eng.(*Engine)
		cc   *cache.CatalogCache
		tbls []*tablePair
		now  time.Time

		accIds, dbIds, tblIds []uint64
	)

	dynamicCtx.Lock()
	gamaDur := dynamicCtx.conf.CorrectionDuration
	gamaLimit := max(dynamicCtx.conf.GetTableListLimit/100, 100)
	dynamicCtx.Unlock()

	decodeIdsFromSqlRet := func(sqlRet ie.InternalExecResult) {
		accIds = accIds[:0]
		dbIds = dbIds[:0]
		tblIds = tblIds[:0]

		for i := range sqlRet.RowCount() {
			if val, err = sqlRet.Value(ctx, i, 0); err != nil {
				logutil.Info("correction task failed", zap.Error(err))
				continue
			}
			accIds = append(accIds, uint64(val.(int64)))

			if sqlRet.ColumnCount() == 1 {
				continue
			}

			if val, err = sqlRet.Value(ctx, i, 1); err != nil {
				logutil.Info("correction task failed", zap.Error(err))
				continue
			}
			dbIds = append(dbIds, uint64(val.(int64)))

			if sqlRet.ColumnCount() == 2 {
				continue
			}

			if val, err = sqlRet.Value(ctx, i, 2); err != nil {
				logutil.Info("correction task failed", zap.Error(err))
				continue
			}

			tblIds = append(tblIds, uint64(val.(int64)))
		}
	}

	// incremental update tables with heartbeat update_time
	// may leave some tables never been updated.
	// this opA does such correction.
	opA := func() {
		sql := fmt.Sprintf(getNullStatsSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, gamaLimit)
		sqlRet := executeSQL(ctx, sql, "gama task: get null stats list")
		if sqlRet.Error() != nil {
			logutil.Info("correction task get null stats failed",
				zap.String("sql", sql),
				zap.Error(sqlRet.Error()))
			return
		}

		now = time.Now()
		tbls = tbls[:0]

		decodeIdsFromSqlRet(sqlRet)

		cc = de.GetLatestCatalogCache()
		for i := range tblIds {
			item := cc.GetTableById(uint32(accIds[i]), uint64(dbIds[i]), uint64(tblIds[i]))
			if item == nil {
				// account, db, tbl may delete already
				// the `update_time` not change anymore
				continue
			}

			tbl := allocateTablePair()

			tbl.tblName = item.Name
			tbl.dbName = item.DatabaseName
			tbl.relKind = item.Kind
			tbl.pkSequence = item.PrimarySeqnum
			tbl.onlyUpdateTS = false

			tbls = append(tbls, tbl)
		}

		if err = alphaTask(
			ctx, service, eng, tbls, types.BuildTS(now.UnixNano(), 0)); err != nil {
			logutil.Info("correction task failed", zap.Error(err))
		}

		logutil.Info("correction task force update",
			zap.Int("tbl cnt", len(tbls)), zap.Duration("takes", time.Since(now)))

		v2.GamaTaskCountingHistogram.Observe(float64(len(tbls)))
		v2.GamaTaskDurationHistogram.Observe(time.Since(now).Seconds())
	}

	colName1 := []string{
		"account_id", "dat_id", "rel_id",
	}
	colName2 := []string{
		"account_id", "database_id", "table_id",
	}

	tblName := []string{
		catalog.MOAccountTable, catalog.MO_DATABASE, catalog.MO_TABLES,
	}

	deleteByStep := func(step int) {
		sql := fmt.Sprintf(getNextCheckAliveListSQL,
			colName2[step], catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
			colName2[step], options.DefaultBlockMaxRows)

		sqlRet := executeSQL(ctx, sql, fmt.Sprintf("gama task-%d-0", step))
		if sqlRet.Error() != nil {
			return
		}

		decodeIdsFromSqlRet(sqlRet)
		if len(accIds) == 0 {
			return
		}

		sql = fmt.Sprintf(getCheckAliveSQL,
			colName1[step], catalog.MO_CATALOG, tblName[step],
			colName1[step], intsJoin(accIds, ","))
		sqlRet = executeSQL(ctx, sql, fmt.Sprintf("gama task-%d-1", step))
		if sqlRet.Error() != nil {
			return
		}

		for i := range sqlRet.RowCount() {
			if val, err = sqlRet.Value(ctx, i, 0); err != nil {
				continue
			}

			// remove alive acc from slice
			idx := slices.Index(accIds, uint64(val.(int32)))
			accIds = append(accIds[:idx], accIds[idx+1:]...)
		}

		if len(accIds) != 0 {
			sql = fmt.Sprintf(getDeleteFromStatsSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
				colName2[step], intsJoin(accIds, ","))
			executeSQL(ctx, sql, fmt.Sprintf("gama task-%d-2", step))
		}
	}

	// clear deleted tbl, db, account
	opB := func() {
		// the stats belong to any deleted accounts/databases/tables have unchanged update time
		// since they have been deleted.
		// so opB collects tables ascending their update time and then check if they deleted.

		deleteByStep(0) // clean account
		deleteByStep(1) // clean database
		deleteByStep(2) // clean tables
	}

	tickerA := time.NewTicker(gamaDur)
	tickerB := time.NewTicker(gamaDur + time.Minute)

	for {
		select {
		case <-ctx.Done():
			return

		case <-tickerA.C:
			opA()
			tickerA.Reset(gamaDur)

		case <-tickerB.C:
			opB()
			tickerB.Reset(gamaDur)
		}
	}
}

func statsCalculateOp(
	ctx context.Context,
	service string,
	fs fileservice.FileService,
	snapshot types.TS,
	pState *logtailreplay.PartitionState,
) (sl statsList, err error) {

	bcs := betaCycleStash{
		born:       time.Now(),
		snapshot:   snapshot,
		dataObjIds: dynamicCtx.objIdsPool.Get().(*[]types.Objectid),
	}

	defer func() {
		*bcs.dataObjIds = (*bcs.dataObjIds)[:0]
		dynamicCtx.objIdsPool.Put(bcs.dataObjIds)
	}()

	if err = collectVisibleData(&bcs, pState); err != nil {
		return
	}

	if err = applyTombstones(ctx, &bcs, fs, pState); err != nil {
		return
	}

	sl = stashToStats(bcs)

	v2.CalculateStatsDurationHistogram.Observe(time.Since(bcs.born).Seconds())

	return sl, nil
}

func getCandidates(
	ctx context.Context,
	service string,
	eng engine.Engine,
	offsetTS types.TS,
) (accs, dbs, tbls []uint64, ts []*timestamp.Timestamp, err error) {

	dynamicCtx.Lock()
	limit := dynamicCtx.conf.GetTableListLimit
	dynamicCtx.Unlock()

	var (
		sql    string
		where  = ""
		sqlRet ie.InternalExecResult
	)

	if !offsetTS.IsEmpty() {
		where = fmt.Sprintf("where update_time >= '%s'",
			offsetTS.ToTimestamp().
				ToStdTime().
				Format("2006-01-02 15:04:05.000000"))
	}

	sql = fmt.Sprintf(getNextReadyListSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, where, limit)
	sqlRet = executeSQL(ctx, sql, "get next ready to update list")
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

	defer func() {
		err = txnOperator.Commit(ctx)
	}()

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
			// account, db, tbl may delete already
			// the `update_time` not change anymore
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
			// account, db, tbl may delete already
			// the `update_time` not change anymore
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
		*bcs.dataObjIds = append(
			*bcs.dataObjIds, *obj.ObjectStats.ObjectName().ObjectId())
	}

	if bcs.totalRows != 0 {
		estimatedOneRowSize = bcs.totalSize / bcs.totalRows
	}

	// 1. inserts on appendable object
	for dRowIter.Next() {
		entry := dRowIter.Entry()

		idx := slices.IndexFunc(*bcs.dataObjIds, func(objId types.Objectid) bool {
			return objId.EQ(entry.BlockID.Object())
		})

		if idx == -1 {
			*bcs.dataObjIds = append(*bcs.dataObjIds, *entry.BlockID.Object())
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
				cnt := getDeletedRows(*bcs.dataObjIds, rowIds)

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

		bcs.deletedRows += float64(getDeletedRows(*bcs.dataObjIds, []types.Rowid{entry.RowID}))
		//deletedRows += float64(getDeletedRows(moTableStats.bcs.dataObjIds, []types.Rowid{entry.RowID}))

		return true, nil
	})

	//return deletedRows, loaded, err
	return nil
}

func bulkUpdateTableStatsList(
	ctx context.Context,
	service string,
	bat *sync.Map,
) (err error) {
	var (
		val  []byte
		vals []string
		tbls []*tablePair

		now = time.Now()
	)

	defer func() {
		for i := range tbls {
			tbls[i].Done(err)
		}

		if len(tbls) > 0 {
			v2.BulkUpdateStatsCountingHistogram.Observe(float64(len(tbls)))
			v2.BulkUpdateStatsDurationHistogram.Observe(time.Since(now).Seconds())
		}
	}()

	bat.Range(func(key, value any) bool {
		tbl := key.(*tablePair)
		sl := value.(statsList)

		tbls = append(tbls, tbl)

		if val, err = json.Marshal(sl.stats); err != nil {
			return false
		}

		vals = append(vals, fmt.Sprintf("(%d,%d,%d,'%s','%s','%s','%s',%d)",
			tbl.acc, tbl.db, tbl.tbl,
			tbl.dbName, tbl.tblName,
			string(val),
			tbl.snapshot.ToTimestamp().
				ToStdTime().
				Format("2006-01-02 15:04:05.000000"),
			sl.took))

		return true
	})

	if len(vals) == 0 {
		return
	}

	sql := fmt.Sprintf(bulkInsertOrUpdateStatsListSQL,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
		strings.Join(vals, ","))

	ret := executeSQL(ctx, sql, "bulk update table stats")
	return ret.Error()
}

func bulkUpdateTableOnlyTS(
	ctx context.Context,
	service string,
	tbls []*tablePair,
) {

	if len(tbls) == 0 {
		return
	}

	var (
		vals = make([]string, 0, len(tbls))
		now  = time.Now()
	)

	defer func() {
		v2.BulkUpdateOnlyTSCountingHistogram.Observe(float64(len(tbls)))
		v2.BulkUpdateOnlyTSDurationHistogram.Observe(time.Since(now).Seconds())
	}()

	for i := range tbls {
		vals = append(vals, fmt.Sprintf("(%d,%d,%d,'%s','%s','{}','%s',0)",
			tbls[i].acc, tbls[i].db, tbls[i].tbl,
			tbls[i].dbName, tbls[i].tblName,
			tbls[i].snapshot.ToTimestamp().
				ToStdTime().
				Format("2006-01-02 15:04:05.000000")))
	}

	sql := fmt.Sprintf(bulkInsertOrUpdateOnlyTSSQL,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
		strings.Join(vals, ","))

	ret := executeSQL(ctx, sql, "bulk update only ts")

	for i := range tbls {
		tbls[i].Done(ret.Error())
	}
}