// Copyright 2024 Matrix Origin
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
	"github.com/matrixorigin/matrixone/pkg/predefine"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
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
//  mo_ctl("cn", "MoTableStats", "echo_current_setting:true|false");
//	mo_ctl("cn", "MoTableStats", "recomputing:account_id, account_id...")
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
				from (
					select
						%s, min(update_time) as min_update
					from
						%s.%s
					group by
						%s
				) sub
				order by
				    min_update asc
				limit %d;`

	getCheckAliveSQL = `
				select 
					distinct(%s)
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

	insertNewTablesSQL = `
				insert ignore into 
				    %s.%s (account_id, database_id, table_id, database_name, table_name, table_stats, update_time, takes)
					values %s;`

	findNewTableSQL = `
				select 
					A.account_id, A.reldatabase, A.reldatabase_id, A.relname, A.rel_id, A.relkind
				from 
				    %s.%s as A
				left join 
					%s.%s as B
				on
					A.account_id = B.account_id and A.reldatabase_id = B.database_id and A.rel_id = B.table_id
				where
				    B.table_id is NULL;`

	getNullStatsSQL = `
				select 
					account_id, database_id, table_id 
				from 
				    %s.%s
				where 
				    table_stats = "{}" 
				or
				    update_time < '%s' 
				limit
					%d;`

	accumulateIdsByAccSQL = `
 				select 
					account_id, reldatabase_id, rel_id
				from 
				    %s.%s
				where
				    account_id in (%s);`
)

const (
	defaultAlphaCycleDur     = time.Minute
	defaultGamaCycleDur      = time.Minute
	defaultGetTableListLimit = options.DefaultBlockMaxRows

	logHeader = "MO-TABLE-STATS-TASK"
)

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
	betaTaskName = "beta"
	gamaTaskName = "gama"
)

var TableStatsName = [TableStatsCnt]string{
	"table_size",
	"table_rows",
	"tobject_cnt",
	"dobject_cnt",
	"tblock_cnt",
	"dblock_cnt",
}

var defaultStatsVals = []any{
	// table size
	float64(0),
	// table rows
	float64(0),
	// tombstone object cnt
	int(0),
	// data object cnt
	int(0),
	// tombstone blk cnt
	int(0),
	// data blk cnt
	int(0),
}

func initMoTableStatsConfig(
	ctx context.Context,
	eng *Engine,
) (err error) {

	eng.dynamicCtx.once.Do(func() {

		defer func() {
			eng.dynamicCtx.defaultConf = eng.dynamicCtx.conf
			if err != nil {
				logutil.Error(logHeader,
					zap.String("source", "init mo table stats config"),
					zap.Error(err))
			}
		}()

		eng.dynamicCtx.de = eng

		if eng.dynamicCtx.alphaTaskPool, err = ants.NewPool(
			runtime.NumCPU(),
			ants.WithNonblocking(false)); err != nil {
			return
		}

		eng.dynamicCtx.conf = eng.config.statsConf

		function.MoTableRowsSizeUseOldImpl.Store(eng.dynamicCtx.conf.DisableStatsTask)

		eng.dynamicCtx.executorPool = sync.Pool{
			New: func() interface{} {
				return eng.config.ieFactory()
			},
		}

		eng.dynamicCtx.sqlOpts = ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()

		if eng.dynamicCtx.conf.GetTableListLimit <= 0 {
			eng.dynamicCtx.conf.GetTableListLimit = defaultGetTableListLimit
		}

		if eng.dynamicCtx.conf.UpdateDuration <= 0 {
			eng.dynamicCtx.conf.UpdateDuration = defaultAlphaCycleDur
		}

		if eng.dynamicCtx.conf.CorrectionDuration <= 0 {
			eng.dynamicCtx.conf.CorrectionDuration = defaultGamaCycleDur
		}

		eng.dynamicCtx.objIdsPool = sync.Pool{
			New: func() interface{} {
				objIds := make([]types.Objectid, 0)
				return &objIds
			},
		}

		eng.dynamicCtx.tblQueue = make(chan tablePair, options.DefaultBlockMaxRows*2)

		eng.dynamicCtx.cleanDeletesQueue = make(chan struct{})
		eng.dynamicCtx.updateForgottenQueue = make(chan struct{})

		// registerMoTableSizeRows
		{
			ff1 := func() func(
				context.Context,
				[]uint64, []uint64, []uint64,
				engine.Engine, bool, bool) ([]uint64, error) {
				return eng.dynamicCtx.MTSTableSize
			}
			function.GetMoTableSizeFunc.Store(&ff1)

			ff2 := func() func(
				context.Context,
				[]uint64, []uint64, []uint64,
				engine.Engine, bool, bool) ([]uint64, error) {
				return eng.dynamicCtx.MTSTableRows
			}
			function.GetMoTableRowsFunc.Store(&ff2)
		}

		eng.dynamicCtx.tableStock.tbls = make([]tablePair, 0, 1)

		{
			eng.dynamicCtx.beta.executor = eng.dynamicCtx.betaTask
			eng.dynamicCtx.gama.executor = eng.dynamicCtx.gamaTask

			if eng.dynamicCtx.beta.taskPool, err = ants.NewPool(
				runtime.NumCPU(),
				ants.WithNonblocking(false),
				ants.WithPanicHandler(func(e interface{}) {
					logutil.Error(logHeader,
						zap.String("source", "beta task panic"),
						zap.Any("error", e))
				})); err != nil {
				return
			}

			if eng.dynamicCtx.gama.taskPool, err = ants.NewPool(
				runtime.NumCPU(),
				ants.WithNonblocking(false),
				ants.WithPanicHandler(func(e interface{}) {
					logutil.Error(logHeader,
						zap.String("source", "gama task panic"),
						zap.Any("error", e))
				})); err != nil {
				return
			}
		}

		launch := func(hint string, task *taskState) {
			if task.running {
				return
			}

			task.launchTimes++
			task.running = true

			logutil.Info(logHeader,
				zap.String("source", fmt.Sprintf("launch %s", hint)),
				zap.Int("times", task.launchTimes))

			go func() {
				defer func() {
					eng.dynamicCtx.Lock()
					task.running = false
					eng.dynamicCtx.Unlock()
				}()

				// there should not have a deadline
				taskCtx := turn2SysCtx(ctx)
				task.executor(taskCtx, eng.service, eng)
			}()
		}

		// beta task expect to be running on every cn.
		// gama task expect to be running only on one cn.
		eng.dynamicCtx.launchTask = func(name string) {
			eng.dynamicCtx.Lock()
			defer eng.dynamicCtx.Unlock()

			switch name {
			case gamaTaskName:
				eng.dynamicCtx.isMainRunner = true
				launch("gama task", &eng.dynamicCtx.gama)
			case betaTaskName:
				launch("beta task", &eng.dynamicCtx.beta)
			}
		}

		go func() {
			newCtx := turn2SysCtx(ctx)
			ticker := time.NewTicker(time.Second)

			for {
				select {
				case <-newCtx.Done():
					return
				case <-ticker.C:
					if eng.config.moServerStateChecker == nil || !eng.config.moServerStateChecker() {
						continue
					}

					if eng.dynamicCtx.initCronTask(newCtx) {
						return
					}

					ticker.Reset(time.Second)
				}
			}
		}()
	})

	return err
}

func (d *dynamicCtx) initCronTask(
	ctx context.Context,
) bool {
	// insert mo table stats task meta into sys_cron_task table.
	var (
		err error
		val any
		sql string
	)

	insertTask := func() {
		sql, err = predefine.GenInitCronTaskSQL(int32(task.TaskCode_MOTableStats))
		if err != nil {
			logutil.Error(logHeader,
				zap.String("source", "init cron task"),
				zap.Error(err))
		}

		d.executeSQL(ctx, sql, "init cron task")
	}

	checkTask := func() bool {
		sqlRet := d.executeSQL(ctx,
			fmt.Sprintf(`select count(*) from %s.%s where task_metadata_id = '%s';`,
				catalog.MOTaskDB, "sys_cron_task", "mo_table_stats"), "check cron task")

		if sqlRet.Error() != nil || sqlRet.RowCount() != 1 {
			return false
		}

		val, err = sqlRet.Value(ctx, 0, 0)
		if err != nil {
			return false
		}

		return val.(int64) == 1
	}

	if checkTask() {
		logutil.Info(logHeader, zap.String("source", "init cron task succeed"))
		return true
	}

	insertTask()

	return false
}

type taskState struct {
	taskPool    *ants.Pool
	running     bool
	executor    func(context.Context, string, engine.Engine)
	launchTimes int
}

type dynamicCtx struct {
	sync.RWMutex

	once sync.Once

	defaultConf MoTableStatsConfig
	conf        MoTableStatsConfig

	tblQueue             chan tablePair
	cleanDeletesQueue    chan struct{}
	updateForgottenQueue chan struct{}

	de         *Engine
	objIdsPool sync.Pool

	tableStock struct {
		tbls   []tablePair
		newest types.TS
	}

	isMainRunner bool

	beta, gama taskState
	launchTask func(name string)

	alphaTaskPool *ants.Pool

	executorPool sync.Pool

	sqlOpts ie.SessionOverrideOptions
}

func (d *dynamicCtx) LogDynamicCtx() string {
	d.Lock()
	defer d.Unlock()

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("cur-conf:[alpha-dur: %v; gama-dur: %v; limit: %v; force-update: %v; use-old-impl: %v; disable-task: %v]\n",
		d.conf.UpdateDuration,
		d.conf.CorrectionDuration,
		d.conf.GetTableListLimit,
		d.conf.ForceUpdate,
		d.conf.StatsUsingOldImpl,
		d.conf.DisableStatsTask))

	buf.WriteString(fmt.Sprintf("default-conf:[alpha-dur: %v; gama-dur: %v; limit: %v; force-update: %v; use-old-impl: %v; disable-task: %v]\n",
		d.defaultConf.UpdateDuration,
		d.defaultConf.CorrectionDuration,
		d.defaultConf.GetTableListLimit,
		d.defaultConf.ForceUpdate,
		d.defaultConf.StatsUsingOldImpl,
		d.defaultConf.DisableStatsTask))

	buf.WriteString(fmt.Sprintf("beta: [running: %v; launched-time: %v]\n",
		d.beta.running,
		d.beta.launchTimes))

	buf.WriteString(fmt.Sprintf("gama: [running: %v; launched-time: %v]\n",
		d.gama.running,
		d.gama.launchTimes))

	return buf.String()
}

func (d *dynamicCtx) Close() {
	if d.alphaTaskPool != nil {
		_ = d.alphaTaskPool.ReleaseTimeout(time.Second * 3)
	}
	if d.beta.taskPool != nil {
		_ = d.beta.taskPool.ReleaseTimeout(time.Second * 3)
	}
	if d.gama.taskPool != nil {
		_ = d.gama.taskPool.ReleaseTimeout(time.Second * 3)
	}
}

////////////////// MoTableStats Interface //////////////////

func (d *dynamicCtx) HandleMoTableStatsCtl(cmd string) string {
	cmds := strings.Split(cmd, ":")

	if len(cmds) != 2 {
		return "invalid command"
	}

	typ, val := cmds[0], cmds[1]

	typ = strings.TrimSpace(typ)
	val = strings.TrimSpace(val)

	if typ != "recomputing" {
		if val != "false" && val != "true" {
			return "failed, cmd invalid"
		}
	}

	switch typ {
	case "use_old_impl":
		return d.setUseOldImpl(val == "true")

	case "force_update":
		return d.setForceUpdate(val == "true")

	case "move_on":
		return d.setMoveOnTask(val == "true")

	case "restore_default_setting":
		return d.restoreDefaultSetting(val == "true")

	case "echo_current_setting":
		return d.echoCurrentSetting(val == "true")

	case "recomputing":
		return d.recomputing(val)

	default:
		return "failed, cmd invalid"
	}
}

func (d *dynamicCtx) recomputing(para string) string {
	{
		d.Lock()
		if !d.isMainRunner {
			d.Unlock()
			return "not main runner"
		}
		d.Unlock()
	}

	var (
		err    error
		ok     bool
		id     uint64
		buf    bytes.Buffer
		retAcc []uint64
	)

	ids := strings.Split(para, ",")

	accIds := make([]uint64, 0, len(ids))

	for i := range ids {
		id, err = strconv.ParseUint(ids[i], 10, 64)
		if err == nil {
			accIds = append(accIds, id)
		}
	}

	_, retAcc, err, ok = d.QueryTableStatsByAccounts(
		context.Background(), nil, accIds, false, true)

	if ok {
		uniqueAcc := make(map[uint64]struct{})
		for i := range retAcc {
			uniqueAcc[retAcc[i]] = struct{}{}
		}

		for k := range uniqueAcc {
			buf.WriteString(fmt.Sprintf("%d ", k))
		}

		buf.WriteString("succeed")
	} else {
		buf.WriteString(fmt.Sprintf("failed, err: %v", err))
	}

	return buf.String()
}

func (d *dynamicCtx) checkMoveOnTask() bool {
	d.Lock()
	defer d.Unlock()

	disable := d.conf.DisableStatsTask

	logutil.Info(logHeader,
		zap.String("source", "check move on"),
		zap.Bool("disable", disable))

	return disable
}

func (d *dynamicCtx) echoCurrentSetting(ok bool) string {
	if !ok {
		return "noop"
	}

	d.Lock()
	defer d.Unlock()

	return fmt.Sprintf("move_on(%v), use_old_impl(%v), force_update(%v)",
		!d.conf.DisableStatsTask,
		d.conf.StatsUsingOldImpl,
		d.conf.ForceUpdate)
}

func (d *dynamicCtx) restoreDefaultSetting(ok bool) string {
	if !ok {
		return "noop"
	}

	d.Lock()
	defer d.Unlock()

	d.conf = d.defaultConf
	function.MoTableRowsSizeUseOldImpl.Store(d.conf.StatsUsingOldImpl)
	function.MoTableRowsSizeForceUpdate.Store(d.conf.ForceUpdate)

	return fmt.Sprintf("move_on(%v), use_old_impl(%v), force_update(%v)",
		!d.conf.DisableStatsTask,
		d.conf.StatsUsingOldImpl,
		d.conf.ForceUpdate)
}

func (d *dynamicCtx) setMoveOnTask(newVal bool) string {
	d.Lock()
	defer d.Unlock()

	oldState := !d.conf.DisableStatsTask
	d.conf.DisableStatsTask = !newVal

	ret := fmt.Sprintf("move on: %v to %v", oldState, newVal)
	logutil.Info(logHeader,
		zap.String("source", "set move on"),
		zap.String("state", ret))

	return ret
}

func (d *dynamicCtx) setUseOldImpl(newVal bool) string {
	d.Lock()
	defer d.Unlock()

	oldState := d.conf.StatsUsingOldImpl
	function.MoTableRowsSizeUseOldImpl.Store(newVal)
	d.conf.StatsUsingOldImpl = newVal

	ret := fmt.Sprintf("use old impl: %v to %v", oldState, newVal)
	logutil.Info(logHeader,
		zap.String("source", "set use old impl"),
		zap.String("state", ret))

	return ret
}

func (d *dynamicCtx) setForceUpdate(newVal bool) string {
	d.Lock()
	defer d.Unlock()

	oldState := d.conf.ForceUpdate
	function.MoTableRowsSizeForceUpdate.Store(newVal)
	d.conf.ForceUpdate = newVal

	ret := fmt.Sprintf("force update: %v to %v", oldState, newVal)
	logutil.Info(logHeader,
		zap.String("source", "set force update"),
		zap.String("state", ret))

	return ret
}

func (d *dynamicCtx) executeSQL(ctx context.Context, sql string, hint string) ie.InternalExecResult {
	exec := d.executorPool.Get()
	defer d.executorPool.Put(exec)

	var (
		newCtx = ctx
		cancel context.CancelFunc
	)

	if _, exist := ctx.Deadline(); !exist {
		newCtx, cancel = context.WithTimeout(ctx, time.Minute*2)
		defer cancel()
	}

	ret := exec.(ie.InternalExecutor).Query(newCtx, sql, d.sqlOpts)
	if ret.Error() != nil {
		logutil.Info(logHeader,
			zap.String("source", hint),
			zap.Error(ret.Error()),
			zap.String("sql", sql))
	}

	return ret
}

func intsJoin(items []uint64, delimiter string) string {
	var builder strings.Builder
	builder.Grow(mpool.MB)
	for i, item := range items {
		builder.WriteString(strconv.FormatUint(item, 10))
		if i < len(items)-1 {
			builder.WriteString(delimiter)
		}
	}
	return builder.String()
}

func (d *dynamicCtx) forceUpdateQuery(
	ctx context.Context,
	wantedStatsIdxes []int,
	accs, dbs, tbls []uint64,
	resetUpdateTime bool,
	eng *Engine,
) (statsVals [][]any, err error) {

	if len(tbls) == 0 {
		return
	}

	var (
		to      types.TS
		val     any
		stdTime time.Time
		pairs   = make([]tablePair, 0, len(tbls))
		oldTS   = make([]*timestamp.Timestamp, len(tbls))
	)

	if !resetUpdateTime {

		sql := fmt.Sprintf(getUpdateTSSQL,
			catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
			intsJoin(accs, ","),
			intsJoin(dbs, ","),
			intsJoin(tbls, ","))

		sqlRet := d.executeSQL(ctx, sql, "force update query")
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

		var notExist = make([]uint64, 0, 1)
		for i := range oldTS {
			if oldTS[i] == nil {
				oldTS[i] = &timestamp.Timestamp{}
				notExist = append(notExist, tbls[i])
			}
		}

		if err = getChangedTableList(
			ctx, eng.service, eng, accs, dbs, tbls, oldTS, &pairs, &to); err != nil {
			return
		}

		// if a table not exist in mo table stats table, need update stats.
		if len(notExist) != 0 {
			for i := range pairs {
				idx := slices.Index(notExist, uint64(pairs[i].tbl))
				if idx != -1 {
					pairs[i].onlyUpdateTS = false
				}
			}
		}

	} else {
		to = types.BuildTS(time.Now().UnixNano(), 0)

		for i := range tbls {
			tbl, ok := buildTablePairFromCache(eng, accs[i], dbs[i], tbls[i], to, false)
			if !ok {
				continue
			}

			pairs = append(pairs, tbl)
		}
	}

	if err = d.alphaTask(ctx, eng.service, eng, pairs,
		fmt.Sprintf("forceUpdateQuery(reset_update=%v)", resetUpdateTime)); err != nil {
		return nil, err
	}

	if statsVals, err = d.normalQuery(
		ctx, wantedStatsIdxes,
		accs, dbs, tbls); err != nil {
		return nil, err
	}

	return
}

func (d *dynamicCtx) normalQuery(
	ctx context.Context,
	wantedStatsIdxes []int,
	accs, dbs, tbls []uint64,
) (statsVals [][]any, err error) {

	if len(tbls) == 0 {
		return
	}

	sql := fmt.Sprintf(getTableStatsSQL,
		catalog.MO_CATALOG,
		catalog.MO_TABLE_STATS,
		intsJoin(accs, ","),
		intsJoin(dbs, ","),
		intsJoin(tbls, ","))

	sqlRet := d.executeSQL(ctx, sql, "normal query")
	if sqlRet.Error() != nil {
		return nil, sqlRet.Error()
	}

	var (
		val any

		idxes   = make([]int, 0, sqlRet.RowCount())
		gotTIds = make([]int64, 0, sqlRet.RowCount())

		tblIdColIdx = uint64(0)
		statsColIdx = uint64(2)
	)

	statsVals = make([][]any, len(wantedStatsIdxes))

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
			for k := range wantedStatsIdxes {
				statsVals[k] = append(statsVals[k], defaultStatsVals[wantedStatsIdxes[k]])
			}
			continue
		}

		if val, err = sqlRet.Value(ctx, uint64(idxes[idx]), statsColIdx); err != nil {
			return nil, err
		}

		var stats map[string]any
		if err = json.Unmarshal([]byte(val.(bytejson.ByteJson).String()), &stats); err != nil {
			return
		}

		for k := range wantedStatsIdxes {
			ss := stats[TableStatsName[wantedStatsIdxes[k]]]
			if ss == nil {
				ss = defaultStatsVals[wantedStatsIdxes[k]]
			}
			statsVals[k] = append(statsVals[k], ss)
		}
	}

	return statsVals, nil
}

func (d *dynamicCtx) QueryTableStatsByAccounts(
	ctx context.Context,
	wantedStatsIdxes []int,
	accs []uint64,
	forceUpdate bool,
	resetUpdateTime bool,
) (statsVals [][]any, retAcc []uint64, err error, ok bool) {

	if len(accs) == 0 {
		return
	}

	now := time.Now()

	newCtx := turn2SysCtx(ctx)

	sql := fmt.Sprintf(accumulateIdsByAccSQL,
		catalog.MO_CATALOG, catalog.MO_TABLES, intsJoin(accs, ","))

	sqlRet := d.executeSQL(newCtx, sql, "query table stats by accounts")
	if err = sqlRet.Error(); err != nil {
		return
	}

	var (
		val any

		accs2 = make([]uint64, 0, sqlRet.RowCount())
		dbs   = make([]uint64, 0, sqlRet.RowCount())
		tbls  = make([]uint64, 0, sqlRet.RowCount())
	)

	for i := range sqlRet.RowCount() {
		if val, err = sqlRet.Value(newCtx, i, 0); err != nil {
			return
		}
		accs2 = append(accs2, uint64(val.(uint32)))

		if val, err = sqlRet.Value(newCtx, i, 1); err != nil {
			return
		}
		dbs = append(dbs, val.(uint64))

		if val, err = sqlRet.Value(newCtx, i, 2); err != nil {
			return
		}
		tbls = append(tbls, val.(uint64))
	}

	statsVals, err, ok = d.QueryTableStats(
		newCtx, wantedStatsIdxes, accs2, dbs, tbls, forceUpdate, resetUpdateTime, nil)

	logutil.Info(logHeader,
		zap.String("source", "QueryTableStatsByAccounts"),
		zap.Int("acc cnt", len(accs)),
		zap.Int("tbl cnt", len(tbls)),
		zap.Bool("forceUpdate", forceUpdate),
		zap.Bool("resetUpdateTime", resetUpdateTime),
		zap.Duration("takes", time.Since(now)),
		zap.Bool("ok", ok),
		zap.Error(err))

	return statsVals, accs2, err, ok
}

func (d *dynamicCtx) QueryTableStats(
	ctx context.Context,
	wantedStatsIdxes []int,
	accs, dbs, tbls []uint64,
	forceUpdate bool,
	resetUpdateTime bool,
	eng engine.Engine,
) (statsVals [][]any, err error, ok bool) {

	d.Lock()
	useOld := d.conf.StatsUsingOldImpl
	d.Unlock()
	if useOld {
		return
	}

	if eng == nil {
		d.Lock()
		eng = d.de
		d.Unlock()
	}

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
			logutil.Warn(logHeader,
				zap.String("source", "query with force update"),
				zap.Int("tbl-list-length", len(tbls)))
		}

		var de *Engine
		if val, ok := eng.(*engine.EntireEngine); ok {
			de = val.Engine.(*Engine)
		} else {
			de = eng.(*Engine)
		}

		statsVals, err = d.forceUpdateQuery(
			newCtx, wantedStatsIdxes,
			accs, dbs, tbls,
			resetUpdateTime,
			de)
		return statsVals, err, true
	}

	statsVals, err = d.normalQuery(newCtx, wantedStatsIdxes, accs, dbs, tbls)

	return statsVals, err, true
}

func (d *dynamicCtx) MTSTableSize(
	ctx context.Context,
	accs, dbs, tbls []uint64,
	eng engine.Engine,
	forceUpdate bool,
	resetUpdateTime bool,
) (sizes []uint64, err error) {

	statsVals, err, _ := d.QueryTableStats(
		ctx, []int{TableStatsTableSize},
		accs, dbs, tbls,
		forceUpdate, resetUpdateTime, eng)
	if err != nil {
		return nil, err
	}

	if len(statsVals) == 0 {
		return
	}

	for i := range statsVals[0] {
		sizes = append(sizes, uint64(statsVals[0][i].(float64)))
	}

	return
}

func (d *dynamicCtx) MTSTableRows(
	ctx context.Context,
	accs, dbs, tbls []uint64,
	eng engine.Engine,
	forceUpdate bool,
	resetUpdateTime bool,
) (sizes []uint64, err error) {

	statsVals, err, _ := d.QueryTableStats(
		ctx, []int{TableStatsTableRows},
		accs, dbs, tbls,
		forceUpdate, resetUpdateTime, eng)
	if err != nil {
		return nil, err
	}

	if len(statsVals) == 0 {
		return
	}

	for i := range statsVals[0] {
		sizes = append(sizes, uint64(statsVals[0][i].(float64)))
	}

	return
}

/////////////// MoTableStats Implementation ///////////////

type tablePair struct {
	valid bool

	errChan chan error

	relKind    string
	pkSequence int

	acc, db, tbl    int64
	dbName, tblName string

	onlyUpdateTS bool

	snapshot types.TS
	pState   *logtailreplay.PartitionState
}

func buildTablePairFromCache(
	eng engine.Engine,
	accId, dbId, tblId uint64,
	snapshot types.TS,
	onlyUpdateTS bool,
) (tbl tablePair, ok bool) {

	item := eng.(*Engine).GetLatestCatalogCache().GetTableById(uint32(accId), dbId, tblId)
	if item == nil {
		// account, db, tbl may delete already
		// the `update_time` not change anymore
		return
	}

	if item.Kind == "v" {
		return
	}

	tbl.valid = true
	tbl.acc = int64(accId)
	tbl.db = int64(dbId)
	tbl.tbl = int64(tblId)
	tbl.tblName = item.Name
	tbl.dbName = item.DatabaseName
	tbl.relKind = item.Kind
	tbl.pkSequence = item.PrimarySeqnum
	tbl.snapshot = snapshot
	tbl.onlyUpdateTS = onlyUpdateTS

	return tbl, true
}

func (tp *tablePair) String() string {
	return fmt.Sprintf("%d-%s(%d)-%s(%d)",
		tp.acc, tp.dbName, tp.db, tp.tblName, tp.tbl)
}

func (tp *tablePair) Done(err error) {
	if tp.errChan != nil {
		tp.errChan <- err
		tp.errChan = nil
	}
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
		return eng.(*Engine).dynamicCtx.tableStatsExecutor(ctx, service, eng)
	}
}

func turn2SysCtx(ctx context.Context) context.Context {
	newCtx := ctx
	if val := ctx.Value(defines.TenantIDKey{}); val == nil || val.(uint32) != catalog.System_Account {
		newCtx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}

	return newCtx
}

func (d *dynamicCtx) LaunchMTSTasksForUT() {
	d.launchTask(gamaTaskName)
	d.launchTask(betaTaskName)
}

func (d *dynamicCtx) tableStatsExecutor(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	newCtx := turn2SysCtx(ctx)

	tickerDur := time.Second
	executeTicker := time.NewTicker(tickerDur)

	for {
		select {
		case <-newCtx.Done():
			logutil.Info(logHeader,
				zap.String("source", "table stats top executor"),
				zap.String("exit by ctx done", fmt.Sprintf("%v", newCtx.Err())))
			return newCtx.Err()

		case <-executeTicker.C:
			if d.checkMoveOnTask() {
				continue
			}

			if err = d.prepare(newCtx, service, eng); err != nil {
				return err
			}

			d.Lock()
			tbls := d.tableStock.tbls[:]
			d.Unlock()

			if err = d.alphaTask(
				newCtx, service, eng,
				tbls,
				"main routine",
			); err != nil {
				logutil.Info(logHeader,
					zap.String("source", "table stats top executor"),
					zap.String("exit by alpha err", err.Error()))
				return err
			}

			d.Lock()
			executeTicker.Reset(d.conf.UpdateDuration)
			d.tableStock.tbls = d.tableStock.tbls[:0]
			d.Unlock()
		}
	}
}

func (d *dynamicCtx) prepare(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	// gama task running only on a specified cn
	d.launchTask(gamaTaskName)

	d.Lock()
	defer d.Unlock()

	offsetTS := types.TS{}
	for len(d.tableStock.tbls) == 0 {
		accs, dbs, tbls, ts, err := d.getCandidates(ctx, service, eng, d.conf.GetTableListLimit, offsetTS)
		if err != nil {
			return err
		}

		if len(ts) == 0 {
			break
		}

		err = getChangedTableList(
			ctx, service, eng, accs, dbs, tbls, ts,
			&d.tableStock.tbls,
			&d.tableStock.newest)

		if err != nil {
			return err
		}

		// in case of all candidates have been deleted.
		offsetTS = types.TimestampToTS(*ts[len(ts)-1])

		if len(d.tableStock.tbls) == 0 && offsetTS.IsEmpty() {
			// there exists a large number of deleted table which are new inserts
			logutil.Info(logHeader,
				zap.String("source", "prepare"),
				zap.String("info", "found new inserts deletes, force clean"))

			d.NotifyCleanDeletes()
			break
		}
	}

	return err
}

func (d *dynamicCtx) alphaTask(
	ctx context.Context,
	service string,
	eng engine.Engine,
	tbls []tablePair,
	caller string,
) (err error) {

	if len(tbls) == 0 {
		return
	}

	// maybe the task exited, need to launch a new one
	d.launchTask(betaTaskName)

	var (
		requestCnt       = len(tbls)
		errWaitToReceive = len(tbls)
		ticker           *time.Ticker
		processed        int

		enterWait  bool
		waitErrDur time.Duration
	)

	// batCnt -> [200, 500]
	batCnt := min(max(500, len(tbls)/5), 1000)

	now := time.Now()
	defer func() {
		dur := time.Since(now)

		logutil.Info(logHeader,
			zap.String("source", "alpha task"),
			zap.Int("processed", processed),
			zap.Int("requested", requestCnt),
			zap.Int("batch count", batCnt),
			zap.Duration("takes", dur),
			zap.Duration("wait err", waitErrDur),
			zap.String("caller", caller))

		v2.AlphaTaskDurationHistogram.Observe(dur.Seconds())
		v2.AlphaTaskCountingHistogram.Observe(float64(processed))
	}()

	wg := sync.WaitGroup{}

	errQueue := make(chan error, len(tbls)*2)
	ticker = time.NewTicker(time.Millisecond * 10)

	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return err

		case err = <-errQueue:
			if err != nil {
				logutil.Error(logHeader,
					zap.String("source", "alpha task received err"),
					zap.Error(err))
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
				d.tblQueue <- tablePair{}
				ticker.Reset(time.Second)

				if enterWait {
					waitErrDur += time.Second
				} else {
					enterWait = true
				}

				if waitErrDur > time.Minute*5 {
					// waiting reached the limit
					return nil
				}

				if waitErrDur > 0 && waitErrDur%(time.Second*10) == 0 {
					logutil.Warn(logHeader,
						zap.String("source", "alpha task"),
						zap.String("event", "waited err another 10s"),
						zap.String("caller", caller),
						zap.Int("total", processed),
						zap.Int("left", errWaitToReceive))
				}

				continue
			}

			start := time.Now()
			submitted := 0
			for i := 0; i < batCnt && i < len(tbls); i++ {
				submitted++
				wg.Add(1)

				err = d.alphaTaskPool.Submit(func() {
					defer wg.Done()

					var err2 error
					var pState *logtailreplay.PartitionState
					if !tbls[i].onlyUpdateTS {
						if pState, err2 = subscribeTable(ctx, service, eng, tbls[i]); err2 != nil {
							logutil.Info(logHeader,
								zap.String("source", "alpha task"),
								zap.String("subscribe failed", err2.Error()),
								zap.String("tbl", tbls[i].String()))

							tbls[i].Done(err2)
							return
						}
					}

					tbls[i].pState = pState
					tbls[i].errChan = errQueue
					d.tblQueue <- tbls[i]
				})

				if err != nil {
					wg.Done()
					tbls[i].Done(err)
				}
			}

			if submitted == 0 {
				continue
			}

			wg.Wait()

			// let beta know that a batch done
			d.tblQueue <- tablePair{}

			dur := time.Since(start)
			// the longer the update takes, the longer we would pause,
			// but waiting for 1s at most, 50ms at least.
			ticker.Reset(max(min(dur, time.Second), time.Millisecond*50))

			processed += submitted
			tbls = tbls[submitted:]
		}
	}
}

func (d *dynamicCtx) betaTask(
	ctx context.Context,
	service string,
	eng engine.Engine,
) {
	var err error
	defer func() {
		logutil.Info(logHeader,
			zap.String("source", "beta task exit"),
			zap.Error(err))
	}()

	var (
		de        = eng.(*Engine)
		slBat     sync.Map
		onlyTSBat []tablePair

		bulkWait sync.WaitGroup
	)

	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return

		case tbl := <-d.tblQueue:
			if !tbl.valid {
				// an alpha batch transmit done
				d.bulkUpdateTableOnlyTS(ctx, service, onlyTSBat)

				bulkWait.Wait()
				_ = d.bulkUpdateTableStatsList(ctx, service, &slBat)

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
			if err = d.beta.taskPool.Submit(func() {
				defer bulkWait.Done()

				sl, err2 := d.statsCalculateOp(ctx, service, de.fs, tbl.snapshot, tbl.pState)
				if err2 != nil {
					tbl.Done(err2)
				} else {
					slBat.Store(tbl, sl)
				}
			}); err != nil {
				bulkWait.Done()
				tbl.Done(err)
				return
			}
		}
	}
}

func (d *dynamicCtx) NotifyCleanDeletes() {
	d.cleanDeletesQueue <- struct{}{}
}

func (d *dynamicCtx) NotifyUpdateForgotten() {
	d.updateForgottenQueue <- struct{}{}
}

func (d *dynamicCtx) gamaInsertNewTables(
	ctx context.Context,
	service string,
	eng engine.Engine,
) {

	var (
		err    error
		sql    string
		sqlRet ie.InternalExecResult
	)

	sql = fmt.Sprintf(findNewTableSQL,
		catalog.MO_CATALOG, catalog.MO_TABLES,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
	)

	sqlRet = d.executeSQL(ctx, sql, "insert new table-0: get new tables")
	if err = sqlRet.Error(); err != nil {
		return
	}

	var (
		val any

		values = make([]string, 0, sqlRet.RowCount())

		dbName, tblName    string
		accId, dbId, tblId uint64
	)

	defer func() {
		logutil.Error(logHeader,
			zap.String("source", "gama insert new table"),
			zap.Int("cnt", len(values)),
			zap.Error(err))
	}()

	valFmt := "(%d,%d,%d,'%s','%s','{}','%s',0)"

	for i := range sqlRet.RowCount() {
		if val, err = sqlRet.Value(ctx, i, 5); err != nil {
			return
		}

		if strings.ToLower(string(val.([]byte))) == "v" {
			continue
		}

		if val, err = sqlRet.Value(ctx, i, 0); err != nil {
			return
		}
		accId = uint64(val.(uint32))

		if val, err = sqlRet.Value(ctx, i, 1); err != nil {
			return
		}
		dbName = string(val.([]uint8))

		if val, err = sqlRet.Value(ctx, i, 2); err != nil {
			return
		}
		dbId = val.(uint64)

		if val, err = sqlRet.Value(ctx, i, 3); err != nil {
			return
		}
		tblName = string(val.([]uint8))

		if val, err = sqlRet.Value(ctx, i, 4); err != nil {
			return
		}
		tblId = val.(uint64)

		values = append(values, fmt.Sprintf(valFmt,
			accId, dbId, tblId, dbName, tblName,
			timestamp.Timestamp{}.ToStdTime().
				Format("2006-01-02 15:04:05.000000")))
	}

	if len(values) == 0 {
		return
	}

	sql = fmt.Sprintf(insertNewTablesSQL,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
		strings.Join(values, ","))

	sqlRet = d.executeSQL(ctx, sql, "insert new table-1: insert new tables")
	err = sqlRet.Error()
}

func decodeIdsFromMoTableStatsSqlRet(
	ctx context.Context,
	sqlRet ie.InternalExecResult,
) (ids1, ids2, ids3 []uint64, err error) {

	var val any

	for i := range sqlRet.RowCount() {
		if val, err = sqlRet.Value(ctx, i, 0); err != nil {
			continue
		}
		ids1 = append(ids1, uint64(val.(int64)))

		if sqlRet.ColumnCount() == 1 {
			continue
		}

		if val, err = sqlRet.Value(ctx, i, 1); err != nil {
			continue
		}
		ids2 = append(ids2, uint64(val.(int64)))

		if sqlRet.ColumnCount() == 2 {
			continue
		}

		if val, err = sqlRet.Value(ctx, i, 2); err != nil {
			continue
		}

		ids3 = append(ids3, uint64(val.(int64)))
	}

	return
}

func (d *dynamicCtx) gamaUpdateForgotten(
	ctx context.Context,
	service string,
	de *Engine,
	limit int,
) {
	// case 1:
	// 		incremental update tables with heartbeat update_time
	// 		may leave some tables never been updated.
	// 		this opA does such correction.
	//
	// case 2:
	// 		stats update time un changed for long time. ( >= 1H?)

	var (
		err error
		now = time.Now()

		tbls   = make([]tablePair, 0, 1)
		accIds []uint64
		dbIds  []uint64
		tblIds []uint64
	)

	defer func() {
		logutil.Info(logHeader,
			zap.String("source", "gama task"),
			zap.Int("update forgotten", len(tbls)),
			zap.Duration("takes", time.Since(now)),
			zap.Error(err))
	}()

	staleTS := types.BuildTS(time.Now().Add(-2*time.Hour).UnixNano(), 0).ToTimestamp()

	sql := fmt.Sprintf(getNullStatsSQL,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
		staleTS.ToStdTime().Format("2006-01-02 15:04:05.000000"), limit)
	sqlRet := d.executeSQL(ctx, sql, "gama task: get null stats list")
	if err = sqlRet.Error(); err != nil {
		return
	}

	to := types.BuildTS(time.Now().UnixNano(), 0)

	accIds, dbIds, tblIds, err = decodeIdsFromMoTableStatsSqlRet(ctx, sqlRet)
	if err != nil {
		return
	}

	for i := range tblIds {
		tbl, ok := buildTablePairFromCache(de, accIds[i], dbIds[i], tblIds[i], to, false)
		if !ok {
			continue
		}

		tbls = append(tbls, tbl)
	}

	if err = d.alphaTask(
		ctx, service, de, tbls, "gama opA"); err != nil {
		return
	}

	v2.GamaTaskCountingHistogram.Observe(float64(len(tbls)))
	v2.GamaTaskDurationHistogram.Observe(time.Since(now).Seconds())
}

func (d *dynamicCtx) gamaCleanDeletes(
	ctx context.Context,
	de *Engine,
) {
	//mo_account, mo_database, mo_tables col name
	colName1 := []string{
		"account_id", "dat_id", "rel_id",
	}
	// mo_table_stats col name
	colName2 := []string{
		"account_id", "database_id", "table_id",
	}

	tblName := []string{
		catalog.MOAccountTable, catalog.MO_DATABASE, catalog.MO_TABLES,
	}

	deleteByStep := func(step int) {
		var (
			err error
			now = time.Now()

			ids []uint64
		)

		defer func() {
			logutil.Info(logHeader,
				zap.String("source", "gama task"),
				zap.Int(fmt.Sprintf("deleted %s", colName2[step]), len(ids)),
				zap.Duration("takes", time.Since(now)),
				zap.String("detail", intsJoin(ids, ",")))
		}()

		sql := fmt.Sprintf(getNextCheckAliveListSQL,
			colName2[step], colName2[step], catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
			colName2[step], options.DefaultBlockMaxRows)

		sqlRet := d.executeSQL(ctx, sql, fmt.Sprintf("gama task-%d-0", step))
		if sqlRet.Error() != nil {
			return
		}

		ids, _, _, err = decodeIdsFromMoTableStatsSqlRet(ctx, sqlRet)
		if len(ids) == 0 || err != nil {
			return
		}

		sql = fmt.Sprintf(getCheckAliveSQL,
			colName1[step], catalog.MO_CATALOG, tblName[step],
			colName1[step], intsJoin(ids, ","))
		sqlRet = d.executeSQL(ctx, sql, fmt.Sprintf("gama task-%d-1", step))
		if err = sqlRet.Error(); err != nil {
			return
		}

		var val any
		for i := range sqlRet.RowCount() {
			if val, err = sqlRet.Value(ctx, i, 0); err != nil {
				continue
			}

			// remove alive id from slice
			var aliveId uint64
			if _, ok := val.(int32); ok {
				aliveId = uint64(val.(int32))
			} else {
				aliveId = val.(uint64)
			}

			idx := slices.Index(ids, aliveId)
			ids = append(ids[:idx], ids[idx+1:]...)
		}

		if len(ids) != 0 {
			sql = fmt.Sprintf(getDeleteFromStatsSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
				colName2[step], intsJoin(ids, ","))
			sqlRet = d.executeSQL(ctx, sql, fmt.Sprintf("gama task-%d-2", step))
			if err = sqlRet.Error(); err != nil {
				return
			}
		}
	}

	// clear deleted tbl, db, account

	// the stats belong to any deleted accounts/databases/tables have unchanged update time
	// since they have been deleted.
	// so opB collects tables ascending their update time and then check if they deleted.

	deleteByStep(0) // clean account
	deleteByStep(1) // clean database
	deleteByStep(2) // clean tables
}

func (d *dynamicCtx) gamaTask(
	ctx context.Context,
	service string,
	eng engine.Engine,
) {

	var (
		de = eng.(*Engine)
	)

	d.Lock()
	gamaDur := d.conf.CorrectionDuration
	gamaLimit := max(d.conf.GetTableListLimit/10, 8192)
	d.Unlock()

	randDuration := func(n int) time.Duration {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		return gamaDur + time.Duration(rnd.Intn(1*n))*time.Minute
	}

	const baseFactory = 30
	tickerA := time.NewTicker(randDuration(baseFactory))
	tickerB := time.NewTicker(randDuration(baseFactory))
	tickerC := time.NewTicker(time.Millisecond)

	for {
		select {
		case <-ctx.Done():
			logutil.Info(logHeader,
				zap.String("source", "gama task exit"),
				zap.Error(ctx.Err()))
			return

		case <-tickerA.C:
			d.gama.taskPool.Submit(func() {
				d.gamaUpdateForgotten(ctx, service, de, gamaLimit)
			})
			tickerA.Reset(randDuration(baseFactory))

		case <-d.updateForgottenQueue:
			d.gama.taskPool.Submit(func() {
				d.gamaUpdateForgotten(ctx, service, de, gamaLimit)
			})
			tickerA.Reset(randDuration(baseFactory))

		case <-tickerB.C:
			d.gama.taskPool.Submit(func() {
				d.gamaCleanDeletes(ctx, de)
			})
			tickerB.Reset(randDuration(baseFactory))

		case <-d.cleanDeletesQueue:
			// emergence, do clean now
			d.gama.taskPool.Submit(func() {
				d.gamaCleanDeletes(ctx, de)
			})
			tickerB.Reset(randDuration(baseFactory))

		case <-tickerC.C:
			d.gama.taskPool.Submit(func() {
				d.gamaInsertNewTables(ctx, service, de)
			})
			// try insert table at [1, 5] min
			tickerC.Reset(randDuration(5))
		}
	}
}

func (d *dynamicCtx) statsCalculateOp(
	ctx context.Context,
	service string,
	fs fileservice.FileService,
	snapshot types.TS,
	pState *logtailreplay.PartitionState,
) (sl statsList, err error) {

	bcs := betaCycleStash{
		born:       time.Now(),
		snapshot:   snapshot,
		dataObjIds: d.objIdsPool.Get().(*[]types.Objectid),
	}

	defer func() {
		*bcs.dataObjIds = (*bcs.dataObjIds)[:0]
		d.objIdsPool.Put(bcs.dataObjIds)
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

func (d *dynamicCtx) getCandidates(
	ctx context.Context,
	service string,
	eng engine.Engine,
	limit int,
	offsetTS types.TS,
) (accs, dbs, tbls []uint64, ts []*timestamp.Timestamp, err error) {

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
	sqlRet = d.executeSQL(ctx, sql, "get next ready to update list")
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
	pairs *[]tablePair,
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

			if dbs[i] == catalog.MO_CATALOG_ID {
				// the account id will always be 0 if mo_catalog db is given on tn side,
				// later causing account_id, db_id, tbl_id un matched ==> catalogCache returns nil.
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

	var (
		newCtx = ctx
		cancel context.CancelFunc
	)

	if _, ok := ctx.Deadline(); !ok {
		newCtx, cancel = context.WithTimeout(ctx, time.Minute*2)
		defer cancel()
	}

	de := eng.(*Engine)
	txnOperator, err := de.cli.New(newCtx, timestamp.Timestamp{})
	if err != nil {
		return
	}

	defer func() {
		err = txnOperator.Commit(newCtx)
	}()

	proc := process.NewTopProcess(
		newCtx,
		de.mp, de.cli, txnOperator,
		de.fs, de.ls,
		de.qc, de.hakeeper,
		de.us, nil,
	)

	var resp *cmd_util.GetChangedTableListResp
	if len(req.AccIds) != 0 {
		handler := ctl.GetTNHandlerFunc(api.OpCode_OpGetChangedTableList, whichTN, payload, responseUnmarshaler)
		ret, err := handler(proc, "DN", "", ctl.MoCtlTNCmdSender)
		if err != nil {
			return err
		}

		resp = ret.Data.([]any)[0].(*cmd_util.GetChangedTableListResp)
	} else {
		now := types.BuildTS(time.Now().UnixNano(), 0).ToTimestamp()
		resp = &cmd_util.GetChangedTableListResp{
			Newest: &now,
		}
	}

	*to = types.TimestampToTS(*resp.Newest)

	for i := range resp.AccIds {
		tp, ok := buildTablePairFromCache(de, resp.AccIds[i],
			resp.DatabaseIds[i], resp.TableIds[i], *to, false)
		if !ok {
			continue
		}

		*pairs = append(*pairs, tp)
	}

	for i := range tbls {
		if idx := slices.Index(resp.TableIds, tbls[i]); idx != -1 {
			// need calculate, already in it
			continue
		}

		// 1. if the tbls belongs to mo_catalog
		//		force update it.
		// 2. otherwise, has no changes, only update TS

		onlyUpdateTS := true
		if dbs[i] == catalog.MO_CATALOG_ID {
			onlyUpdateTS = false
		}

		tp, ok := buildTablePairFromCache(de,
			accs[i], dbs[i], tbls[i], *to, onlyUpdateTS)
		if !ok {
			continue
		}
		*pairs = append(*pairs, tp)
	}

	return
}

func subscribeTable(
	ctx context.Context,
	service string,
	eng engine.Engine,
	tbl tablePair,
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

func (d *dynamicCtx) bulkUpdateTableStatsList(
	ctx context.Context,
	service string,
	bat *sync.Map,
) (err error) {
	var (
		val  []byte
		vals []string
		tbls []tablePair

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
		tbl := key.(tablePair)
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
			sl.took.Microseconds()))

		return true
	})

	if len(vals) == 0 {
		return
	}

	sql := fmt.Sprintf(bulkInsertOrUpdateStatsListSQL,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS,
		strings.Join(vals, ","))

	ret := d.executeSQL(ctx, sql, "bulk update table stats")
	return ret.Error()
}

func (d *dynamicCtx) bulkUpdateTableOnlyTS(
	ctx context.Context,
	service string,
	tbls []tablePair,
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

	ret := d.executeSQL(ctx, sql, "bulk update only ts")

	for i := range tbls {
		tbls[i].Done(ret.Error())
	}
}
