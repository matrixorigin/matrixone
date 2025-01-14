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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
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

	getDeleteFromStatsSQL = `
				delete from
				    %s.%s
				where
					%s;`

	getUpdateTSSQL = `
				select
					table_id, update_time
				from
					%s.%s
				where
					account_id in (%v) and
				    database_id in (%v) and
				    table_id in (%v)
				group by
				    account_id, database_id, table_id, update_time;`

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

	findDeletedAccountSQL = `
				select
					distinct(A.account_id)
				from
				    %s.%s as A
				left join
					%s.%s as B
				on
					A.account_id = B.account_id
				where
				    B.account_id is NULL;`

	findDeletedDatabaseSQL = `
				select
					A.account_id, A.database_id
				from
				    %s.%s as A
				left join
					%s.%s as B
				on
					A.account_id = B.account_id and A.database_name = B.datname and A.database_id = B.dat_id
				where
				    B.datname is NULL and A.database_id not in (%v)
				group by
					A.account_id, A.database_id;
				`

	findDeletedTableSQL = `
				select
					A.account_id, A.database_id, A.table_id
				from
				    %s.%s as A
				left join
					%s.%s as B
				on
					A.account_id = B.account_id and
				    A.database_name = B.reldatabase and
				    A.table_name = B.relname and
				    A.database_id = B.reldatabase_id and
				    A.table_id = B.rel_id
				where
				    B.relname is NULL and A.database_id != %d
				group by
					A.account_id, A.database_id, A.table_id;
				`

	getNullStatsSQL = `
				select
					account_id, database_id, table_id
				from
				    %s.%s
				where
				    table_stats = "{}"
				limit
					%d;`

	getNextCheckBatchSQL = `
				select
					account_id, database_id, table_id
				from
				    %s.%s
				where
				    table_id > %d and database_id != %d
				group by
				    account_id, database_id, table_id
				order by
				    table_id asc
				limit
					%d;`

	accumulateIdsByAccSQL = `
 				select
					account_id, reldatabase_id, rel_id
				from
				    %s.%s
				where
				    account_id in (%s);`

	accumulateIdsByDatabaseSQL = `
				select
					account_id, reldatabase_id, rel_id
				from
					%s.%s
				where
				    reldatabase_id in (%v)
				group by
				    account_id, reldatabase_id, rel_id;`

	accumulateIdsByTableSQL = `
				select
					account_id, reldatabase_id, rel_id
				from
					%s.%s
				where
				    rel_id in (%v)
				group by
				    account_id, reldatabase_id, rel_id;`

	getSingleTableStatsForUpdateSQL = `
				select
					table_stats
				from
					%s.%s
				where
				    account_id = %d and database_id = %d and table_id = %d
				for update;`

	findAccountByTable = `
				select
					account_id, rel_id
				from
				    %s.%s
				where
				    rel_id in (%v);`
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

	specialStart          = "start"
	specialMinimalChecked = "minimal checked"

	specialAccountId  = 0
	specialDatabaseId = 0
	specialTableId    = 1
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
		eng.dynamicCtx.insertNewTableQueue = make(chan struct{})

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

	tblQueue chan tablePair

	cleanDeletesQueue    chan struct{}
	updateForgottenQueue chan struct{}
	insertNewTableQueue  chan struct{}

	de         *Engine
	objIdsPool sync.Pool

	tableStock struct {
		tbls []tablePair

		specialFrom types.TS
		specialTo   types.TS
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

// acc.x,y,z
// db.a,b,c
// tbl.r,s,t
// gama.forgotten|clean|new
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
		err error
		ok  bool
		id  uint64
	)

	idsByCmd := func(str string) []uint64 {
		idStrs := strings.Split(str, ",")

		ids := make([]uint64, 0, len(idStrs))

		for i := range idStrs {
			id, err = strconv.ParseUint(idStrs[i], 10, 64)
			if err == nil {
				ids = append(ids, id)
			}
		}

		return ids
	}

	recomputingAccount := func(str string) string {
		ids := idsByCmd(str)
		_, _, ok, err = d.QueryTableStatsByAccounts(
			context.Background(), nil, ids, false, true)

		if ok {
			return "success"
		} else {
			return fmt.Sprintf("failed, err: %v", err)
		}
	}

	recomputingDatabase := func(str string) string {
		ids := idsByCmd(str)
		_, _, ok, err = d.QueryTableStatsByDatabase(
			context.Background(), nil, ids, false, true)
		if ok {
			return "success"
		} else {
			return fmt.Sprintf("failed, err: %v", err)
		}
	}

	recomputingTable := func(str string) string {
		ids := idsByCmd(str)
		_, _, ok, err = d.QueryTableStatsByTable(
			context.Background(), nil, ids, false, true)
		if ok {
			return "success"
		} else {
			return fmt.Sprintf("failed, err: %v", err)
		}
	}

	strs := strings.Split(para, ".")
	if len(strs) != 2 {
		return "invalid command"
	}

	switch strs[0] {
	case "acc":
		return recomputingAccount(strs[1])
	case "db":
		return recomputingDatabase(strs[1])
	case "tbl":
		return recomputingTable(strs[1])
	case "gama":
		switch strs[1] {
		case "forgotten":
			d.NotifyUpdateForgotten()
		case "clean":
			d.NotifyCleanDeletes()
		case "new":
			d.NotifyInsertNewTable()
		default:
			return "invalid command"
		}
		return "success"
	default:
		return "invalid command"
	}
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

type builder struct {
	buf []byte
}

func (b *builder) writeString(s string) {
	b.buf = append(b.buf, s...)
}

func (b *builder) string() string {
	return unsafe.String(unsafe.SliceData(b.buf), len(b.buf))
}

func (b *builder) reset() {
	b.buf = b.buf[:0]
}

var builderPool = sync.Pool{
	New: func() any {
		return &builder{
			buf: make([]byte, 0, 8192),
		}
	},
}

func intsJoin(items []uint64, delimiter string) (string, func()) {
	bb := builderPool.Get().(*builder)
	for i, item := range items {
		bb.writeString(strconv.FormatUint(item, 10))
		if i < len(items)-1 {
			bb.writeString(delimiter)
		}
	}

	return bb.string(), func() {
		bb.reset()
		builderPool.Put(bb)
	}
}

func joinAccountDatabase(accIds []uint64, dbIds []uint64) (string, func()) {
	bb := builderPool.Get().(*builder)
	for i := range accIds {
		bb.writeString("(account_id = ")
		bb.writeString(strconv.FormatUint(accIds[i], 10))
		bb.writeString(" AND database_id = ")
		bb.writeString(strconv.FormatUint(dbIds[i], 10))
		bb.writeString(")")

		if i < len(accIds)-1 {
			bb.writeString(" OR ")
		}
	}

	return bb.string(), func() {
		bb.reset()
		builderPool.Put(bb)
	}
}

func joinAccountDatabaseTable(
	accIds []uint64, dbIds []uint64, tblIds []uint64,
) (string, func()) {

	bb := builderPool.Get().(*builder)
	for i := range accIds {
		bb.writeString("(account_id = ")
		bb.writeString(strconv.FormatUint(accIds[i], 10))
		bb.writeString(" AND database_id = ")
		bb.writeString(strconv.FormatUint(dbIds[i], 10))
		bb.writeString(" AND table_id = ")
		bb.writeString(strconv.FormatUint(tblIds[i], 10))
		bb.writeString(")")

		if i < len(accIds)-1 {
			bb.writeString(" OR ")
		}
	}

	return bb.string(), func() {
		bb.reset()
		builderPool.Put(bb)
	}
}

func (d *dynamicCtx) executeSQL(ctx context.Context, sql string, hint string) ie.InternalExecResult {
	exec := d.executorPool.Get()
	defer d.executorPool.Put(exec)

	ctx = turn2SysCtx(ctx)

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

func (d *dynamicCtx) forceUpdateQuery(
	ctx context.Context,
	wantedStatsIdxes []int,
	accs, dbs, tbls []uint64,
	resetUpdateTime bool,
	caller string,
) (statsVals [][]any, err error) {

	if len(tbls) == 0 {
		return
	}

	var (
		to      types.TS
		val     any
		stdTime time.Time
		pairs   = make([]tablePair, 0, len(tbls))
		oldTS   = make([]timestamp.Timestamp, len(tbls))
	)

	if !resetUpdateTime {

		accStr, release1 := intsJoin(accs, ",")
		dbStr, release2 := intsJoin(dbs, ",")
		tblStr, release3 := intsJoin(tbls, ",")

		defer func() {
			release1()
			release2()
			release3()
		}()

		sql := fmt.Sprintf(getUpdateTSSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, accStr, dbStr, tblStr)

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
			oldTS[idx] = timestamp.Timestamp{PhysicalTime: stdTime.UnixNano()}
		}

		if err = getChangedTableList(
			ctx, d.de.service, d.de,
			accs, dbs, tbls,
			oldTS, &pairs, &to, cmd_util.CheckChanged); err != nil {
			return
		}

	} else {
		to = types.BuildTS(time.Now().UnixNano(), 0)

		for i := range tbls {
			tbl, ok := buildTablePairFromCache(d.de, accs[i], dbs[i], tbls[i], to, false)
			if !ok {
				continue
			}

			pairs = append(pairs, tbl)
		}
	}

	if err = d.callAlphaWithRetry(
		ctx, d.de.service, pairs, caller, 2); err != nil {
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

	accStr, release1 := intsJoin(accs, ",")
	dbStr, release2 := intsJoin(dbs, ",")
	tblStr, release3 := intsJoin(tbls, ",")

	defer func() {
		release1()
		release2()
		release3()
	}()

	sql := fmt.Sprintf(getTableStatsSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, accStr, dbStr, tblStr)

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

func (d *dynamicCtx) queryTableStatsByXXX(
	ctx context.Context,
	sql string,
	hint string,
	wantedStatsIdxes []int,
	forceUpdate bool,
	resetUpdateTime bool,
) (statsVals [][]any, accs, dbs, tbls []uint64, ok bool, err error) {

	sqlRet := d.executeSQL(ctx, sql, hint)
	if err = sqlRet.Error(); err != nil {
		return
	}

	if accs, dbs, tbls, err = decodeIdsFromMOTablesSqlRet(ctx, sqlRet); err != nil {
		return
	}

	statsVals, err, ok = d.QueryTableStats(
		ctx, wantedStatsIdxes, accs, dbs, tbls, forceUpdate, resetUpdateTime)

	return
}

func (d *dynamicCtx) QueryTableStatsByAccounts(
	ctx context.Context,
	wantedStatsIdxes []int,
	accs []uint64,
	forceUpdate bool,
	resetUpdateTime bool,
) (statsVals [][]any, retAcc []uint64, ok bool, err error) {

	if len(accs) == 0 {
		return
	}

	now := time.Now()

	accStr, release := intsJoin(accs, ",")
	sql := fmt.Sprintf(accumulateIdsByAccSQL, catalog.MO_CATALOG, catalog.MO_TABLES, accStr)
	release()

	var (
		tbls []uint64
	)

	statsVals, retAcc, _, tbls, ok, err = d.queryTableStatsByXXX(
		ctx, sql, "query table stats by account",
		wantedStatsIdxes, forceUpdate, resetUpdateTime)

	logutil.Info(logHeader,
		zap.String("source", "QueryTableStatsByAccounts"),
		zap.Int("acc cnt", len(accs)),
		zap.Int("tbl cnt", len(tbls)),
		zap.Bool("forceUpdate", forceUpdate),
		zap.Bool("resetUpdateTime", resetUpdateTime),
		zap.Duration("takes", time.Since(now)),
		zap.Bool("ok", ok),
		zap.Error(err))

	return statsVals, retAcc, ok, err
}

func (d *dynamicCtx) QueryTableStatsByDatabase(
	ctx context.Context,
	wantedStatsIdxes []int,
	dbIds []uint64,
	forceUpdate bool,
	resetUpdateTime bool,
) (statsVals [][]any, retDb []uint64, ok bool, err error) {

	if len(dbIds) == 0 {
		return
	}

	now := time.Now()

	dbIdStr, release := intsJoin(dbIds, ",")
	sql := fmt.Sprintf(accumulateIdsByDatabaseSQL, catalog.MO_CATALOG, catalog.MO_TABLES, dbIdStr)
	release()

	var tbls []uint64

	statsVals, _, retDb, tbls, ok, err = d.queryTableStatsByXXX(
		ctx, sql, "query table stats by database", wantedStatsIdxes, forceUpdate, resetUpdateTime)

	logutil.Info(logHeader,
		zap.String("source", "QueryTableStatsByDatabase"),
		zap.Int("db cnt", len(dbIds)),
		zap.Int("tbl cnt", len(tbls)),
		zap.Bool("forceUpdate", forceUpdate),
		zap.Bool("resetUpdateTime", resetUpdateTime),
		zap.Duration("takes", time.Since(now)),
		zap.Bool("ok", ok),
		zap.Error(err))

	return statsVals, retDb, ok, err
}

func (d *dynamicCtx) QueryTableStatsByTable(
	ctx context.Context,
	wantedStatsIdxes []int,
	tblIds []uint64,
	forceUpdate bool,
	resetUpdateTime bool,
) (statsVals [][]any, retTbls []uint64, ok bool, err error) {

	if len(tblIds) == 0 {
		return
	}

	now := time.Now()

	tblStr, release := intsJoin(tblIds, ",")
	sql := fmt.Sprintf(accumulateIdsByTableSQL, catalog.MO_CATALOG, catalog.MO_TABLES, tblStr)
	release()

	statsVals, _, _, retTbls, ok, err = d.queryTableStatsByXXX(
		ctx, sql, "query table stats by table", wantedStatsIdxes, forceUpdate, resetUpdateTime)

	logutil.Info(logHeader,
		zap.String("source", "QueryTableStatsByTable"),
		zap.Int("tbl cnt", len(tblIds)),
		zap.Bool("forceUpdate", forceUpdate),
		zap.Bool("resetUpdateTime", resetUpdateTime),
		zap.Duration("takes", time.Since(now)),
		zap.Bool("ok", ok),
		zap.Error(err))

	return statsVals, retTbls, ok, err
}

func (d *dynamicCtx) QueryTableStats(
	ctx context.Context,
	wantedStatsIdxes []int,
	accs, dbs, tbls []uint64,
	forceUpdate bool,
	resetUpdateTime bool,
) (statsVals [][]any, err error, ok bool) {

	d.Lock()
	useOld := d.conf.StatsUsingOldImpl
	d.Unlock()
	if useOld {
		return
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
		statsVals, err = d.forceUpdateQuery(
			newCtx, wantedStatsIdxes,
			accs, dbs, tbls,
			resetUpdateTime,
			"query table stats")
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
		forceUpdate, resetUpdateTime)
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
		forceUpdate, resetUpdateTime)
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

	errChan chan alphaError

	relKind    string
	pkSequence int

	acc, db, tbl    int64
	dbName, tblName string

	onlyUpdateTS bool

	snapshot types.TS
	pState   *logtailreplay.PartitionState

	alphaOrder int
}

type alphaError struct {
	order int
	err   error
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
		tp.errChan <- alphaError{
			err:   err,
			order: tp.alphaOrder,
		}
		tp.errChan = nil
	}

	tp.pState = nil
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

func (d *dynamicCtx) cleanTableStock() {
	d.Lock()
	defer d.Unlock()

	for i := range d.tableStock.tbls {
		// cannot pin this pState
		d.tableStock.tbls[i].pState = nil
	}
	d.tableStock.tbls = d.tableStock.tbls[:0]
}

func (d *dynamicCtx) tableStatsExecutor(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (err error) {

	defer func() {
		d.cleanTableStock()
		logutil.Info(logHeader,
			zap.String("source", "table stats top executor"),
			zap.String("exit by", fmt.Sprintf("%v", err)))
	}()

	var (
		timeout bool
		newCtx  = turn2SysCtx(ctx)

		tickerDur     = time.Second
		executeTicker = time.NewTicker(tickerDur)
	)

	for {
		select {
		case <-newCtx.Done():
			err = newCtx.Err()
			return

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

			if timeout, err = d.alphaTask(
				newCtx, service,
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
			if len(d.tableStock.tbls) > 0 && !timeout && err == nil {
				// if alpha timeout, this round should mark as failed,
				// skip the update of the special stats start.
				if _, err = d.updateSpecialStatsStart(
					ctx, service, eng, d.tableStock.specialTo, d.tableStock.specialFrom); err != nil {
					d.Unlock()
					return err
				}
			}

			d.Unlock()

			d.cleanTableStock()
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

	var (
		from types.TS
		to   types.TS
	)

	if from, to, err = d.getBetweenForChangedList(ctx, service, eng); err != nil {
		return
	}

	d.tableStock.specialTo = to
	d.tableStock.specialFrom = from

	err = getChangedTableList(
		ctx, service, eng, nil, nil, nil,
		[]timestamp.Timestamp{from.ToTimestamp(), to.ToTimestamp()},
		&d.tableStock.tbls,
		&d.tableStock.specialTo, cmd_util.CollectChanged)

	return err
}

func (d *dynamicCtx) callAlphaWithRetry(
	ctx context.Context,
	service string,
	tbls []tablePair,
	caller string,
	retryTimes int,
) (err error) {

	var (
		round   int
		timeout bool
	)

	defer func() {
		if round > 1 || round >= retryTimes || timeout {
			logutil.Warn(logHeader,
				zap.String("source", caller),
				zap.Int("alpha tried", round),
				zap.Int("specified retry times", retryTimes),
				zap.Bool("final round timeout", timeout))
		}
	}()

	for {
		if timeout, err = d.alphaTask(
			ctx, d.de.service, tbls,
			caller); err != nil {
			return err
		}

		round++
		if !timeout || round >= retryTimes {
			break
		}

		time.Sleep(time.Second)
	}

	return nil
}

func (d *dynamicCtx) alphaTask(
	ctx context.Context,
	service string,
	tbls []tablePair,
	caller string,
) (timeout bool, err error) {

	// maybe the task exited, need to launch a new one
	d.launchTask(betaTaskName)

	var (
		requestCnt       = len(tbls)
		errWaitToReceive = len(tbls)
		ticker           *time.Ticker
		processed        int

		enterWait  bool
		waitErrDur time.Duration

		tblBackup    = tbls[:]
		orderCounter int

		timeoutBuf = bytes.Buffer{}
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
			zap.String("caller", caller),
			zap.String("timeout tbl", timeoutBuf.String()))

		v2.AlphaTaskDurationHistogram.Observe(dur.Seconds())
		v2.AlphaTaskCountingHistogram.Observe(float64(processed))
	}()

	if len(tbls) == 0 {
		return
	}

	for i := range tbls {
		tbls[i].alphaOrder = orderCounter
		orderCounter++
	}

	wg := sync.WaitGroup{}

	errQueue := make(chan alphaError, len(tbls)*2)
	ticker = time.NewTicker(time.Millisecond * 10)

	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return

		case ae := <-errQueue:
			if ae.err != nil {
				logutil.Error(logHeader,
					zap.String("source", "alpha task received err"),
					zap.Error(ae.err))
			}

			tblBackup[ae.order].valid = false

			errWaitToReceive--
			if errWaitToReceive <= 0 {
				// all processed
				close(errQueue)
				return
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

				if waitErrDur >= time.Minute*5 {
					// waiting reached the limit
					for i := range tblBackup {
						// cannot pin this pState
						tblBackup[i].pState = nil
						if !tblBackup[i].valid {
							continue
						}
						timeoutBuf.WriteString(fmt.Sprintf("%d-%d(%v)-%d(%v); ",
							tblBackup[i].acc, tblBackup[i].db, tblBackup[i].dbName,
							tblBackup[i].tbl, tblBackup[i].tblName))
					}
					return true, nil
				}

				if waitErrDur > 0 && waitErrDur%(time.Second*30) == 0 {
					logutil.Warn(logHeader,
						zap.String("source", "alpha task"),
						zap.String("event", "waited err another 30s"),
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
						if pState, err2 = subscribeTable(ctx, service, d.de, tbls[i]); err2 != nil {
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

func (d *dynamicCtx) NotifyInsertNewTable() {
	d.insertNewTableQueue <- struct{}{}
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

		pairs []tablePair

		accId, dbId, tblId uint64

		values = make([]string, 0, sqlRet.RowCount())
		now    = types.BuildTS(time.Now().UnixNano(), 0)
	)

	defer func() {
		logutil.Error(logHeader,
			zap.String("source", "gama insert new table"),
			zap.Int("cnt", len(values)),
			zap.Error(err))
	}()

	if sqlRet.RowCount() == 0 {
		return
	}

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

		if val, err = sqlRet.Value(ctx, i, 2); err != nil {
			return
		}
		dbId = val.(uint64)

		if val, err = sqlRet.Value(ctx, i, 4); err != nil {
			return
		}
		tblId = val.(uint64)

		if tp, ok := buildTablePairFromCache(
			eng, accId, dbId, tblId, now, false); !ok {
			continue
		} else {
			pairs = append(pairs, tp)
		}
	}

	err = d.callAlphaWithRetry(ctx, service, pairs, "gama insert new tables", 2)
}

func decodeIdsFromMOTablesSqlRet(
	ctx context.Context,
	sqlRet ie.InternalExecResult,
) (ids1, ids2, ids3 []uint64, err error) {

	var val any

	for i := range sqlRet.RowCount() {
		if val, err = sqlRet.Value(ctx, i, 0); err != nil {
			return
		}
		ids1 = append(ids1, uint64(val.(uint32)))

		if sqlRet.ColumnCount() == 1 {
			continue
		}

		if val, err = sqlRet.Value(ctx, i, 1); err != nil {
			return
		}
		ids2 = append(ids2, val.(uint64))

		if sqlRet.ColumnCount() == 2 {
			continue
		}

		if val, err = sqlRet.Value(ctx, i, 2); err != nil {
			return
		}
		ids3 = append(ids3, val.(uint64))
	}

	return ids1, ids2, ids3, nil
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

	var (
		err error
		now = time.Now()

		nullStatsCnt       int
		missUpdateCheckCnt int
	)

	defer func() {
		logutil.Info(logHeader,
			zap.String("source", "gama task"),
			zap.Int("null stats cnt", nullStatsCnt),
			zap.Int("miss update check cnt", missUpdateCheckCnt),
			zap.Duration("takes", time.Since(now)),
			zap.Error(err))
	}()

	idsToPairs := func(accIds, dbIds, tblIds []uint64, to types.TS) []tablePair {
		tbls := make([]tablePair, 0, 1)

		for i := range tblIds {
			tbl, ok := buildTablePairFromCache(de, accIds[i], dbIds[i], tblIds[i], to, false)
			if !ok {
				continue
			}

			tbls = append(tbls, tbl)
		}

		return tbls
	}

	retrieveIds := func(sql string) ([]uint64, []uint64, []uint64, error) {
		var (
			accIds []uint64
			dbIds  []uint64
			tblIds []uint64
		)

		sqlRet := d.executeSQL(ctx, sql, "gama task: get null stats list")
		if err = sqlRet.Error(); err != nil {
			return accIds, dbIds, tblIds, err
		}

		accIds, dbIds, tblIds, err = decodeIdsFromMoTableStatsSqlRet(ctx, sqlRet)
		if err != nil {
			return accIds, dbIds, tblIds, err
		}

		return accIds, dbIds, tblIds, nil
	}

	updateNullStats := func() error {
		var (
			tbls []tablePair
			sql  string

			accIds []uint64
			dbIds  []uint64
			tblIds []uint64
		)

		to := types.BuildTS(time.Now().UnixNano(), 0)

		sql = fmt.Sprintf(getNullStatsSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, limit)

		if accIds, dbIds, tblIds, err = retrieveIds(sql); err != nil {
			return err
		}

		tbls = idsToPairs(accIds, dbIds, tblIds, to)

		nullStatsCnt = len(tbls)

		err = d.callAlphaWithRetry(ctx, service, tbls, "gama update null stats", 2)

		v2.GamaTaskCountingHistogram.Observe(float64(nullStatsCnt))
		v2.GamaTaskDurationHistogram.Observe(time.Since(now).Seconds())

		return nil
	}

	checkForgotten := func() error {
		var (
			sql string

			accIds []uint64
			dbIds  []uint64
			tblIds []uint64

			minimalChecked uint64
			stats          map[string]any
		)

		if stats, err = d.getSpecialStats(ctx, service, d.de); err != nil {
			return err
		}

		if stats != nil && stats[specialMinimalChecked] != nil {
			minimalChecked = uint64(stats[specialMinimalChecked].(float64))
		}

		sql = fmt.Sprintf(getNextCheckBatchSQL,
			catalog.MO_CATALOG, catalog.MO_TABLE_STATS, minimalChecked, specialDatabaseId, limit*2)

		if accIds, dbIds, tblIds, err = retrieveIds(sql); err != nil {
			return err
		}

		missUpdateCheckCnt = len(tblIds)
		if len(tblIds) > 0 {
			if _, err = d.forceUpdateQuery(
				ctx, nil, accIds, dbIds, tblIds, false,
				"gama update forgotten"); err != nil {
				return err
			}

			minimalChecked = slices.Max(tblIds)
		} else {
			// if there has no table > minimal checked table id, means that
			// all tables have checked in this round.
			logutil.Info(logHeader,
				zap.String("source", "reset minimal checked table id"),
				zap.Uint64("last checked table id", minimalChecked))

			minimalChecked = 0
		}

		if _, err = d.updateSpecialStatsMinimalChecked(
			ctx, service, d.de, types.BuildTS(time.Now().UnixNano(), 0), minimalChecked); err != nil {
			return err
		}

		return nil
	}

	if err = updateNullStats(); err != nil {
		return
	}

	if err = checkForgotten(); err != nil {
		return
	}

}

func (d *dynamicCtx) gamaCleanDeletes(
	ctx context.Context,
	de *Engine,
) {

	var (
		sql    string
		sqlRet ie.InternalExecResult

		accIds, dbIds, tblIds []uint64
	)

	cleanAccounts := func() (deleted []uint64, err error) {
		sql = fmt.Sprintf(findDeletedAccountSQL,
			catalog.MO_CATALOG, catalog.MO_TABLE_STATS, catalog.MO_CATALOG, catalog.MOAccountTable)

		sqlRet = d.executeSQL(ctx, sql, "clean accounts: find deleted account")
		if err = sqlRet.Error(); err != nil {
			return
		}

		if accIds, _, _, err = decodeIdsFromMoTableStatsSqlRet(ctx, sqlRet); err != nil {
			return
		}

		if len(accIds) == 0 {
			return
		}

		accStr, release := intsJoin(accIds, ",")
		defer func() {
			release()
		}()

		where := fmt.Sprintf("account_id in (%v)", accStr)

		sql = fmt.Sprintf(getDeleteFromStatsSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, where)
		sqlRet = d.executeSQL(ctx, sql, "clean accounts: clean deleted account")
		return accIds, sqlRet.Error()
	}

	cleanDatabase := func() (deleted []uint64, err error) {
		tmpStr, release := intsJoin([]uint64{specialDatabaseId, catalog.MO_CATALOG_ID}, ",")

		sql = fmt.Sprintf(findDeletedDatabaseSQL, catalog.MO_CATALOG,
			catalog.MO_TABLE_STATS, catalog.MO_CATALOG, catalog.MO_DATABASE, tmpStr)
		release()

		sqlRet = d.executeSQL(ctx, sql, "clean database: find deleted database")
		if err = sqlRet.Error(); err != nil {
			return
		}

		if accIds, dbIds, _, err = decodeIdsFromMoTableStatsSqlRet(ctx, sqlRet); err != nil {
			return
		}

		if len(accIds) == 0 {
			return
		}

		where, release := joinAccountDatabase(accIds, dbIds)
		sql = fmt.Sprintf(getDeleteFromStatsSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, where)
		release()

		sqlRet = d.executeSQL(ctx, sql, "clean database: clean deleted database")
		return dbIds, sqlRet.Error()
	}

	cleanTable := func() (deleted []uint64, err error) {
		sql = fmt.Sprintf(findDeletedTableSQL,
			catalog.MO_CATALOG, catalog.MO_TABLE_STATS, catalog.MO_CATALOG, catalog.MO_TABLES, specialDatabaseId)

		sqlRet = d.executeSQL(ctx, sql, "clean table: find deleted table")
		if err = sqlRet.Error(); err != nil {
			return
		}

		if accIds, dbIds, tblIds, err = decodeIdsFromMoTableStatsSqlRet(ctx, sqlRet); err != nil {
			return
		}

		if len(accIds) == 0 {
			return
		}

		where, release := joinAccountDatabaseTable(accIds, dbIds, tblIds)
		sql = fmt.Sprintf(getDeleteFromStatsSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, where)
		release()

		sqlRet = d.executeSQL(ctx, sql, "clean table: clean deleted table")
		return tblIds, sqlRet.Error()
	}

	var (
		err1 error
		err2 error
		err3 error

		now = time.Now()

		delAcc   []uint64
		delDB    []uint64
		delTable []uint64
	)

	defer func() {
		logutil.Info(logHeader,
			zap.String("source", "gama clean deletes"),
			zap.Duration("takes", time.Since(now)),
			zap.Any("clean account err", err1),
			zap.Any("clean database err", err2),
			zap.Any("clean table err", err3),
			zap.Any("clean accounts cnt", len(delAcc)),
			zap.Any("clean database cnt", len(delDB)),
			zap.Any("clean table cnt", len(delTable)))
	}()

	delAcc, err1 = cleanAccounts()
	delDB, err2 = cleanDatabase()
	delTable, err3 = cleanTable()
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

	const baseFactory = 60
	tickerA := time.NewTicker(randDuration(baseFactory))
	tickerB := time.NewTicker(randDuration(baseFactory))
	tickerC := time.NewTicker(randDuration(baseFactory))

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
			tickerC.Reset(randDuration(baseFactory))

		case <-d.insertNewTableQueue:
			d.gama.taskPool.Submit(func() {
				d.gamaInsertNewTables(ctx, service, de)
			})
			tickerC.Reset(randDuration(baseFactory))
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

func (d *dynamicCtx) getSpecialStats(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (map[string]any, error) {

	var (
		err    error
		val    any
		sql    string
		stats  map[string]any
		sqlRet ie.InternalExecResult
	)

	sql = fmt.Sprintf(getSingleTableStatsForUpdateSQL,
		catalog.MO_CATALOG, catalog.MO_TABLE_STATS, specialAccountId, specialDatabaseId, specialTableId)

	sqlRet = d.executeSQL(ctx, sql, "get special stats")
	if err = sqlRet.Error(); err != nil {
		return nil, err
	}

	if sqlRet.RowCount() == 0 {
		return nil, nil
	}

	if val, err = sqlRet.Value(ctx, 0, 0); err != nil {
		return nil, err
	}

	if err = json.Unmarshal([]byte(val.(bytejson.ByteJson).String()), &stats); err != nil {
		return nil, err
	}

	return stats, nil
}

func (d *dynamicCtx) updateSpecialStatsMinimalChecked(
	ctx context.Context,
	service string,
	eng engine.Engine,
	snapshot types.TS,
	minimalChecked uint64,
) (bool, error) {

	var (
		err    error
		val    []byte
		stats  map[string]any
		sql    string
		sqlRet ie.InternalExecResult
	)

	if stats, err = d.getSpecialStats(ctx, service, eng); err != nil {
		return false, err
	}

	if stats == nil {
		stats = make(map[string]any)
	}

	stats[specialMinimalChecked] = minimalChecked
	if val, err = json.Marshal(stats); err != nil {
		return false, err
	}

	record := fmt.Sprintf("(%d,%d,%d,'%s','%s','%s','%s',%d)",
		specialAccountId, specialDatabaseId, specialTableId, "", "", string(val),
		snapshot.ToTimestamp().ToStdTime().Format("2006-01-02 15:04:05.000000"), 0)

	sql = fmt.Sprintf(bulkInsertOrUpdateStatsListSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, record)

	sqlRet = d.executeSQL(ctx, sql, "updateSpecialStatsMinimalChecked")

	return true, sqlRet.Error()
}

func (d *dynamicCtx) updateSpecialStatsStart(
	ctx context.Context,
	service string,
	eng engine.Engine,
	newStart types.TS,
	oldStart types.TS,
) (bool, error) {

	var (
		err    error
		val    []byte
		stats  map[string]any
		sql    string
		sqlRet ie.InternalExecResult
	)

	if stats, err = d.getSpecialStats(ctx, service, eng); err != nil {
		return false, err
	}

	if stats != nil && stats[specialStart] != nil {
		start := stats[specialStart].(string)
		old := oldStart.ToTimestamp().ToStdTime().Format("2006-01-02 15:04:05.000000")
		if old != start {
			logutil.Info(logHeader,
				zap.String("source", "update special stats start"),
				zap.String("given up update", "the start changed already"),
				zap.String("current", start),
				zap.String("last read", old))

			return false, nil
		}
	}

	if stats == nil {
		stats = make(map[string]any)
	}

	ns := newStart.ToTimestamp().ToStdTime().Format("2006-01-02 15:04:05.000000")
	stats[specialStart] = ns
	if val, err = json.Marshal(stats); err != nil {
		return false, err
	}

	record := fmt.Sprintf("(%d,%d,%d,'%s','%s','%s','%s',%d)",
		specialAccountId, specialDatabaseId, specialTableId, "", "", string(val), ns, 0)

	sql = fmt.Sprintf(bulkInsertOrUpdateStatsListSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS, record)

	sqlRet = d.executeSQL(ctx, sql, "updateSpecialStatsStart")

	return true, sqlRet.Error()
}

func (d *dynamicCtx) getBetweenForChangedList(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (from, to types.TS, err error) {

	defaultBetween := func() {
		from = types.TS{}
		to = types.BuildTS(time.Now().UnixNano(), 0)
	}

	var (
		stats map[string]any
	)

	stats, err = d.getSpecialStats(ctx, service, eng)

	if stats == nil || stats[specialStart] == nil {
		defaultBetween()
		return
	}

	start := stats[specialStart].(string)
	if len(start) == 0 {
		defaultBetween()
	}

	tt, err := time.Parse("2006-01-02 15:04:05.000000", start)
	if err != nil {
		return
	}

	from = types.BuildTS(tt.UnixNano(), 0)
	to = types.BuildTS(time.Now().UnixNano(), 0)

	return
}

func correctAccountForCatalogTables(
	ctx context.Context,
	eng *Engine,
	resp *cmd_util.GetChangedTableListResp,
) (err error) {

	// the TN cannot distinguish the account id if
	// a table belongs to the mo_catalog. here, correct the account id miss-matched.
	var mm []uint64
	for i := range resp.TableIds {
		if resp.DatabaseIds[i] == catalog.MO_CATALOG_ID {
			mm = append(mm, resp.TableIds[i])
		}
	}

	if len(mm) > 0 {
		tmpStr, release := intsJoin(mm, ",")
		sql := fmt.Sprintf(findAccountByTable, catalog.MO_CATALOG, catalog.MO_TABLES, tmpStr)
		release()

		sqlRet := eng.executeSQL(ctx, sql, "correct account for catalog tables")
		if err = sqlRet.Error(); err != nil {
			return
		}

		var (
			val any
		)

		for i := range sqlRet.RowCount() {
			if val, err = sqlRet.Value(ctx, i, 1); err != nil {
				return
			}

			tid := val.(uint64)
			idx := slices.Index(resp.TableIds, tid)

			if val, err = sqlRet.Value(ctx, i, 0); err != nil {
				return
			}

			aid := uint64(val.(uint32))

			resp.AccIds[idx] = aid
		}
	}

	return nil
}

func getChangedTableList(
	ctx context.Context,
	service string,
	eng engine.Engine,
	accs []uint64,
	dbs []uint64,
	tbls []uint64,
	ts []timestamp.Timestamp,
	pairs *[]tablePair,
	to *types.TS,
	typ cmd_util.ChangedListType,
) (err error) {

	req := &cmd_util.GetChangedTableListReq{
		Type: typ,
	}

	var (
		nullTSAcc []uint64
		nullTSDB  []uint64
		nullTSTbl []uint64
	)

	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payload := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		if typ == cmd_util.CollectChanged {
			req.TS = append(req.TS, &ts[0])
			req.TS = append(req.TS, &ts[1])
		} else {
			for i := range tbls {
				if ts[i].IsEmpty() {
					// no need to query tn
					nullTSAcc = append(nullTSAcc, accs[i])
					nullTSDB = append(nullTSDB, dbs[i])
					nullTSTbl = append(nullTSTbl, tbls[i])
				} else {
					req.AccIds = append(req.AccIds, accs[i])
					req.DatabaseIds = append(req.DatabaseIds, dbs[i])
					req.TableIds = append(req.TableIds, tbls[i])
					req.TS = append(req.TS, &ts[i])
				}
			}
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

	handler := ctl.GetTNHandlerFunc(api.OpCode_OpGetChangedTableList, whichTN, payload, responseUnmarshaler)
	ret, err := handler(proc, "DN", "", ctl.MoCtlTNCmdSender)
	if err != nil {
		return err
	}

	resp = ret.Data.([]any)[0].(*cmd_util.GetChangedTableListResp)
	if resp.Newest == nil {
		*to = types.BuildTS(time.Now().UnixNano(), 0)
	} else {
		*to = types.TimestampToTS(*resp.Newest)
	}

	if err = correctAccountForCatalogTables(ctx, eng.(*Engine), resp); err != nil {
		return
	}

	var (
		tp tablePair
		ok bool
	)

	for i := range nullTSAcc {
		if tp, ok = buildTablePairFromCache(de,
			nullTSAcc[i], nullTSDB[i], nullTSTbl[i], *to, false); !ok {
			continue
		}
		*pairs = append(*pairs, tp)
	}

	// a table in the response means there exists updates,
	// need calculate the size/rows...
	for i := range resp.AccIds {
		if tp, ok = buildTablePairFromCache(de,
			resp.AccIds[i], resp.DatabaseIds[i], resp.TableIds[i], *to, false); !ok {
			continue
		}

		*pairs = append(*pairs, tp)
	}

	// for the CheckChanged type, a table not in the response
	// means no update yet, update it's ts only.
	//for i := range tbls {
	//	if idx := slices.Index(resp.TableIds, tbls[i]); idx != -1 {
	//		// need calculate, already in it
	//		continue
	//	}
	//
	//	if tp, ok = buildTablePairFromCache(
	//		de, accs[i], dbs[i], tbls[i], *to, true); !ok {
	//		continue
	//	}
	//	*pairs = append(*pairs, tp)
	//}

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
		dObjIter objectio.ObjectIter

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
		tObjIter objectio.ObjectIter

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

				if _, release, err = ioutil.ReadDeletes(
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
