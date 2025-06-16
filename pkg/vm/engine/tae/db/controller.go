// Copyright 2021 Matrix Origin
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

package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	rpc2 "github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	gc2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"go.uber.org/zap"
)

type ControlCmdType uint32

const (
	ControlCmd_Customized ControlCmdType = iota
	ControlCmd_ToReplayMode
	ControlCmd_ToWriteMode
)

type ControlCmd interface {
	WaitDone()
	Error() error
}

type stepFunc struct {
	fn   func() error
	desc string
}

type stepFuncs []stepFunc

func (s *stepFuncs) Apply(msg string, reversed bool, logLevel int) (err error) {
	var (
		logger = logutil.Info
		now    = time.Now()
	)
	defer func() {
		if logLevel > 0 {
			logger(
				msg,
				zap.String("step", "all"),
				zap.Duration("duration", time.Since(now)),
				zap.Error(err),
			)
		}
	}()

	rollbackStep := func(i int) (err2 error) {
		start := time.Now()
		err2 = (*s)[i].fn()
		if err2 != nil {
			logger = logutil.Error
		}
		if logLevel > 1 || err2 != nil {
			logger(
				msg,
				zap.Int("step-i", i),
				zap.String("step-desc", (*s)[i].desc),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err2),
			)
		}
		return
	}

	if reversed {
		for i := len(*s) - 1; i >= 0; i-- {
			if err = rollbackStep(i); err != nil {
				return
			}
		}
	} else {
		for i := 0; i < len(*s); i++ {
			if err = rollbackStep(i); err != nil {
				return
			}
		}
	}
	return nil
}

func (s *stepFuncs) Add(desc string, fn func() error) {
	*s = append(*s, stepFunc{
		fn:   fn,
		desc: desc,
	})
}

func newControlCmd(
	ctx context.Context,
	typ ControlCmdType,
	sarg string,
) *controlCmd {
	cmd := &controlCmd{
		id:   uuid.Must(uuid.NewV7()),
		typ:  typ,
		ctx:  ctx,
		sarg: sarg,
	}
	cmd.wg.Add(1)
	return cmd
}

type controlCmd struct {
	id   uuid.UUID
	typ  ControlCmdType
	err  error
	ctx  context.Context
	wg   sync.WaitGroup
	sarg string
	fn   func() error
}

func (c *controlCmd) String() string {
	return fmt.Sprintf("ControlCmd{id:%s, typ:%d, sarg:%s}", c.id, c.typ, c.sarg)
}

func (c *controlCmd) setError(err error) {
	c.err = err
	c.wg.Done()
}

func (c *controlCmd) WaitDone() {
	c.wg.Wait()
}

func (c *controlCmd) Error() error {
	return c.err
}

type Controller struct {
	queue     sm.Queue
	db        *DB
	closedCmd atomic.Pointer[controlCmd]
}

func NewController(db *DB) *Controller {
	c := &Controller{
		db: db,
	}
	c.queue = sm.NewSafeQueue(
		1, 1, c.onCmd,
	)
	return c
}

func (c *Controller) stopReceiver(fn func() error) bool {
	closeCmd := newControlCmd(context.Background(), ControlCmd_Customized, "")
	closeCmd.fn = fn
	if c.closedCmd.CompareAndSwap(nil, closeCmd) {
		now := time.Now()
		for {
			if _, err := c.queue.Enqueue(closeCmd); err != nil {
				time.Sleep(time.Millisecond * 1)
				if int(time.Since(now).Seconds())%10 == 1 {
					logutil.Warn(
						"controller-stopReceiver",
						zap.Error(err),
					)
				}
				continue
			}
			closeCmd.WaitDone()
			return true
		}
	}
	c.closedCmd.Load().WaitDone()
	return false
}

func (c *Controller) onReceiveCmd(cmd *controlCmd) (err error) {
	if c.closedCmd.Load() != nil {
		err = moerr.NewInternalErrorNoCtx("controller is closed")
		cmd.setError(err)
		return
	}
	if _, err = c.queue.Enqueue(cmd); err != nil {
		cmd.setError(err)
	}
	return
}

func (c *Controller) onCmd(cmds ...any) {
	for _, cmd := range cmds {
		command := cmd.(*controlCmd)
		switch command.typ {
		case ControlCmd_Customized:
			c.handleCustomized(command)
		case ControlCmd_ToReplayMode:
			c.handleToReplayCmd(command)
		case ControlCmd_ToWriteMode:
			c.handleToWriteCmd(command)
		default:
			command.setError(
				moerr.NewInternalErrorNoCtxf("unknown command type %d", command.typ),
			)
		}
	}
}

func (c *Controller) handleCustomized(cmd *controlCmd) {
	if cmd.fn != nil {
		cmd.setError(cmd.fn())
	} else {
		cmd.setError(nil)
	}
}

func (c *Controller) handleToReplayCmd(cmd *controlCmd) {
	switch c.db.GetTxnMode() {
	case DBTxnMode_Replay:
		cmd.setError(nil)
		return
	case DBTxnMode_Write:
	default:
		cmd.setError(
			moerr.NewTxnControlErrorNoCtxf("bad db txn mode %d to replay", c.db.GetTxnMode()),
		)
	}
	// write mode -> replay mode switch steps:
	// TODO: error handling

	var (
		err           error
		start         time.Time = time.Now()
		rollbackSteps stepFuncs
	)

	ctx, cancel := context.WithTimeout(cmd.ctx, 10*time.Minute)
	defer cancel()

	logger := logutil.Info
	logger(
		"DB-SwitchToReplay-Start",
		zap.String("cmd", cmd.String()),
	)

	defer func() {
		err2 := err
		if err2 != nil {
			err = rollbackSteps.Apply("DB-SwitchToReplay-Rollback", true, 1)
		}
		if err2 != nil {
			logger = logutil.Error
		}
		if err != nil {
			logger = logutil.Fatal
		}
		logger(
			"DB-SwitchToReplay-Done",
			zap.String("cmd", cmd.String()),
			zap.Duration("duration", time.Since(start)),
			zap.Any("rollback-error", err2),
			zap.Error(err),
		)
		cmd.setError(err)
	}()

	// 1. stop the merge scheduler
	c.db.MergeScheduler.Stop()
	rollbackSteps.Add("stop merge scheduler", func() error {
		c.db.MergeScheduler.Start()
		return nil
	})
	// TODO

	// 2. switch the checkpoint|diskcleaner to replay mode

	// 2.1 remove GC disk cron job. no new GC job will be issued from now on
	RemoveCronJob(c.db, CronJobs_Name_GCDisk)
	RemoveCronJob(c.db, CronJobs_Name_GCCheckpoint)
	// RemoveCronJob(c.db, CronJobs_Name_Scanner)
	if err = c.db.DiskCleaner.SwitchToReplayMode(ctx); err != nil {
		// Rollback
		return
	}
	// 2.x TODO: checkpoint runner
	flushCfg := c.db.BGFlusher.GetCfg()
	c.db.BGFlusher.Stop()
	rollbackSteps.Add("start bg flusher", func() error {
		c.db.BGFlusher.Restart(checkpoint.WithFlusherCfg(flushCfg))
		return nil
	})

	// 3. build forward write request tunnel to the new write candidate
	if err = c.db.TxnServer.SwitchTxnHandleStateTo(
		rpc2.TxnForwardWait, rpc2.WithForwardTarget(metadata.TNShard{
			Address: "todo",
		})); err != nil {
		return
	}
	rollbackSteps.Add("rollback txn handle state", func() error {
		return c.db.TxnServer.SwitchTxnHandleStateTo(rpc2.TxnLocalHandle)
	})

	// 4. build logtail tunnel to the new write candidate
	// TODO

	// 5. freeze the write requests consumer
	// TODO

	// 6. switch the txn mode to readonly mode
	if err = c.db.TxnMgr.SwitchToReadonly(cmd.ctx); err != nil {
		c.db.TxnMgr.ToWriteMode()
		// TODO: recover the previous state
		return
	}

	// 7. wait the logtail push queue to be flushed
	// TODO

	// 8. send change-writer-config txn to the logservice
	// TODO

	// 9. change the logtail push queue sourcer to the write candidate tunnel
	// TODO

	// 10. forward the write requests to the new write candidate
	if err = c.db.TxnServer.SwitchTxnHandleStateTo(rpc2.TxnForwarding); err != nil {
		return
	}

	if err = CheckCronJobs(c.db, DBTxnMode_Replay); err != nil {
		// rollback
		return
	}
	// 11. replay the log entries from the logservice
	// 11.1 switch the txn mode to replay mode
	c.db.TxnMgr.ToReplayMode()
	// 11.2 TODO: replay the log entries

	WithTxnMode(DBTxnMode_Replay)(c.db)
}

func (c *Controller) handleToWriteCmd(cmd *controlCmd) {
	switch c.db.GetTxnMode() {
	case DBTxnMode_Write:
		cmd.setError(nil)
		return
	case DBTxnMode_Replay:
	default:
		cmd.setError(
			moerr.NewTxnControlErrorNoCtxf("bad db txn mode %d to write", c.db.GetTxnMode()),
		)
	}
	var (
		err           error
		start         time.Time = time.Now()
		rollbackSteps stepFuncs
	)

	ctx, cancel := context.WithTimeout(cmd.ctx, 10*time.Minute)
	defer cancel()

	logger := logutil.Info
	logger(
		"DB-SwitchToWrite-Start",
		zap.String("cmd", cmd.String()),
	)

	defer func() {
		err2 := err
		if err2 != nil {
			err = rollbackSteps.Apply("DB-SwitchToWrite-Rollback", true, 1)
		}
		if err2 != nil {
			logger = logutil.Error
		}
		if err != nil {
			logger = logutil.Fatal
		}
		logger(
			"DB-SwitchToWrite-Done",
			zap.String("cmd", cmd.String()),
			zap.Duration("duration", time.Since(start)),
			zap.Any("rollback-error", err2),
			zap.Error(err),
		)
		cmd.setError(err)
	}()

	// TODO: error handling
	// replay mode -> write mode switch steps:

	// 1. it can only be changed after it receives the change-writer-config txn from the logservice
	// TODO

	// 2. stop replaying the log entries
	// TODO

	// 3. switch the txnmgr to write mode
	c.db.TxnMgr.ToWriteMode()

	// 4. unfreeze the write requests
	if err = c.db.TxnServer.SwitchTxnHandleStateTo(rpc2.TxnLocalHandle); err != nil {
		return
	}

	c.db.MergeScheduler.Start()
	rollbackSteps.Add("stop merge scheduler", func() error {
		c.db.MergeScheduler.Stop()
		return nil
	})

	// 5. start merge scheduler|checkpoint|diskcleaner
	// 5.1 TODO: start the merger|checkpoint|flusher
	c.db.BGFlusher.Restart() // TODO: Restart with new config
	rollbackSteps.Add("stop bg flusher", func() error {
		c.db.BGFlusher.Stop()
		return nil
	})

	// 5.2 switch the diskcleaner to write mode
	if err = c.db.DiskCleaner.SwitchToWriteMode(ctx); err != nil {
		// Rollback
		return
	}
	if err = AddCronJob(
		c.db, CronJobs_Name_GCDisk, true,
	); err != nil {
		// Rollback
		return
	}
	if err = AddCronJob(
		c.db, CronJobs_Name_GCCheckpoint, true,
	); err != nil {
		// Rollback
		return
	}
	// if err = AddCronJob(
	// 	c.db, CronJobs_Name_Scanner, true,
	// ); err != nil {
	// 	// Rollback
	// 	return
	// }
	if err = CheckCronJobs(c.db, DBTxnMode_Write); err != nil {
		// Rollback
		return
	}
	// 5.x TODO

	WithTxnMode(DBTxnMode_Write)(c.db)
}

func (c *Controller) Start() {
	c.queue.Start()
}

func (c *Controller) Stop(fn func() error) {
	if c.stopReceiver(fn) {
		c.queue.Stop()
	}
}

func (c *Controller) ScheduleCustomized(
	ctx context.Context,
	fn func() error,
) (cmd ControlCmd, err error) {
	command := newControlCmd(ctx, ControlCmd_Customized, "")
	command.fn = fn
	if err = c.onReceiveCmd(command); err != nil {
		cmd = nil
		return
	}
	cmd = command
	return
}

func (c *Controller) SwitchTxnMode(
	ctx context.Context,
	iarg int,
	sarg string,
) (err error) {
	var typ ControlCmdType
	switch iarg {
	case 1:
		typ = ControlCmd_ToReplayMode
	case 2:
		typ = ControlCmd_ToWriteMode
	default:
		return moerr.NewTxnControlErrorNoCtxf("unknown txn mode switch iarg %d", iarg)
	}
	cmd := newControlCmd(ctx, typ, sarg)
	if err = c.onReceiveCmd(cmd); err != nil {
		return
	}
	cmd.WaitDone()
	return cmd.err
}

func (c *Controller) AssembleDB(ctx context.Context) (err error) {
	var (
		db            = c.db
		txnMode       = db.GetTxnMode()
		rollbackSteps stepFuncs
		errMsg        string
	)
	defer func() {
		if err != nil {
			logutil.Error(
				Phase_Open,
				zap.String("error-msg", errMsg),
				zap.Error(err),
			)
			if err2 := rollbackSteps.Apply(
				"DB-Assemble-Rollback", true, 2,
			); err2 != nil {
				panic(err2)
			}
		}
	}()

	if !txnMode.IsValid() {
		return moerr.NewTxnControlErrorNoCtxf("bad txn mode %d", txnMode)
	}

	txnStoreFactory := txnimpl.TxnStoreFactory(
		db.Opts.Ctx,
		db.Catalog,
		db.Wal,
		db.Runtime,
		db.Opts.MaxMessageSize,
	)

	txnFactory := txnimpl.TxnFactory(db.Catalog)
	var txnMgrOpts []txnbase.TxnManagerOption
	switch txnMode {
	case DBTxnMode_Write:
		txnMgrOpts = append(txnMgrOpts, txnbase.WithWriteMode)
	case DBTxnMode_Replay:
		txnMgrOpts = append(txnMgrOpts, txnbase.WithReplayMode)
	}
	db.TxnMgr = txnbase.NewTxnManager(
		txnStoreFactory, txnFactory, db.Opts.Clock, txnMgrOpts...,
	)
	db.Runtime.Now = db.TxnMgr.Now
	db.LogtailMgr = logtail.NewManager(
		db.Runtime,
		int(db.Opts.LogtailCfg.PageSize),
		db.TxnMgr.Now,
	)
	db.TxnMgr.CommitListener.AddTxnCommitListener(db.LogtailMgr)

	db.TxnMgr.Start(db.Opts.Ctx)
	db.LogtailMgr.Start()

	rollbackSteps.Add("stop logtail mgr", func() error {
		db.LogtailMgr.Stop()
		return nil
	})
	rollbackSteps.Add("stop txn mgr", func() error {
		db.TxnMgr.Stop()
		return nil
	})

	db.BGCheckpointRunner = checkpoint.NewRunner(
		db.Opts.Ctx,
		db.Runtime,
		db.Catalog,
		logtail.NewDirtyCollector(db.LogtailMgr, db.Opts.Clock, db.Catalog, new(catalog.LoopProcessor)),
		db.Wal,
		&checkpoint.CheckpointCfg{
			MinCount:                    db.Opts.CheckpointCfg.MinCount,
			IncrementalReservedWALCount: db.Opts.CheckpointCfg.ReservedWALEntryCount,
			IncrementalInterval:         db.Opts.CheckpointCfg.IncrementalInterval,
			GlobalMinCount:              db.Opts.CheckpointCfg.GlobalMinCount,
			GlobalHistoryDuration:       db.Opts.CheckpointCfg.GlobalVersionInterval,
		},
	)
	db.BGCheckpointRunner.Start()
	rollbackSteps.Add("stop bg checkpoint runner", func() error {
		db.BGCheckpointRunner.Stop()
		return nil
	})

	db.BGFlusher = checkpoint.NewFlusher(
		db.Runtime,
		db.BGCheckpointRunner,
		db.Catalog,
		db.BGCheckpointRunner.GetDirtyCollector(),
		db.IsReplayMode(),
		checkpoint.WithFlusherInterval(db.Opts.CheckpointCfg.FlushInterval),
		checkpoint.WithFlusherCronPeriod(db.Opts.CheckpointCfg.ScanInterval),
	)

	// TODO: WithGCInterval requires configuration parameters
	gc2.SetDeleteTimeout(db.Opts.GCCfg.GCDeleteTimeout)
	gc2.SetDeleteBatchSize(db.Opts.GCCfg.GCDeleteBatchSize)

	// sjw TODO: cleaner need to support replay and write mode
	cleaner := gc2.NewCheckpointCleaner(
		db.Opts.Ctx,
		db.Opts.SID,
		db.Opts.Fs,
		db.Wal,
		db.BGCheckpointRunner,
		gc2.WithCanGCCacheSize(db.Opts.GCCfg.CacheSize),
		gc2.WithMaxMergeCheckpointCount(db.Opts.GCCfg.GCMergeCount),
		gc2.WithEstimateRows(db.Opts.GCCfg.GCestimateRows),
		gc2.WithGCProbility(db.Opts.GCCfg.GCProbility),
		gc2.WithCheckOption(db.Opts.GCCfg.CheckGC),
		gc2.WithGCCheckpointOption(!db.Opts.CheckpointCfg.DisableGCCheckpoint))
	cleaner.AddChecker(
		func(item any) bool {
			checkpoint := item.(*checkpoint.CheckpointEntry)
			ts := types.BuildTS(time.Now().UTC().UnixNano()-int64(db.Opts.GCCfg.GCTTL), 0)
			endTS := checkpoint.GetEnd()
			return !endTS.GE(&ts)
		}, cmd_util.CheckerKeyTTL)

	db.DiskCleaner = gc2.NewDiskCleaner(cleaner, db.IsWriteMode())

	var (
		checkpointed        types.TS
		ckpLSN              uint64
		releaseReplayPinned func()
		replayCtl           *replayCtl
	)
	defer func() {
		if err != nil {
			if replayCtl != nil {
				replayCtl.Stop()
			}
		} else {
			logutil.Info(
				Phase_Open,
				zap.String("checkpointed", checkpointed.ToString()),
				zap.Uint64("checkpoint-lsn", ckpLSN),
			)
			db.ReplayCtl = replayCtl
		}
	}()
	if checkpointed, ckpLSN, releaseReplayPinned, err = c.replayFromCheckpoints(ctx); err != nil {
		return
	}
	db.TxnMgr.TryUpdateMaxCommittedTS(checkpointed)

	if replayCtl, err = db.ReplayWal(
		ctx, checkpointed, ckpLSN, releaseReplayPinned,
	); err != nil {
		return
	}

	// in the write mode
	// 1. we need to wait the replayCtl to finish the wal replay
	// 2. close the replayCtl after replaying the wal in write mode
	// in the replay mode
	// there is one replay loop running in the background
	// replayCtl is will be assigned to db.ReplayCtl
	// we can use db.ReplayCtl to control the replay loop
	if db.IsWriteMode() {
		if err = replayCtl.Wait(); err != nil {
			return
		}
		replayCtl.Stop()
		replayCtl = nil
	}

	db.MergeScheduler = merge.NewMergeScheduler(
		db.Runtime.Options.CheckpointCfg.ScanInterval,
		&merge.TNCatalogEventSource{Catalog: db.Catalog, TxnManager: db.TxnMgr},
		merge.NewTNMergeExecutor(db.Runtime),
		merge.NewStdClock(),
	)
	db.MergeScheduler.Start()
	rollbackSteps.Add("stop merge scheduler", func() error {
		db.MergeScheduler.Stop()
		return nil
	})

	// start flusher and disk cleaner
	db.BGFlusher.Start()
	rollbackSteps.Add("stop bg flusher", func() error {
		db.BGFlusher.Stop()
		return nil
	})
	db.DiskCleaner.Start()
	rollbackSteps.Add("stop disk cleaner", func() error {
		db.DiskCleaner.Stop()
		return nil
	})

	db.CronJobs = tasks.NewCancelableJobs()
	err = AddCronJobs(db)
	return
}

func (c *Controller) replayFromCheckpoints(ctx context.Context) (
	checkpointed types.TS,
	ckpLSN uint64,
	release func(),
	err error,
) {
	var (
		db  = c.db
		now = time.Now()
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			if release != nil {
				release()
				release = nil
			}
			logger = logutil.Error
		}
		logger(
			Phase_Open,
			zap.Duration("replay-checkpoints-cost", time.Since(now)),
			zap.String("checkpointed", checkpointed.ToString()),
			zap.Uint64("checkpoint-lsn", ckpLSN),
			zap.Error(err),
		)
	}()

	ckpReplayer := db.BGCheckpointRunner.BuildReplayer(ioutil.GetCheckpointDir())
	release = ckpReplayer.Close
	if err = ckpReplayer.ReadCkpFiles(); err != nil {
		return
	}

	// 1. replay three tables objectlist
	if checkpointed, ckpLSN, err = ckpReplayer.ReplayThreeTablesObjectlist(Phase_Open); err != nil {
		return
	}

	// 2. replay all table Entries
	if err = ckpReplayer.ReplayCatalog(
		db.TxnMgr.OpenOfflineTxn(checkpointed),
		Phase_Open,
	); err != nil {
		return
	}

	// 3. replay other tables' objectlist
	err = ckpReplayer.ReplayObjectlist(ctx, Phase_Open)
	return
}
