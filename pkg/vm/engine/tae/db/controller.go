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
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	rpc2 "github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"go.uber.org/zap"
)

type ControlCmdType uint32

const (
	ControlCmd_Noop ControlCmdType = iota
	ControlCmd_ToReplayMode
	ControlCmd_ToWriteMode
)

type stepFunc struct {
	fn   func() error
	desc string
}

type stepFuncs []stepFunc

func (s stepFuncs) Apply(msg string, reversed bool, logLevel int) (err error) {
	now := time.Now()
	defer func() {
		if logLevel > 0 {
			logutil.Info(
				msg,
				zap.String("step", "all"),
				zap.Duration("duration", time.Since(now)),
				zap.Error(err),
			)
		}
	}()

	if reversed {
		for i := len(s) - 1; i >= 0; i-- {
			start := time.Now()
			if err = s[i].fn(); err != nil {
				logutil.Error(
					msg,
					zap.String("step", s[i].desc),
					zap.Error(err),
				)
				return
			}
			if logLevel > 1 {
				logutil.Info(
					msg,
					zap.String("step", s[i].desc),
					zap.Duration("duration", time.Since(start)),
				)
			}
		}
	} else {
		for i := 0; i < len(s); i++ {
			if err = s[i].fn(); err != nil {
				logutil.Error(
					msg,
					zap.String("step", s[i].desc),
					zap.Error(err),
				)
				return
			}
			if logLevel > 1 {
				logutil.Info(
					msg,
					zap.String("step", s[i].desc),
				)
			}
		}
	}
	return nil
}

func (s stepFuncs) Push(ss ...stepFunc) stepFuncs {
	return append(s, ss...)
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
}

func (c *controlCmd) String() string {
	return fmt.Sprintf("ControlCmd{id:%s, typ:%d, sarg:%s}", c.id, c.typ, c.sarg)
}

func (c *controlCmd) setError(err error) {
	c.err = err
	c.wg.Done()
}

func (c *controlCmd) waitDone() {
	c.wg.Wait()
}

type Controller struct {
	queue sm.Queue
	db    *DB
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

func (c *Controller) onCmd(cmds ...any) {
	for _, cmd := range cmds {
		command := cmd.(*controlCmd)
		switch command.typ {
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
	// TODO

	// 2. switch the checkpoint|diskcleaner to replay mode

	// 2.1 remove GC disk cron job. no new GC job will be issued from now on
	RemoveCronJob(c.db, CronJobs_Name_GCDisk)
	RemoveCronJob(c.db, CronJobs_Name_GCCheckpoint)
	if err = c.db.DiskCleaner.SwitchToReplayMode(ctx); err != nil {
		// Rollback
		return
	}
	// 2.x TODO: checkpoint runner
	flushCfg := c.db.BGFlusher.GetCfg()
	c.db.BGFlusher.Stop()
	rollbackSteps.Push(
		struct {
			fn   func() error
			desc string
		}{
			fn: func() error {
				c.db.BGFlusher.Restart(checkpoint.WithFlusherCfg(flushCfg))
				return nil
			},
			desc: "start bg flusher",
		})

	// 3. build forward write request tunnel to the new write candidate
	if err = c.db.TxnServer.SwitchTxnHandleStateTo(
		rpc2.TxnForwardWait, rpc2.WithForwardTarget(metadata.TNShard{
			Address: "todo",
		})); err != nil {
		return
	}
	rollbackSteps.Push(struct {
		fn   func() error
		desc string
	}{
		fn: func() error {
			return c.db.TxnServer.SwitchTxnHandleStateTo(rpc2.TxnLocalHandle)
		},
		desc: "rollback txn handle state",
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

	// 5. start merge scheduler|checkpoint|diskcleaner
	// 5.1 TODO: start the merger|checkpoint|flusher
	c.db.BGFlusher.Restart() // TODO: Restart with new config
	rollbackSteps.Push(
		struct {
			fn   func() error
			desc string
		}{
			fn: func() error {
				c.db.BGFlusher.Stop()
				return nil
			},
			desc: "stop bg flusher",
		},
	)

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

func (c *Controller) Stop() {
	c.queue.Stop()
}

func (c *Controller) SwitchTxnMode(
	ctx context.Context,
	iarg int,
	sarg string,
) error {
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
	if _, err := c.queue.Enqueue(cmd); err != nil {
		return err
	}
	cmd.waitDone()
	return cmd.err
}
