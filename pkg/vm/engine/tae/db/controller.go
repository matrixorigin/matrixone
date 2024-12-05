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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"go.uber.org/zap"
)

type ControlCmdType uint32

const (
	ControlCmd_Noop ControlCmdType = iota
	ControlCmd_ToReplayMode
	ControlCmd_ToWriteMode
)

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
		err   error
		start time.Time = time.Now()
	)

	logger := logutil.Info
	logger(
		"DB-SwitchToReplay-Start",
		zap.String("cmd", cmd.String()),
	)

	defer func() {
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"DB-SwitchToReplay-Done",
			zap.String("cmd", cmd.String()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err),
		)
		cmd.setError(err)
	}()

	// 1. stop the merge scheduler
	// TODO

	// 2. switch the checkpoint|diskcleaner to replay mode
	// TODO

	// 3. build forward write request tunnel to the new write candidate
	// TODO

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
	// TODO

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
		err   error
		start time.Time = time.Now()
	)

	logger := logutil.Info
	logger(
		"DB-SwitchToWrite-Start",
		zap.String("cmd", cmd.String()),
	)

	defer func() {
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"DB-SwitchToWrite-Done",
			zap.String("cmd", cmd.String()),
			zap.Duration("duration", time.Since(start)),
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
	// TODO

	// 5. start merge scheduler|checkpoint|diskcleaner
	// TODO

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
