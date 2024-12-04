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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

type ControlCmdType uint32

const (
	ControlCmd_Noop ControlCmdType = iota
	ControlCmd_ToReplayMode
	ControlCmd_ToWriteMode
)

type controlCmd struct {
	typ     ControlCmdType
	err     error
	ctx     context.Context
	wg      sync.WaitGroup
	payload []byte
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
	switch c.db.TxnMode {
	case DBTxnMode_Replay:
		cmd.setError(nil)
	case DBTxnMode_ReplayToWrite, DBTxnMode_WriteToReplay:
		cmd.setError(
			moerr.NewTxnControlErrorNoCtx("bad db txn mode %d to replay", c.db.TxnMode),
		)
	}
	// write mode -> replay mode switch steps:
	// 1. stop the merge scheduler
	// 2. switch the checkpoint|diskcleaner to replay mode
	// 3. build forward write request tunnel to the new write candidate
	// 4. build logtail tunnel to the new write candidate
	// 5. freeze the write requests consumer
	// 6. switch the txn mode to replay mode
	// 7. wait the logtail push queue to be flushed
	// 8. send change-writer-config txn to the logservice
	// 9. change the logtail push queue sourcer to the write candidate tunnel
	// 10. forward the write requests to the new write candidate
	// 11. replay the log entries from the logservice

	// TODO: error handling
}

func (c *Controller) handleToWriteCmd(cmd *controlCmd) {
	switch c.db.TxnMode {
	case DBTxnMode_Write:
		cmd.setError(nil)
	case DBTxnMode_ReplayToWrite, DBTxnMode_WriteToReplay:
		cmd.setError(
			moerr.NewTxnControlErrorNoCtx("bad db txn mode %d to write", c.db.TxnMode),
		)
	}
	// replay mode -> write mode switch steps:
	// 1. it can only be changed after it receives the change-writer-config txn from the logservice
	// 2. stop replaying the log entries
	// 3. switch the txnmgr to write mode
	// 4. unfreeze the write requests
	// 5. start merge scheduler|checkpoint|diskcleaner

	// TODO: error handling
}

func (c *Controller) Start() {
	c.queue.Start()
}

func (c *Controller) Stop() {
	c.queue.Stop()
}

func (c *Controller) ToReplayMode(ctx context.Context) error {
	c.onCmd(ControlCmd_ToReplayMode)
	return nil
}
