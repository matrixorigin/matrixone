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
	typ ControlCmdType
	err error
	ctx context.Context
	wg  sync.WaitGroup
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
		case ControlCmd_ToWriteMode:
		default:
			command.setError(
				moerr.NewInternalErrorNoCtxf("unknown command type %d", command.typ),
			)
		}
	}
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
