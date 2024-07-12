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

package ops

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	iw "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker/base"
)

type Cmd = uint8

const (
	QUIT Cmd = iota
)

const (
	CREATED int32 = iota
	RUNNING
	StoppingReceiver
	StoppingCMD
	STOPPED
)

const (
	QueueSize = 10000
)

var (
	_ iw.IOpWorker = (*OpWorker)(nil)
)

type OpExecFunc func(iops.IOp)

type Stats struct {
	Processed atomic.Uint64
	Successed atomic.Uint64
	Failed    atomic.Uint64
	AvgTime   atomic.Int64
}

func (s *Stats) AddProcessed() {
	s.Processed.Add(1)
}

func (s *Stats) AddSuccessed() {
	s.Successed.Add(1)
}

func (s *Stats) AddFailed() {
	s.Failed.Add(1)
}

func (s *Stats) RecordTime(t int64) {
	procced := s.Processed.Load()
	avg := s.AvgTime.Load()
	//TODO: avgTime is wrong
	s.AvgTime.Store((avg*int64(procced-1) + t) / int64(procced))
}

func (s *Stats) String() string {
	r := fmt.Sprintf("Total: %d, Succ: %d, Fail: %d, AvgTime: %dus",
		s.Processed.Load(),
		s.Failed.Load(),
		s.AvgTime.Load(),
		s.AvgTime.Load())
	return r
}

type OpWorker struct {
	ctx        context.Context
	name       string
	opC        chan iops.IOp
	cmdC       chan Cmd
	state      atomic.Int32
	pending    atomic.Int64
	closedC    chan struct{}
	stats      Stats
	ExecFunc   OpExecFunc
	CancelFunc OpExecFunc
}

func NewOpWorker(ctx context.Context, name string, args ...int) *OpWorker {
	var l int
	if len(args) == 0 {
		l = QueueSize
	} else {
		l = args[0]
		if l < 0 {
			logutil.Warnf("Create OpWorker with negtive queue size %d", l)
			l = QueueSize
		}
	}
	if name == "" {
		name = fmt.Sprintf("[worker-%d]", common.NextGlobalSeqNum())
	}
	worker := &OpWorker{
		ctx:     ctx,
		name:    name,
		opC:     make(chan iops.IOp, l),
		cmdC:    make(chan Cmd, l),
		closedC: make(chan struct{}),
	}
	worker.state.Store(CREATED)
	worker.ExecFunc = worker.onOp
	worker.CancelFunc = worker.opCancelOp
	return worker
}

func (w *OpWorker) Start() {
	logutil.Debugf("%s Started", w.name)
	if w.state.Load() != CREATED {
		panic(fmt.Sprintf("logic error: %v", w.state.Load()))
	}
	w.state.Store(RUNNING)
	go func() {
		for {
			state := w.state.Load()
			if state == STOPPED {
				break
			}
			select {
			case op := <-w.opC:
				w.ExecFunc(op)
				// if state == RUNNING {
				// 	w.ExecFunc(op)
				// } else {
				// 	w.CancelFunc(op)
				// }
				w.pending.Add(-1)
			case cmd := <-w.cmdC:
				w.onCmd(cmd)
			}
		}
	}()
}

func (w *OpWorker) Stop() {
	w.StopReceiver()
	w.WaitStop()
	logutil.Debugf("%s Stopped", w.name)
}

func (w *OpWorker) StopReceiver() {
	state := w.state.Load()
	if state >= StoppingReceiver {
		return
	}
	w.state.CompareAndSwap(state, StoppingReceiver)
}

func (w *OpWorker) WaitStop() {
	state := w.state.Load()
	if state <= RUNNING {
		panic("logic error")
	}
	if state == STOPPED {
		return
	}
	if w.state.CompareAndSwap(StoppingReceiver, StoppingCMD) {
		pending := w.pending.Load()
		for {
			if pending == 0 {
				break
			}
			pending = w.pending.Load()
		}
		w.cmdC <- QUIT
	}
	<-w.closedC
}

func (w *OpWorker) SendOp(op iops.IOp) bool {
	state := w.state.Load()
	if state != RUNNING {
		return false
	}
	w.pending.Add(1)
	if w.state.Load() != RUNNING {
		w.pending.Add(-1)
		return false
	}
	w.opC <- op
	return true
}

func (w *OpWorker) opCancelOp(op iops.IOp) {
	op.SetError(moerr.NewInternalErrorNoCtx("op cancelled"))
}

func (w *OpWorker) onOp(op iops.IOp) {
	err := op.OnExec(w.ctx)
	w.stats.AddProcessed()
	if err != nil {
		w.stats.AddFailed()
	} else {
		w.stats.AddSuccessed()
	}
	op.SetError(err)
	w.stats.RecordTime(op.GetExecuteTime())
}

func (w *OpWorker) onCmd(cmd Cmd) {
	switch cmd {
	case QUIT:
		// log.Infof("Quit OpWorker")
		close(w.cmdC)
		close(w.opC)
		if !w.state.CompareAndSwap(StoppingCMD, STOPPED) {
			panic("logic error")
		}
		w.closedC <- struct{}{}
	default:
		panic(fmt.Sprintf("Unsupported cmd %d", cmd))
	}
}

func (w *OpWorker) StatsString() string {
	return fmt.Sprintf("| Stats | %s | w | %s", w.stats.String(), w.name)
}
