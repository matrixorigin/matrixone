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
	"fmt"
	"sync/atomic"

	"errors"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	iw "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker/base"
)

var ErrOpCancelled = errors.New("op cancelled")

type Cmd = uint8

const (
	QUIT Cmd = iota
)

type State = int32

const (
	CREATED State = iota
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

type OpExecFunc func(op iops.IOp)

type Stats struct {
	Processed uint64
	Successed uint64
	Failed    uint64
	AvgTime   int64
}

func (s *Stats) AddProcessed() {
	atomic.AddUint64(&s.Processed, uint64(1))
}

func (s *Stats) AddSuccessed() {
	atomic.AddUint64(&s.Successed, uint64(1))
}

func (s *Stats) AddFailed() {
	atomic.AddUint64(&s.Failed, uint64(1))
}

func (s *Stats) RecordTime(t int64) {
	procced := atomic.LoadUint64(&s.Processed)
	avg := atomic.LoadInt64(&s.AvgTime)
	//TODO: avgTime is wrong
	atomic.StoreInt64(&s.AvgTime, (avg*int64(procced-1)+t)/int64(procced))
}

func (s *Stats) String() string {
	procced := atomic.LoadUint64(&s.Processed)
	succ := atomic.LoadUint64(&s.Successed)
	fail := atomic.LoadUint64(&s.Failed)
	avg := atomic.LoadInt64(&s.AvgTime)
	r := fmt.Sprintf("Total: %d, Succ: %d, Fail: %d, AvgTime: %dus", procced, succ, fail, avg)
	return r
}

type OpWorker struct {
	Name       string
	OpC        chan iops.IOp
	CmdC       chan Cmd
	State      State
	Pending    int64
	ClosedCh   chan struct{}
	Stats      Stats
	ExecFunc   OpExecFunc
	CancelFunc OpExecFunc
}

func NewOpWorker(name string, args ...int) *OpWorker {
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
		Name:     name,
		OpC:      make(chan iops.IOp, l),
		CmdC:     make(chan Cmd, l),
		State:    CREATED,
		ClosedCh: make(chan struct{}),
	}
	worker.ExecFunc = worker.onOp
	worker.CancelFunc = worker.opCancelOp
	return worker
}

func (w *OpWorker) Start() {
	logutil.Debugf("%s Started", w.Name)
	if w.State != CREATED {
		panic(fmt.Sprintf("logic error: %v", w.State))
	}
	w.State = RUNNING
	go func() {
		for {
			state := atomic.LoadInt32(&w.State)
			if state == STOPPED {
				break
			}
			select {
			case op := <-w.OpC:
				w.ExecFunc(op)
				// if state == RUNNING {
				// 	w.ExecFunc(op)
				// } else {
				// 	w.CancelFunc(op)
				// }
				atomic.AddInt64(&w.Pending, int64(-1))
			case cmd := <-w.CmdC:
				w.onCmd(cmd)
			}
		}
	}()
}

func (w *OpWorker) Stop() {
	w.StopReceiver()
	w.WaitStop()
	logutil.Debugf("%s Stopped", w.Name)
}

func (w *OpWorker) StopReceiver() {
	state := atomic.LoadInt32(&w.State)
	if state >= StoppingReceiver {
		return
	}
	if atomic.CompareAndSwapInt32(&w.State, state, StoppingReceiver) {
		return
	}
}

func (w *OpWorker) WaitStop() {
	state := atomic.LoadInt32(&w.State)
	if state <= RUNNING {
		panic("logic error")
	}
	if state == STOPPED {
		return
	}
	if atomic.CompareAndSwapInt32(&w.State, StoppingReceiver, StoppingCMD) {
		pending := atomic.LoadInt64(&w.Pending)
		for {
			if pending == 0 {
				break
			}
			pending = atomic.LoadInt64(&w.Pending)
		}
		w.CmdC <- QUIT
	}
	<-w.ClosedCh
}

func (w *OpWorker) SendOp(op iops.IOp) bool {
	state := atomic.LoadInt32(&w.State)
	if state != RUNNING {
		return false
	}
	atomic.AddInt64(&w.Pending, int64(1))
	if atomic.LoadInt32(&w.State) != RUNNING {
		atomic.AddInt64(&w.Pending, int64(-1))
		return false
	}
	w.OpC <- op
	return true
}

func (w *OpWorker) opCancelOp(op iops.IOp) {
	op.SetError(ErrOpCancelled)
}

func (w *OpWorker) onOp(op iops.IOp) {
	err := op.OnExec()
	w.Stats.AddProcessed()
	if err != nil {
		w.Stats.AddFailed()
	} else {
		w.Stats.AddSuccessed()
	}
	op.SetError(err)
	w.Stats.RecordTime(op.GetExecutTime())
}

func (w *OpWorker) onCmd(cmd Cmd) {
	switch cmd {
	case QUIT:
		// log.Infof("Quit OpWorker")
		close(w.CmdC)
		close(w.OpC)
		if !atomic.CompareAndSwapInt32(&w.State, StoppingCMD, STOPPED) {
			panic("logic error")
		}
		w.ClosedCh <- struct{}{}
	default:
		panic(fmt.Sprintf("Unsupported cmd %d", cmd))
	}
}

func (w *OpWorker) StatsString() string {
	s := fmt.Sprintf("| Stats | %s | w | %s", w.Stats.String(), w.Name)
	return s
}
