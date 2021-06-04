package ops

import (
	"fmt"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type Cmd = uint8

const (
	QUIT Cmd = iota
)

type State = int32

const (
	CREATED State = iota
	RUNNING
	STOPPING_RECEIVER
	STOPPING_CMD
	STOPPED
)

const (
	QUEUE_SIZE = 10000
)

var (
	_ iw.IOpWorker = (*OpWorker)(nil)
)

type OpWorker struct {
	OpC      chan iops.IOp
	CmdC     chan Cmd
	State    State
	Wg       sync.WaitGroup
	ClosedCh chan struct{}
}

func NewOpWorker(args ...int) *OpWorker {
	var l int
	if len(args) == 0 {
		l = QUEUE_SIZE
	} else {
		l = args[0]
		if l < 0 {
			log.Warnf("Create OpWorker with negtive queue size %d", l)
			l = QUEUE_SIZE
		}
	}
	worker := &OpWorker{
		OpC:      make(chan iops.IOp, l),
		CmdC:     make(chan Cmd, l),
		State:    CREATED,
		ClosedCh: make(chan struct{}),
	}
	return worker
}

func (w *OpWorker) Start() {
	log.Infof("Start OpWorker")
	if w.State != CREATED {
		panic("logic error")
	}
	w.State = RUNNING
	go func() {
		for {
			if atomic.LoadInt32(&w.State) == STOPPED {
				break
			}
			select {
			case op := <-w.OpC:
				w.onOp(op)
				w.Wg.Done()
			case cmd := <-w.CmdC:
				w.onCmd(cmd)
			}
		}
	}()
}

func (w *OpWorker) Stop() {
	w.StopReceiver()
	w.WaitStop()
}

func (w *OpWorker) StopReceiver() {
	state := atomic.LoadInt32(&w.State)
	if state >= STOPPING_RECEIVER {
		return
	}
	if atomic.CompareAndSwapInt32(&w.State, state, STOPPING_RECEIVER) {
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
	if atomic.CompareAndSwapInt32(&w.State, STOPPING_RECEIVER, STOPPING_CMD) {
		w.Wg.Wait()
		w.CmdC <- QUIT
	}
	<-w.ClosedCh
}

func (w *OpWorker) SendOp(op iops.IOp) bool {
	state := atomic.LoadInt32(&w.State)
	if state != RUNNING {
		return false
	}
	w.Wg.Add(1)
	if atomic.LoadInt32(&w.State) != RUNNING {
		w.Wg.Done()
		return false
	}
	w.OpC <- op
	return true
}

func (w *OpWorker) onOp(op iops.IOp) {
	// log.Info("OpWorker: onOp")
	err := op.OnExec()
	op.SetError(err)
}

func (w *OpWorker) onCmd(cmd Cmd) {
	switch cmd {
	case QUIT:
		log.Infof("Quit OpWorker")
		close(w.CmdC)
		close(w.OpC)
		if !atomic.CompareAndSwapInt32(&w.State, STOPPING_CMD, STOPPED) {
			panic("logic error")
		}
		w.ClosedCh <- struct{}{}
	default:
		panic(fmt.Sprintf("Unsupported cmd %d", cmd))
	}
}
