package ops

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
)

type Cmd = uint8

const (
	QUIT Cmd = iota
)

const (
	QUEUE_SIZE = 10000
)

var (
	_ iw.IOpWorker = (*OpWorker)(nil)
)

type OpWorker struct {
	OpC  chan iops.IOp
	CmdC chan Cmd
	Done bool
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
		OpC:  make(chan iops.IOp, l),
		CmdC: make(chan Cmd, l),
	}
	return worker
}

func (w *OpWorker) Start() {
	log.Infof("Start OpWorker")
	go func() {
		for !w.Done {
			select {
			case op := <-w.OpC:
				w.onOp(op)
			case cmd := <-w.CmdC:
				w.onCmd(cmd)
			}
		}
	}()
}

func (w *OpWorker) Stop() {
	w.CmdC <- QUIT
}

func (w *OpWorker) SendOp(op iops.IOp) {
	w.OpC <- op
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
		w.Done = true
	default:
		panic(fmt.Sprintf("Unsupported cmd %d", cmd))
	}
}
