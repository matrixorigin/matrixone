package ops

import (
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iworker "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"time"

	log "github.com/sirupsen/logrus"
)

func NewOp(impl iops.IOpInternal, w iworker.IOpWorker) *Op {
	op := &Op{
		Impl:       impl,
		Worker:     w,
		CreateTime: time.Now(),
	}
	return op
}

func (op *Op) Push() {
	r := op.Worker.SendOp(op)
	if !r {
		log.Errorf("Send op error!")
	}
}

func (op *Op) SetError(err error) {
	op.EndTime = time.Now()
	op.Err = err
	op.ErrorC <- err
}

func (op *Op) WaitDone() error {
	err := <-op.ErrorC
	return err
}

func (op *Op) PreExecute() error {
	return nil
}

func (op *Op) PostExecute() error {
	return nil
}

func (op *Op) Execute() error {
	return nil
}

func (op *Op) OnExec() error {
	op.StartTime = time.Now()
	err := op.PreExecute()
	if err != nil {
		return err
	}
	err = op.Impl.PreExecute()
	if err != nil {
		return err
	}
	err = op.Impl.Execute()
	if err != nil {
		return err
	}
	err = op.PostExecute()
	if err != nil {
		return err
	}
	err = op.Impl.PostExecute()
	return err
}

func (op *Op) GetCreateTime() time.Time {
	return op.CreateTime
}

func (op *Op) GetStartTime() time.Time {
	return op.StartTime
}

func (op *Op) GetEndTime() time.Time {
	return op.EndTime
}

func (op *Op) GetExecutTime() int64 {
	return op.EndTime.Sub(op.StartTime).Microseconds()
}
