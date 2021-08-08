package ops

import (
	"errors"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iworker "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"time"
	// log "github.com/sirupsen/logrus"
)

func NewOp(impl iops.IOpInternal, w iworker.IOpWorker) *Op {
	op := &Op{
		Impl:       impl,
		Worker:     w,
		CreateTime: time.Now(),
	}
	return op
}

func (op *Op) Push() error {
	r := op.Worker.SendOp(op)
	if !r {
		return errors.New("send op error!")
	}
	return nil
}

func (op *Op) GetError() error {
	return op.Err
}

func (op *Op) SetError(err error) {
	op.EndTime = time.Now()
	op.Err = err
	if op.ErrorC != nil {
		op.ErrorC <- err
		return
	} else if op.DoneCB != nil {
		op.DoneCB(op)
		return
	}
	panic("logic error")
}

func (op *Op) Waitable() bool {
	return op.DoneCB == nil
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
