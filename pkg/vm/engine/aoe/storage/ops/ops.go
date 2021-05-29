package ops

import (
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iworker "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	// log "github.com/sirupsen/logrus"
)

func NewOp(impl iops.IOpInternal, w iworker.IOpWorker) *Op {
	op := &Op{
		Impl:   impl,
		Worker: w,
		// ErrorC: make(chan error, 1000),
	}
	return op
}

func (op *Op) Push() {
	op.Worker.SendOp(op)
}

func (op *Op) SetError(err error) {
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
