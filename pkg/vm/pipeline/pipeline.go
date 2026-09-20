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

package pipeline

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(tableID uint64, attrs []string, op vm.Operator) *Pipeline {
	return &Pipeline{
		rootOp:  op,
		attrs:   attrs,
		tableID: tableID,
	}
}

func NewMerge(op vm.Operator) *Pipeline {
	return &Pipeline{
		rootOp: op,
	}
}

func (p *Pipeline) String() string {
	var buf bytes.Buffer

	vm.String(p.rootOp, &buf)
	return buf.String()
}

func (p *Pipeline) RunWithReader(r engine.Reader, topValueMsgTag int32, proc *process.Process) (end bool, err error) {

	if tableScanOperator, ok := vm.GetLeafOp(p.rootOp).(*table_scan.TableScan); ok {
		tableScanOperator.Reader = r
		tableScanOperator.TopValueMsgTag = topValueMsgTag
		tableScanOperator.Attrs = p.attrs
		tableScanOperator.TableID = p.tableID
	}

	return p.Run(proc)
}

func (p *Pipeline) Run(proc *process.Process) (end bool, err error) {
	defer catchPanic(proc.Ctx, &err)

	continuation, err := p.NewContinuation(proc)
	if err != nil {
		return false, err
	}
	for !continuation.Done() {
		var step StepResult
		step, err = continuation.Step()
		if err != nil {
			return true, err
		}
		if step.Status != StepWaiting {
			continue
		}
		if step.OnReady == nil {
			return true, moerr.NewInternalErrorNoCtx(
				"pipeline continuation returned StepWaiting without readiness registration")
		}
		ready := make(chan struct{}, 1)
		if err = step.OnReady(func() {
			select {
			case ready <- struct{}{}:
			default:
			}
		}); err != nil {
			return true, err
		}
		select {
		case <-ready:
		case <-proc.Ctx.Done():
			return true, proc.Ctx.Err()
		}
	}

	return true, nil
}

// StepStatus is the scheduler-visible state of a pipeline continuation.
// Ready means that the continuation may be stepped again. Waiting is reserved
// for operators that will publish an external readiness event once their
// non-blocking wait contract is implemented. The current VM operators return
// Ready while preserving the historical blocking behavior inside Call.
type StepStatus uint8

const (
	StepReady StepStatus = iota
	StepWaiting
	StepDone
)

type StepResult struct {
	Status StepStatus
	Result vm.CallResult
	// OnReady registers a callback for a continuation that yielded on an
	// external event. It must not block and must invoke the callback at most
	// once. A nil callback is valid only for StepReady and StepDone.
	OnReady func(func()) error
}

// Continuation owns one Pipeline execution generation. Prepare and output
// metadata setup happen once; each Step executes at most one vm.Exec quantum.
// Run above is intentionally a compatibility adapter around this API so the
// scheduler can migrate scope execution incrementally without changing
// existing pipeline callers.
type Continuation struct {
	p        *Pipeline
	proc     *process.Process
	prepared bool
	done     bool
}

func (p *Pipeline) NewContinuation(proc *process.Process) (*Continuation, error) {
	if p == nil || p.rootOp == nil {
		return &Continuation{p: p, proc: proc, done: true}, nil
	}
	if proc == nil {
		return nil, moerr.NewInternalErrorNoCtx("nil process for pipeline continuation")
	}
	return &Continuation{p: p, proc: proc}, nil
}

func (c *Continuation) Done() bool {
	return c == nil || c.done
}

func (c *Continuation) Step() (result StepResult, err error) {
	if c == nil || c.done {
		return StepResult{Status: StepDone}, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = moerr.ConvertPanicError(c.proc.Ctx, recovered)
			logutil.Errorf("panic in pipeline continuation: %v", err)
			c.done = true
		}
		if err != nil {
			c.done = true
		}
	}()

	if !c.prepared {
		if err = vm.Prepare(c.p.rootOp, c.proc); err != nil {
			c.done = true
			return StepResult{}, err
		}
		vm.ModifyOutputOpNodeIdx(c.p.rootOp, c.proc)
		c.prepared = true
	}

	callResult, callErr := vm.Exec(c.p.rootOp, c.proc)
	if callErr != nil {
		c.done = true
		return StepResult{Status: StepDone, Result: callResult}, callErr
	}
	result.Result = callResult
	if callResult.Status == vm.ExecWaiting {
		if callResult.OnReady == nil {
			c.done = true
			return StepResult{Status: StepDone, Result: callResult}, moerr.NewInternalErrorNoCtx(
				"pipeline operator returned ExecWaiting without readiness registration")
		}
		result.Status = StepWaiting
		result.OnReady = callResult.OnReady
		return result, nil
	}
	if callResult.Status == vm.ExecStop || callResult.Batch == nil {
		c.done = true
		result.Status = StepDone
		return result, nil
	}
	result.Status = StepReady
	return result, nil
}

func catchPanic(ctx context.Context, errPtr *error) {
	if e := recover(); e != nil {
		*errPtr = moerr.ConvertPanicError(ctx, e)
		logutil.Errorf("panic in pipeline: %v", *errPtr)
	}
}
