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

// BindReader installs scan metadata before a continuation is admitted. Reader
// binding is part of pipeline construction, not execution, so Step never
// performs reader setup itself.
func (p *Pipeline) BindReader(r engine.Reader, topValueMsgTag int32) {
	if p == nil || r == nil {
		return
	}
	if tableScanOperator, ok := vm.GetLeafOp(p.rootOp).(*table_scan.TableScan); ok {
		tableScanOperator.Reader = r
		tableScanOperator.TopValueMsgTag = topValueMsgTag
		tableScanOperator.Attrs = p.attrs
		tableScanOperator.TableID = p.tableID
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
// Ready means that the continuation may be stepped again. Waiting means the
// operator has registered a concrete edge, spool, message, or cancellation
// event and must not be called again until that event fires.
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
// Run above is a synchronous boundary for callers outside query execution; the
// production Scope scheduler drives this state machine directly.
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

// NewPreparedContinuation creates a continuation for an operator subtree that
// was already prepared by its owning parent pipeline. Operators such as the
// local Shuffle producer execute a child subtree on a separate process after
// the parent VM preparation walk; preparing that subtree a second time would
// duplicate holder/resource admission.
func (p *Pipeline) NewPreparedContinuation(proc *process.Process) (*Continuation, error) {
	continuation, err := p.NewContinuation(proc)
	if err != nil {
		return nil, err
	}
	continuation.prepared = true
	return continuation, nil
}

func (c *Continuation) Done() bool {
	return c == nil || c.done
}

// Context exposes the pipeline cancellation boundary to the query scheduler.
// A continuation may be waiting on a data edge when its owning scope is
// canceled by an early consumer; cancellation is itself a readiness event and
// must re-admit the continuation so it can observe the terminal cause.
func (c *Continuation) Context() context.Context {
	if c == nil || c.proc == nil {
		return nil
	}
	return c.proc.Ctx
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
		if yielded, ok := vm.AsYieldError(callErr); ok {
			// A parent operator may have called ChildrenCall and propagated the
			// child's readiness through its error return. Preserve the continuation
			// as live: the dependency is external readiness, not execution failure.
			result.Result = callResult
			result.Status = StepWaiting
			result.OnReady = yielded.OnReady
			return result, nil
		}
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
	if callResult.Status == vm.ExecStop {
		c.done = true
		result.Status = StepDone
		return result, nil
	}
	// ExecHasMore is an internal continuation quantum.  Operators use it when
	// they advanced their state without producing a downstream batch (for
	// example, an adaptive branch transition).  It must not terminate the
	// pipeline merely because Batch is nil.
	if callResult.Batch == nil && callResult.Status != vm.ExecHasMore {
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
