// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	vmpipeline "github.com/matrixorigin/matrixone/pkg/vm/pipeline"
)

// remoteRunEventState is the client-side remote pipeline state machine. A
// MORPC receive is one external event; after a batch arrives, connector
// forwarding or the retained dispatch receiver runs as ready work. No
// transport loop owns a VM execution loop and no ready worker waits for
// network, edge capacity, or downstream dispatch capacity.
type remoteRunEventState struct {
	s          *Scope
	c          *Compile
	scheduler  *scopeTaskScheduler
	sender     *messageSenderOnClient
	withoutOut bool
	done       func(error)

	mu         sync.Mutex
	finished   bool
	finishOnce sync.Once

	connector *connector.Connector
	pending   *batch.Batch

	fake        *value_scan.ValueScan
	runner      *dispatch.Dispatch
	continuity  *vmpipeline.Continuation
	dispatchBat *batch.Batch
}

func newRemoteRunEventState(
	s *Scope,
	c *Compile,
	scheduler *scopeTaskScheduler,
	sender *messageSenderOnClient,
	withoutOutput bool,
	done func(error),
) *remoteRunEventState {
	state := &remoteRunEventState{
		s:          s,
		c:          c,
		scheduler:  scheduler,
		sender:     sender,
		withoutOut: withoutOutput,
		done:       done,
	}
	if s != nil {
		state.connector, _ = s.RootOp.(*connector.Connector)
	}
	return state
}

func (r *remoteRunEventState) start() error {
	return r.receiveNext()
}

func (r *remoteRunEventState) isFinished() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.finished
}

// receiveNext schedules exactly one blocking stream receive. The event source
// only decodes MORPC messages and publishes a short ready task; VM work stays
// on the query scheduler's ready queue.
func (r *remoteRunEventState) receiveNext() error {
	if r.isFinished() {
		return nil
	}
	err := r.scheduler.submitEventSource("remote-receive", func() {
		bat, end, receiveErr := r.sender.receiveBatch()
		if err := r.scheduler.submitRoot("remote-receive-ready", func() {
			r.handleReceived(bat, end, receiveErr)
		}); err != nil {
			if bat != nil {
				bat.Clean(r.s.Proc.Mp())
				_ = r.sender.acknowledgeRemoteBatch()
			}
			r.finish(err)
		}
	})
	if err != nil {
		r.finish(err)
	}
	return err
}

func (r *remoteRunEventState) handleReceived(bat *batch.Batch, end bool, receiveErr error) {
	if r.isFinished() {
		if bat != nil {
			bat.Clean(r.s.Proc.Mp())
			_ = r.sender.acknowledgeRemoteBatch()
		}
		return
	}
	if receiveErr != nil {
		if bat != nil {
			bat.Clean(r.s.Proc.Mp())
			_ = r.sender.acknowledgeRemoteBatch()
		}
		r.finish(receiveErr)
		return
	}
	if end || bat == nil {
		r.finish(nil)
		return
	}

	if r.withoutOut {
		bat.Clean(r.s.Proc.Mp())
		if err := r.sender.acknowledgeRemoteBatch(); err != nil {
			r.finish(err)
			return
		}
		if err := r.receiveNext(); err != nil {
			r.finish(err)
		}
		return
	}

	if r.connector != nil {
		r.pending = bat
		r.forwardConnectorBatch()
		return
	}
	if _, ok := r.s.RootOp.(*dispatch.Dispatch); ok {
		r.dispatchBat = bat
		r.runDispatchBatch()
		return
	}
	bat.Clean(r.s.Proc.Mp())
	_ = r.sender.acknowledgeRemoteBatch()
	r.finish(moerr.NewInternalErrorNoCtxf(
		"remote run pipeline has an unexpected operator [id = %d] at last", r.s.RootOp.OpType()))
}

func (r *remoteRunEventState) forwardConnectorBatch() {
	bat := r.pending
	if bat == nil {
		return
	}
	nextReg := r.connector.Reg
	if nextReg == nil {
		bat.Clean(r.s.Proc.Mp())
		_ = r.sender.acknowledgeRemoteBatch()
		r.pending = nil
		r.finish(moerr.NewInternalErrorNoCtx("remote batch forward target is nil"))
		return
	}
	if nextReg.TrySendDataDirect(bat, r.s.Proc.Mp()) {
		r.pending = nil
		if err := r.sender.acknowledgeRemoteBatch(); err != nil {
			r.finish(err)
			return
		}
		if err := r.receiveNext(); err != nil {
			r.finish(err)
		}
		return
	}
	if nextReg.Done() != nil {
		select {
		case <-nextReg.Done():
			bat.Clean(r.s.Proc.Mp())
			r.pending = nil
			if err := r.sender.acknowledgeRemoteBatch(); err != nil {
				r.finish(err)
				return
			}
			r.finish(nil)
			return
		default:
		}
	}
	if err := r.registerReady("remote-connector-capacity", nextReg.RegisterCapacityReady, r.forwardConnectorBatch); err != nil {
		bat.Clean(r.s.Proc.Mp())
		r.pending = nil
		_ = r.sender.acknowledgeRemoteBatch()
		r.finish(err)
	}
}

func (r *remoteRunEventState) initDispatch() error {
	if r.continuity != nil {
		return nil
	}
	arg, ok := r.s.RootOp.(*dispatch.Dispatch)
	if !ok {
		return moerr.NewInternalErrorNoCtx("remote dispatch receiver has no dispatch root")
	}
	fake := value_scan.NewArgument()
	runner := buildRemoteDispatchReceiverRoot(arg, fake)
	runner.AdoptCleanupState(arg)
	continuation, err := vmpipeline.New(0, nil, runner).NewContinuation(r.s.Proc)
	if err != nil {
		runner.Release()
		fake.Free(r.s.Proc, true, err)
		fake.Release()
		return err
	}
	r.fake = fake
	r.runner = runner
	r.continuity = continuation
	return nil
}

func (r *remoteRunEventState) runDispatchBatch() {
	if err := r.initDispatch(); err != nil {
		if r.dispatchBat != nil {
			r.dispatchBat.Clean(r.s.Proc.Mp())
			r.dispatchBat = nil
			_ = r.sender.acknowledgeRemoteBatch()
		}
		r.finish(err)
		return
	}
	bat := r.dispatchBat
	if bat == nil {
		return
	}
	if analyzer := r.runner.GetOperatorBase().OpAnalyzer; analyzer != nil {
		analyzer.Network(bat)
	}
	r.fake.Batchs = append(r.fake.Batchs, bat)
	step, err := r.continuity.Step()
	if err != nil {
		bat.Clean(r.s.Proc.Mp())
		r.fake.Batchs[len(r.fake.Batchs)-1] = nil
		r.dispatchBat = nil
		_ = r.sender.acknowledgeRemoteBatch()
		r.finish(err)
		return
	}
	switch step.Status {
	case vmpipeline.StepWaiting:
		if step.OnReady == nil {
			bat.Clean(r.s.Proc.Mp())
			r.fake.Batchs[len(r.fake.Batchs)-1] = nil
			r.dispatchBat = nil
			_ = r.sender.acknowledgeRemoteBatch()
			r.finish(moerr.NewInternalErrorNoCtx(
				"remote dispatch continuation waited without readiness"))
			return
		}
		if err := r.registerReady("remote-dispatch-ready", step.OnReady, r.runDispatchBatch); err != nil {
			bat.Clean(r.s.Proc.Mp())
			r.fake.Batchs[len(r.fake.Batchs)-1] = nil
			r.dispatchBat = nil
			_ = r.sender.acknowledgeRemoteBatch()
			r.finish(err)
		}
	case vmpipeline.StepReady, vmpipeline.StepDone:
		bat.Clean(r.s.Proc.Mp())
		r.fake.Batchs[len(r.fake.Batchs)-1] = nil
		r.dispatchBat = nil
		if err := r.sender.acknowledgeRemoteBatch(); err != nil {
			r.finish(err)
			return
		}
		if step.Status == vmpipeline.StepDone {
			r.finish(nil)
			return
		}
		if err := r.receiveNext(); err != nil {
			r.finish(err)
		}
	default:
		r.finish(moerr.NewInternalErrorNoCtx("remote dispatch continuation returned unknown status"))
	}
}

// registerReady bridges an operator/edge readiness callback to one ready task.
// Context cancellation is the terminal readiness event for every remote
// continuation, preventing a canceled query from leaving a retained batch or
// stream ACK outstanding.
func (r *remoteRunEventState) registerReady(
	name string,
	register func(func()) error,
	resume func(),
) error {
	var once sync.Once
	var stop func() bool
	ready := func() {
		once.Do(func() {
			if stop != nil {
				stop()
			}
			if err := r.scheduler.submitRoot(name, resume); err != nil {
				r.finish(err)
			}
		})
	}
	if ctx := r.s.Proc.Ctx; ctx != nil {
		stop = context.AfterFunc(ctx, ready)
	}
	err := register(ready)
	if err != nil && stop != nil {
		stop()
	}
	return err
}

func (r *remoteRunEventState) finish(err error) {
	r.finishOnce.Do(func() {
		r.mu.Lock()
		r.finished = true
		r.mu.Unlock()
		if submitErr := r.scheduler.submitTeardown("remote-cleanup", func() {
			r.cleanup(err)
		}); submitErr != nil {
			r.cleanup(submitErr)
		}
	})
}

func (r *remoteRunEventState) cleanup(err error) {
	if r.pending != nil {
		r.pending.Clean(r.s.Proc.Mp())
		r.pending = nil
		_ = r.sender.acknowledgeRemoteBatch()
	}
	if r.dispatchBat != nil {
		// A dispatch continuation may still be waiting on downstream capacity.
		// Its fake value scan owns the batch while the continuation is live; take
		// it out before Free so the ACK/cleanup path has one clear owner.
		if r.fake != nil && len(r.fake.Batchs) > 0 &&
			r.fake.Batchs[len(r.fake.Batchs)-1] == r.dispatchBat {
			r.fake.Batchs[len(r.fake.Batchs)-1] = nil
		}
		r.dispatchBat.Clean(r.s.Proc.Mp())
		r.dispatchBat = nil
		_ = r.sender.acknowledgeRemoteBatch()
	}
	if r.runner != nil {
		if arg, ok := r.s.RootOp.(*dispatch.Dispatch); ok {
			arg.AdoptCleanupState(r.runner)
		}
		r.runner.Release()
		r.runner = nil
	}
	if r.fake != nil {
		r.fake.Free(r.s.Proc, err != nil, err)
		r.fake.Batchs = nil
		r.fake.Release()
		r.fake = nil
	}

	queryCtx := scopeRunQueryContext(r.s.Proc)
	terminalErr := error(nil)
	if r.sender != nil && isScopeCancellationError(err) {
		terminalErr = r.sender.waitingTheStopResponse()
	}
	runErr, _ := normalizeScopeRunError(err, r.s.Proc.Ctx, queryCtx)
	if runErr == nil && terminalErr != nil {
		runErr, _ = normalizeScopeRunError(terminalErr, r.s.Proc.Ctx, queryCtx)
	}
	p := vmpipeline.New(0, nil, r.s.RootOp)
	p.CleanRootOperator(r.s.Proc, runErr != nil, r.c.isPrepare, runErr)
	if runErr != nil && r.s.Proc.Cancel != nil {
		r.s.Proc.Cancel(runErr)
	}
	if r.sender != nil {
		if runErr == nil {
			r.sender.prepareForLocalCleanup()
		}
		r.sender.close()
	}
	if r.done != nil {
		r.done(runErr)
	}
}
