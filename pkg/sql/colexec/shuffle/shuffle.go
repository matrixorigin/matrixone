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

package shuffle

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	moerr "github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "shuffle"

func (shuffle *Shuffle) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
}

func (shuffle *Shuffle) OpType() vm.OpType {
	return vm.Shuffle
}

// PrepareChildrenProcess creates the process that will execute the local
// Shuffle's child tree. It must exist before vm.Prepare descends into that
// tree, because children may retain the process context in Prepare.
func (shuffle *Shuffle) PrepareChildrenProcess(proc *process.Process) *process.Process {
	if shuffle.DrainAllBuckets || shuffle.GetShufflePool() == nil {
		return proc
	}
	if shuffle.ctr.producerProc == nil {
		parent, _ := process.GetQueryCtxFromProc(proc)
		if parent == nil {
			parent = proc.Ctx
		}
		producerProc := proc.NewNoContextChildProc(0)
		producerProc.Reg = proc.Reg
		producerProc.Session = proc.Session
		producerProc.BuildPipelineContext(parent)
		shuffle.ctr.producerProc = producerProc
	}
	return shuffle.ctr.producerProc
}

func (shuffle *Shuffle) Prepare(proc *process.Process) error {
	if shuffle.OpAnalyzer == nil {
		shuffle.OpAnalyzer = process.NewAnalyzer(shuffle.GetIdx(), shuffle.IsFirst, shuffle.IsLast, opName)
	} else {
		shuffle.OpAnalyzer.Reset()
	}

	if shuffle.ctr.sels == nil {
		shuffle.ctr.sels = make([][]int32, shuffle.BucketNum)
	}
	if shuffle.GetShufflePool() == nil {
		shuffle.SetShufflePool(NewShufflePool(shuffle.BucketNum, 1, true))
		shuffle.DrainAllBuckets = true
	}

	if shuffle.ShuffleExpr != nil && shuffle.ctr.exprExec == nil {
		var err error
		shuffle.ctr.exprExec, err = colexec.NewExpressionExecutor(proc, shuffle.ShuffleExpr)
		if err != nil {
			return err
		}
	}
	if !shuffle.ctr.shufflePool.hold() {
		// Preserve cancellation and the primary execution failure for the
		// scope collector; a generic admission error would mask their cause.
		if err := shuffle.ctr.shufflePool.terminalError(); err != nil {
			return err
		}
		return moerr.NewInternalError(proc.Ctx, "shuffle pool was aborted before prepare completed")
	}
	shuffle.ctr.held = true
	shuffle.ctr.ending = false
	shuffle.ctr.runtimeFilterHandled = false
	shuffle.ctr.stableStringHash = proc.UsesCompleteStringShuffleHash()
	if !shuffle.DrainAllBuckets {
		shuffle.ctr.producerDone = make(chan struct{})
		shuffle.ctr.producerFinishOnce = sync.Once{}
		// Keep the handoff buffered so the producer can publish a batch and
		// signal readiness without making the scheduler worker part of the
		// producer/consumer rendezvous. The acknowledgement still controls
		// batch ownership and backpressure.
		shuffle.ctr.directBatches = make(chan directHandoff, 1)
		shuffle.ctr.directReady = make(chan struct{}, 1)
		shuffle.ctr.directSpace = make(chan struct{}, 1)
		shuffle.ctr.consumerDone = make(chan struct{})
	}
	return nil
}

func (shuffle *Shuffle) Call(proc *process.Process) (vm.CallResult, error) {
	if !shuffle.DrainAllBuckets {
		return shuffle.callLocal(proc)
	}
	return shuffle.callSynchronous(proc)
}

func (shuffle *Shuffle) callSynchronous(proc *process.Process) (vm.CallResult, error) {
	analyzer := shuffle.OpAnalyzer

	// Put old buf back to pool after cleaning data
	if shuffle.ctr.buf != nil {
		shuffle.ctr.buf.CleanOnlyData()
		shuffle.ctr.shufflePool.putBatchToPool(shuffle.ctr.buf, proc.Mp())
		shuffle.ctr.buf = nil
	}

	getFull := shuffle.ctr.shufflePool.getAnyFullBatch
	getLast := shuffle.ctr.shufflePool.getAnyLastBatch
	for {
		result := vm.NewCallResult()
		tmpBat := getFull()
		if tmpBat != nil { // find a full batch
			shuffle.ctr.buf = tmpBat
			if err := shuffle.handleRuntimeFilter(proc); err != nil {
				return vm.CancelResult, err
			}
			result.Batch = tmpBat
			return result, nil
		}

		if shuffle.ctr.pendingBat != nil {
			done, spaceWaiter, err := shuffle.flushPending(proc)
			if err != nil {
				return vm.CancelResult, err
			}
			if done {
				continue
			}

			if tmpBat = getFull(); tmpBat != nil {
				shuffle.ctr.buf = tmpBat
				if err = shuffle.handleRuntimeFilter(proc); err != nil {
					return vm.CancelResult, err
				}
				result.Batch = tmpBat
				return result, nil
			}
			if !proc.HasEventSubmitter() {
				waitForShuffleSpace(spaceWaiter, shuffle.ctr.shufflePool.endingWaiter, proc)
				continue
			}
			return vm.CallResult{
				Status: vm.ExecWaiting,
				OnReady: shuffle.registerReady(proc, "shuffle-space", func() {
					waitForShuffleSpace(spaceWaiter, shuffle.ctr.shufflePool.endingWaiter, proc)
				}),
			}, nil
		}

		if shuffle.ctr.shufflePool.allStop() {
			shuffle.ctr.ending = true
			tmpBat = getLast()
			if tmpBat != nil {
				shuffle.ctr.buf = tmpBat
				if err := shuffle.handleRuntimeFilter(proc); err != nil {
					return vm.CancelResult, err
				}
				result.Batch = tmpBat
				return result, nil
			}
			return vm.CancelResult, nil
		}

		if shuffle.ctr.ending {
			if !proc.HasEventSubmitter() {
				shuffle.ctr.shufflePool.waitAnyBatchOrEnd(proc)
				result.Batch = batch.EmptyBatch
				return result, nil
			}
			return vm.CallResult{
				Status: vm.ExecWaiting,
				OnReady: shuffle.registerReady(proc, "shuffle-ending", func() {
					shuffle.ctr.shufflePool.waitAnyBatchOrEnd(proc)
				}),
			}, nil
		}

		// do input
		result, err := vm.ChildrenCall(shuffle.GetChildren(0), proc, analyzer)
		if err != nil {
			return result, err
		}
		// Child completion is not shuffle completion: buffered batches may remain.
		result.Status = vm.ExecNext
		bat := result.Batch
		if bat == nil {
			shuffle.ctr.ending = true
			shuffle.stopWritingOnce()
			result.Status = vm.ExecNext
			result.Batch = batch.EmptyBatch
			return result, nil
		} else if bat.Last() {
			return result, nil
		} else if !bat.IsEmpty() {
			if shuffle.ctr.exprExec != nil {
				bat, err = shuffle.evalAndShuffle(bat, proc)
			} else if shuffle.ShuffleType == int32(plan.ShuffleType_Hash) {
				bat, err = hashShuffle(shuffle, bat, proc)
			} else if shuffle.ShuffleType == int32(plan.ShuffleType_Range) {
				bat, err = rangeShuffle(shuffle, bat, proc)
			}
			if err != nil {
				return result, err
			}
			if bat != nil {
				// can directly send this batch
				if err = shuffle.handleRuntimeFilter(proc); err != nil {
					return vm.CancelResult, err
				}
				return result, nil
			}
		}
	}
}

// callLocal is the consumer half of the fixed-bucket local exchange. Its
// producer runs independently, so bounded pool writes cannot prevent this
// goroutine from draining its bucket, including when local shuffles nest.
func (shuffle *Shuffle) callLocal(proc *process.Process) (vm.CallResult, error) {
	shuffle.releasePreviousLocalBatch(proc)
	shuffle.startLocalProducer(proc)

	for {
		if err := proc.Ctx.Err(); err != nil {
			return vm.CancelResult, err
		}
		select {
		case handoff := <-shuffle.ctr.directBatches:
			shuffle.ctr.directAck = handoff.ack
			return shuffle.returnLocalBatch(proc, handoff.bat, false)
		default:
		}
		if bat := shuffle.ctr.shufflePool.getFullBatch(shuffle.CurrentShuffleIdx); bat != nil {
			return shuffle.returnLocalBatch(proc, bat, true)
		}

		if shuffle.ctr.shufflePool.allStop() {
			if err := shuffle.ctr.shufflePool.terminalError(); err != nil {
				return vm.CancelResult, err
			}
			if bat := shuffle.ctr.shufflePool.getLastBatch(shuffle.CurrentShuffleIdx); bat != nil {
				return shuffle.returnLocalBatch(proc, bat, true)
			}
			return vm.CancelResult, nil
		}

		if !proc.HasEventSubmitter() {
			waitForShuffleBucket(
				shuffle.ctr.directReady,
				shuffle.ctr.shufflePool.batchWaiters[shuffle.CurrentShuffleIdx],
				shuffle.ctr.shufflePool.endingWaiters[shuffle.CurrentShuffleIdx],
				shuffle.ctr.consumerDone,
				proc,
			)
			continue
		}
		return vm.CallResult{
			Status: vm.ExecWaiting,
			OnReady: shuffle.registerReady(proc, "shuffle-bucket", func() {
				waitForShuffleBucket(
					shuffle.ctr.directReady,
					shuffle.ctr.shufflePool.batchWaiters[shuffle.CurrentShuffleIdx],
					shuffle.ctr.shufflePool.endingWaiters[shuffle.CurrentShuffleIdx],
					shuffle.ctr.consumerDone,
					proc,
				)
			}),
		}, nil
	}
}

// registerReady moves channel waits out of the scheduler's ready worker. The
// wait itself is an external pool/hand-off event; the callback only re-admits
// this operator's continuation after the event has fired.
func (shuffle *Shuffle) registerReady(proc *process.Process, name string, wait func()) func(func()) error {
	return func(ready func()) error {
		if ready == nil {
			return moerr.NewInvalidInputNoCtx("nil shuffle readiness callback")
		}
		return proc.SubmitBlockingEvent(name, func() {
			wait()
			ready()
		})
	}
}

func waitForShuffleSpace(space, ending <-chan struct{}, proc *process.Process) {
	select {
	case <-space:
	case <-ending:
	case <-proc.Ctx.Done():
	}
}

func waitForShuffleBucket(
	direct <-chan struct{},
	batchReady, ending <-chan bool,
	consumerDone <-chan struct{},
	proc *process.Process,
) {
	select {
	case <-direct:
	case <-batchReady:
	case <-ending:
	case <-consumerDone:
	case <-proc.Ctx.Done():
	}
}

func (shuffle *Shuffle) returnLocalBatch(proc *process.Process, bat *batch.Batch, fromPool bool) (vm.CallResult, error) {
	shuffle.ctr.buf = bat
	shuffle.ctr.bufFromPool = fromPool
	if err := shuffle.handleRuntimeFilter(proc); err != nil {
		if !fromPool {
			shuffle.ackDirectBatch()
			shuffle.ctr.buf = nil
		}
		return vm.CancelResult, err
	}
	result := vm.NewCallResult()
	result.Batch = bat
	return result, nil
}

func (shuffle *Shuffle) releasePreviousLocalBatch(proc *process.Process) {
	shuffle.ackDirectBatch()
	if shuffle.ctr.buf == nil {
		return
	}
	if shuffle.ctr.bufFromPool {
		shuffle.ctr.buf.CleanOnlyData()
		shuffle.ctr.shufflePool.putBatchToPool(shuffle.ctr.buf, proc.Mp())
	}
	shuffle.ctr.buf = nil
	shuffle.ctr.bufFromPool = false
}

func (shuffle *Shuffle) ackDirectBatch() {
	if shuffle.ctr.directAck != nil {
		close(shuffle.ctr.directAck)
		shuffle.ctr.directAck = nil
		if shuffle.ctr.directSpace != nil {
			select {
			case shuffle.ctr.directSpace <- struct{}{}:
			default:
			}
		}
	}
	if shuffle.ctr.directReady != nil {
		select {
		case <-shuffle.ctr.directReady:
		default:
		}
	}
}

func (shuffle *Shuffle) startLocalProducer(proc *process.Process) {
	shuffle.ctr.producerOnce.Do(func() {
		producerProc := shuffle.PrepareChildrenProcess(proc)
		shuffle.ctr.producerStarted = true
		shuffle.ctr.shufflePool.registerProducer(shuffle.CurrentShuffleIdx, producerProc.Cancel)
		if proc.HasReadySubmitter() {
			producerPipeline := pipeline.New(0, nil, shuffle.GetChildren(0))
			continuation, err := producerPipeline.NewPreparedContinuation(producerProc)
			if err != nil {
				shuffle.failLocalProducer(producerProc, err)
				shuffle.finishLocalProducer()
				return
			}
			shuffle.ctr.producerCont = continuation
			if err := shuffle.scheduleProducer(producerProc); err != nil {
				shuffle.failLocalProducer(producerProc, err)
				shuffle.finishLocalProducer()
			}
			return
		}
		// Direct operator callers do not have a query scheduler. Keep the
		// synchronous API boundary usable for unit tests and embedded helpers;
		// production Scope execution always takes the ready-continuation path.
		go shuffle.runLocalProducer(producerProc)
	})
}

// scheduleProducer admits exactly one non-blocking local-shuffle producer
// quantum to the query ready queue. It is intentionally not an event source:
// the producer continuation only performs VM work and bounded in-memory
// transformations once its input/capacity event has fired.
func (shuffle *Shuffle) scheduleProducer(proc *process.Process) error {
	return proc.SubmitReady("shuffle-producer", func() {
		shuffle.runLocalProducerStep(proc)
	})
}

func (shuffle *Shuffle) finishLocalProducer() {
	if shuffle.ctr.held && !shuffle.ctr.writingStopped {
		shuffle.stopWritingOnce()
	}
	if shuffle.ctr.producerDone != nil {
		shuffle.ctr.producerFinishOnce.Do(func() {
			close(shuffle.ctr.producerDone)
		})
	}
}

// registerProducerEvent waits for a pool/direct-handoff event off the ready
// queue and re-admits the producer continuation when it fires. The event
// source is allowed to block on the channel; it never executes VM code.
func (shuffle *Shuffle) registerProducerEvent(
	proc *process.Process,
	name string,
	wait func(),
) error {
	var once sync.Once
	var stop func() bool
	ready := func() {
		once.Do(func() {
			if stop != nil {
				stop()
			}
			if err := shuffle.scheduleProducer(proc); err != nil {
				shuffle.failLocalProducer(proc, err)
				shuffle.finishLocalProducer()
			}
		})
	}
	if proc.Ctx != nil {
		stop = context.AfterFunc(proc.Ctx, ready)
	}
	err := proc.SubmitBlockingEvent(name, func() {
		wait()
		ready()
	})
	if err != nil && stop != nil {
		stop()
	}
	return err
}

func (shuffle *Shuffle) registerProducerContinuationReady(
	proc *process.Process,
	register func(func()) error,
) error {
	var once sync.Once
	var stop func() bool
	ready := func() {
		once.Do(func() {
			if stop != nil {
				stop()
			}
			if err := shuffle.scheduleProducer(proc); err != nil {
				shuffle.failLocalProducer(proc, err)
				shuffle.finishLocalProducer()
			}
		})
	}
	if proc.Ctx != nil {
		stop = context.AfterFunc(proc.Ctx, ready)
	}
	if err := register(ready); err != nil {
		if stop != nil {
			stop()
		}
		return err
	}
	return nil
}

// runLocalProducerStep is the event-driven producer state machine for a
// fixed-bucket local exchange. Every invocation performs at most one child VM
// quantum and never waits on pool capacity or consumer acknowledgement.
func (shuffle *Shuffle) runLocalProducerStep(proc *process.Process) {
	if err := proc.Ctx.Err(); err != nil {
		shuffle.finishLocalProducer()
		return
	}
	if shuffle.ctr.producerPending != nil {
		pending := shuffle.ctr.producerPending
		select {
		case <-pending.ack:
			shuffle.ctr.producerPending = nil
			if err := shuffle.scheduleProducer(proc); err != nil {
				shuffle.failLocalProducer(proc, err)
				shuffle.finishLocalProducer()
			}
			return
		case <-shuffle.ctr.consumerDone:
			shuffle.ctr.producerPending = nil
			shuffle.finishLocalProducer()
			return
		case <-shuffle.ctr.shufflePool.endingWaiter:
			shuffle.ctr.producerPending = nil
			shuffle.finishLocalProducer()
			return
		case <-proc.Ctx.Done():
			shuffle.finishLocalProducer()
			return
		default:
			if err := shuffle.registerProducerEvent(proc, "shuffle-direct-ack", func() {
				select {
				case <-pending.ack:
				case <-shuffle.ctr.consumerDone:
				case <-shuffle.ctr.shufflePool.endingWaiter:
				case <-proc.Ctx.Done():
				}
			}); err != nil {
				shuffle.failLocalProducer(proc, err)
				shuffle.finishLocalProducer()
			}
			return
		}
	}

	if shuffle.ctr.pendingBat != nil {
		done, waiter, err := shuffle.flushPending(proc)
		if err != nil {
			shuffle.failLocalProducer(proc, err)
			shuffle.finishLocalProducer()
			return
		}
		if !done {
			if err := shuffle.registerProducerEvent(proc, "shuffle-producer-space", func() {
				select {
				case <-waiter:
				case <-shuffle.ctr.shufflePool.endingWaiter:
				case <-proc.Ctx.Done():
				}
			}); err != nil {
				shuffle.failLocalProducer(proc, err)
				shuffle.finishLocalProducer()
			}
			return
		}
	}

	step, err := shuffle.ctr.producerCont.Step()
	if err != nil {
		shuffle.failLocalProducer(proc, err)
		shuffle.finishLocalProducer()
		return
	}
	switch step.Status {
	case pipeline.StepWaiting:
		if step.OnReady == nil {
			shuffle.failLocalProducer(proc, moerr.NewInternalErrorNoCtx(
				"shuffle producer continuation waited without readiness"))
			shuffle.finishLocalProducer()
			return
		}
		if err := shuffle.registerProducerContinuationReady(proc, step.OnReady); err != nil {
			shuffle.failLocalProducer(proc, err)
			shuffle.finishLocalProducer()
		}
		return
	case pipeline.StepDone:
		shuffle.finishLocalProducer()
		return
	case pipeline.StepReady:
		// ExecHasMore can advance a child without producing a batch. Re-admit
		// another quantum immediately; normal child output is handled below.
		bat := step.Result.Batch
		if bat == nil {
			if err := shuffle.scheduleProducer(proc); err != nil {
				shuffle.failLocalProducer(proc, err)
				shuffle.finishLocalProducer()
			}
			return
		}
		shuffle.ctr.producerRows += int64(bat.RowCount())
		shuffle.ctr.producerSize += int64(bat.Size())
		if bat.Last() {
			if !shuffle.publishProducerDirect(proc, bat) {
				return
			}
			if err := shuffle.scheduleProducer(proc); err != nil {
				shuffle.failLocalProducer(proc, err)
				shuffle.finishLocalProducer()
			}
			return
		}
		if bat.IsEmpty() {
			if err := shuffle.scheduleProducer(proc); err != nil {
				shuffle.failLocalProducer(proc, err)
				shuffle.finishLocalProducer()
			}
			return
		}
		if shuffle.ctr.exprExec != nil {
			bat, err = shuffle.evalAndShuffle(bat, proc)
		} else if shuffle.ShuffleType == int32(plan.ShuffleType_Hash) {
			bat, err = hashShuffle(shuffle, bat, proc)
		} else if shuffle.ShuffleType == int32(plan.ShuffleType_Range) {
			bat, err = rangeShuffle(shuffle, bat, proc)
		}
		if err != nil {
			shuffle.failLocalProducer(proc, err)
			shuffle.finishLocalProducer()
			return
		}
		if bat != nil {
			if !shuffle.publishProducerDirect(proc, bat) {
				return
			}
		}
		if err := shuffle.scheduleProducer(proc); err != nil {
			shuffle.failLocalProducer(proc, err)
			shuffle.finishLocalProducer()
		}
	default:
		shuffle.failLocalProducer(proc, moerr.NewInternalErrorNoCtx(
			"shuffle producer returned unknown continuation status"))
		shuffle.finishLocalProducer()
	}
}

func (shuffle *Shuffle) publishProducerDirect(proc *process.Process, bat *batch.Batch) bool {
	ack := make(chan struct{})
	pending := &directHandoff{bat: bat, ack: ack}
	select {
	case shuffle.ctr.directBatches <- *pending:
		shuffle.ctr.producerPending = pending
		select {
		case shuffle.ctr.directReady <- struct{}{}:
		default:
		}
		return true
	case <-shuffle.ctr.consumerDone:
		return false
	case <-shuffle.ctr.shufflePool.endingWaiter:
		return false
	case <-proc.Ctx.Done():
		return false
	default:
		if err := shuffle.registerProducerEvent(proc, "shuffle-direct-space", func() {
			select {
			case <-shuffle.ctr.directSpace:
			case <-shuffle.ctr.consumerDone:
			case <-shuffle.ctr.shufflePool.endingWaiter:
			case <-proc.Ctx.Done():
			}
		}); err != nil {
			shuffle.failLocalProducer(proc, err)
			shuffle.finishLocalProducer()
		}
		return false
	}
}

func (shuffle *Shuffle) runLocalProducer(proc *process.Process) {
	producerPipeline := pipeline.New(0, nil, shuffle.GetChildren(0))
	continuation, err := producerPipeline.NewPreparedContinuation(proc)
	if err != nil {
		shuffle.failLocalProducer(proc, err)
		shuffle.finishLocalProducer()
		return
	}
	producerDone := shuffle.ctr.producerDone
	defer func() {
		if recovered := recover(); recovered != nil {
			err := moerr.ConvertPanicError(proc.Ctx, recovered)
			shuffle.ctr.shufflePool.abortWithError(proc.Mp(), err)
		}
		shuffle.stopWritingOnce()
		if producerDone != nil {
			close(producerDone)
		}
	}()

	for {
		if shuffle.ctr.pendingBat != nil {
			done, waiter, err := shuffle.flushPending(proc)
			if err != nil {
				shuffle.failLocalProducer(proc, err)
				return
			}
			if !done {
				select {
				case <-waiter:
				case <-shuffle.ctr.shufflePool.endingWaiter:
					return
				case <-proc.Ctx.Done():
					return
				}
				continue
			}
		}

		step, err := continuation.Step()
		if err != nil {
			if proc.Ctx.Err() == nil {
				shuffle.failLocalProducer(proc, err)
			}
			return
		}
		if step.Status == pipeline.StepWaiting {
			if step.OnReady == nil {
				shuffle.failLocalProducer(proc, moerr.NewInternalErrorNoCtx(
					"shuffle producer child returned ExecWaiting without readiness"))
				return
			}
			ready := make(chan struct{}, 1)
			if waitErr := step.OnReady(func() {
				select {
				case ready <- struct{}{}:
				default:
				}
			}); waitErr != nil {
				if proc.Ctx.Err() == nil {
					shuffle.failLocalProducer(proc, waitErr)
				}
				return
			}
			select {
			case <-ready:
				continue
			case <-proc.Ctx.Done():
				return
			}
		}
		if step.Status == pipeline.StepDone {
			return
		}
		bat := step.Result.Batch
		if bat == nil {
			return
		}
		shuffle.ctr.producerRows += int64(bat.RowCount())
		shuffle.ctr.producerSize += int64(bat.Size())
		if bat.Last() {
			if !shuffle.sendDirectBatch(proc, bat) {
				return
			}
			continue
		}
		if bat.IsEmpty() {
			continue
		}

		if shuffle.ctr.exprExec != nil {
			bat, err = shuffle.evalAndShuffle(bat, proc)
		} else if shuffle.ShuffleType == int32(plan.ShuffleType_Hash) {
			bat, err = hashShuffle(shuffle, bat, proc)
		} else if shuffle.ShuffleType == int32(plan.ShuffleType_Range) {
			bat, err = rangeShuffle(shuffle, bat, proc)
		}
		if err != nil {
			shuffle.failLocalProducer(proc, err)
			return
		}
		if bat != nil && !shuffle.sendDirectBatch(proc, bat) {
			return
		}
	}
}

func (shuffle *Shuffle) sendDirectBatch(proc *process.Process, bat *batch.Batch) bool {
	ack := make(chan struct{})
	handoff := directHandoff{bat: bat, ack: ack}
	select {
	case shuffle.ctr.directBatches <- handoff:
		select {
		case shuffle.ctr.directReady <- struct{}{}:
		default:
		}
	case <-shuffle.ctr.consumerDone:
		return true
	case <-shuffle.ctr.shufflePool.endingWaiter:
		return false
	case <-proc.Ctx.Done():
		return false
	}

	select {
	case <-ack:
		return true
	case <-shuffle.ctr.consumerDone:
		return true
	case <-shuffle.ctr.shufflePool.endingWaiter:
		return false
	case <-proc.Ctx.Done():
		return false
	}
}

func (shuffle *Shuffle) failLocalProducer(proc *process.Process, err error) {
	shuffle.ctr.shufflePool.abortWithError(proc.Mp(), err)
}

func (shuffle *Shuffle) stopWritingOnce() {
	// Only a successfully admitted holder may publish pool completion.
	if !shuffle.ctr.held || shuffle.ctr.writingStopped || shuffle.ctr.shufflePool == nil {
		return
	}
	shuffle.ctr.writingStopped = true
	shuffle.ctr.shufflePool.stopWriting()
}

func (shuffle *Shuffle) flushPending(proc *process.Process) (bool, <-chan struct{}, error) {
	nextBucket, nextOffset, waiter, done, err := shuffle.ctr.shufflePool.tryWrite(
		shuffle.ctr.pendingBat,
		shuffle.ctr.sels,
		shuffle.ctr.pendingBucket,
		shuffle.ctr.pendingOffset,
		proc,
	)
	shuffle.ctr.pendingBucket = nextBucket
	shuffle.ctr.pendingOffset = nextOffset
	if done || err != nil {
		shuffle.ctr.pendingBat = nil
		shuffle.ctr.pendingBucket = 0
		shuffle.ctr.pendingOffset = 0
	}
	return done, waiter, err
}

func (shuffle *Shuffle) enqueueBySels(bat *batch.Batch, proc *process.Process) error {
	shuffle.ctr.pendingBat = bat
	shuffle.ctr.pendingBucket = 0
	shuffle.ctr.pendingOffset = 0
	_, _, err := shuffle.flushPending(proc)
	return err
}

// evalAndShuffle evaluates the ShuffleExpr on the batch, computes shuffle bucket assignments,
// and either returns the batch directly (single bucket) or splits it into the shuffle pool.
// The original batch is NOT modified — the expression result is used only for hashing.
func (shuffle *Shuffle) evalAndShuffle(bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	vec, err := shuffle.ctr.exprExec.Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return nil, err
	}

	if vec.IsConstNull() {
		return shuffle.routeSingleBucket(bat, 0, proc)
	}
	if vec.IsConst() {
		var shuffleIdx uint64
		if shuffle.ShuffleType == int32(plan.ShuffleType_Range) {
			shuffleIdx = rangeShuffleConstVec(shuffle, vec)
		} else {
			shuffleIdx = shuffleConstVecByHash(shuffle, vec)
		}
		return shuffle.routeSingleBucket(bat, int32(shuffleIdx), proc)
	}

	sels := shuffle.clearSels()
	if shuffle.ShuffleType == int32(plan.ShuffleType_Range) {
		rangeShuffleVec(shuffle, sels, vec)
	} else {
		lenRegs := uint64(shuffle.BucketNum)
		if vec.HasNull() {
			hashShuffleVecWithNull(shuffle, sels, vec, lenRegs)
		} else {
			hashShuffleVecWithoutNull(shuffle, sels, vec, lenRegs)
		}
	}

	for i := range sels {
		if len(sels[i]) > 0 && len(sels[i]) != bat.RowCount() {
			break
		}
		if len(sels[i]) == bat.RowCount() {
			return shuffle.routeSingleBucket(bat, int32(i), proc)
		}
	}

	err = shuffle.enqueueBySels(bat, proc)
	return nil, err
}

func (shuffle *Shuffle) routeSingleBucket(bat *batch.Batch, shuffleIdx int32, proc *process.Process) (*batch.Batch, error) {
	bat.ShuffleIDX = shuffleIdx
	if shuffle.DrainAllBuckets || shuffleIdx == shuffle.CurrentShuffleIdx {
		return bat, nil
	}
	sels := shuffle.clearSels()
	for row := 0; row < bat.RowCount(); row++ {
		sels[shuffleIdx] = append(sels[shuffleIdx], int32(row))
	}
	err := shuffle.enqueueBySels(bat, proc)
	return nil, err
}

func (shuffle *Shuffle) handleRuntimeFilter(proc *process.Process) error {
	if shuffle.ctr.runtimeFilterHandled || shuffle.RuntimeFilterSpec == nil {
		return nil
	}
	receiver := message.NewMessageReceiver(
		[]int32{shuffle.RuntimeFilterSpec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		proc.GetMessageBoard())
	msgs, ctxDone, err := receiver.ReceiveMessage(true, proc.Ctx)
	if ctxDone {
		shuffle.ctr.runtimeFilterHandled = true
		return nil
	}
	if err != nil {
		return err
	}
	for i := range msgs {
		msg, ok := msgs[i].(message.RuntimeFilterMessage)
		if !ok {
			continue
		}
		if msg.Typ == message.RuntimeFilter_PASS || msg.Typ == message.RuntimeFilter_DROP {
			shuffle.ctr.runtimeFilterHandled = true
		}
	}
	return nil
}

func (shuffle *Shuffle) clearSels() [][]int32 {
	for i := range shuffle.ctr.sels {
		if len(shuffle.ctr.sels[i]) > 0 {
			shuffle.ctr.sels[i] = shuffle.ctr.sels[i][:0]
		}
	}
	return shuffle.ctr.sels
}

func shuffleConstVectorByHash(ap *Shuffle, bat *batch.Batch) uint64 {
	lenRegs := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		return plan2.SimpleInt64HashToRange(uint64(groupByCol[0]), lenRegs)
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		return stringHashToRange(ap, groupByCol[0].GetByteSlice(area), lenRegs)
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
}

func getShuffledSelsByHashWithNull(ap *Shuffle, bat *batch.Batch) [][]int32 {
	sels := ap.clearSels()
	lenRegs := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(v, lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		for row, v := range groupByCol {
			var regIndex uint64 = 0
			if !groupByVec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		appendStringHashSels(ap, sels, groupByVec, groupByCol, area, lenRegs, true)
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func getShuffledSelsByHashWithoutNull(ap *Shuffle, bat *batch.Batch) [][]int32 {
	sels := ap.clearSels()
	bucketNum := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(v, bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_decimal64:
		groupByCol := vector.MustFixedColNoTypeCheck[types.Decimal64](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_decimal128:
		groupByCol := vector.MustFixedColNoTypeCheck[types.Decimal128](groupByVec)
		for row, v := range groupByCol {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v.B0_63^v.B64_127), bucketNum)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		appendStringHashSels(ap, sels, groupByVec, groupByCol, area, bucketNum, false)
	default:
		panic(fmt.Sprintf("unsupported shuffle type %v, wrong plan!", groupByVec.GetType())) //something got wrong here!
	}
	return sels
}

func hashShuffle(ap *Shuffle, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.IsConstNull() {
		return ap.routeSingleBucket(bat, 0, proc)
	}
	if groupByVec.IsConst() {
		return ap.routeSingleBucket(bat, int32(shuffleConstVectorByHash(ap, bat)), proc)
	}

	if groupByVec.HasNull() {
		getShuffledSelsByHashWithNull(ap, bat)
	} else {
		getShuffledSelsByHashWithoutNull(ap, bat)
	}

	err := ap.enqueueBySels(bat, proc)
	return nil, err
}

func allBatchInOneRange(ap *Shuffle, bat *batch.Batch) (bool, uint64) {
	bucketNum := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.IsConstNull() {
		return true, 0
	}
	if groupByVec.HasNull() {
		return false, 0
	}

	var firstValueSigned, lastValueSigned int64
	var firstValueUnsigned, lastValueUnsigned uint64
	var signed bool
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		signed = true
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		firstValueSigned = groupByCol[0]
		if groupByVec.IsConst() {
			lastValueSigned = firstValueSigned
		} else {
			lastValueSigned = groupByCol[groupByVec.Length()-1]
		}
	case types.T_int32:
		signed = true
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		firstValueSigned = int64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueSigned = firstValueSigned
		} else {
			lastValueSigned = int64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_int16:
		signed = true
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		firstValueSigned = int64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueSigned = firstValueSigned
		} else {
			lastValueSigned = int64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		firstValueUnsigned = groupByCol[0]
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = groupByCol[groupByVec.Length()-1]
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		firstValueUnsigned = uint64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = uint64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		firstValueUnsigned = uint64(groupByCol[0])
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = uint64(groupByCol[groupByVec.Length()-1])
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		firstValueUnsigned = plan2.VarlenaToUint64(&groupByCol[0], area)
		if groupByVec.IsConst() {
			lastValueUnsigned = firstValueUnsigned
		} else {
			lastValueUnsigned = plan2.VarlenaToUint64(&groupByCol[groupByVec.Length()-1], area)
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}

	var regIndexFirst, regIndexLast uint64
	if len(ap.ShuffleRangeInt64) > 0 {
		regIndexFirst = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, firstValueSigned)
		regIndexLast = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, lastValueSigned)
	} else if len(ap.ShuffleRangeUint64) > 0 {
		regIndexFirst = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, firstValueUnsigned)
		regIndexLast = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, lastValueUnsigned)
	} else if signed {
		regIndexFirst = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, firstValueSigned, bucketNum)
		regIndexLast = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, lastValueSigned, bucketNum)
	} else {
		regIndexFirst = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), firstValueUnsigned, bucketNum)
		regIndexLast = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), lastValueUnsigned, bucketNum)
	}

	if regIndexFirst == regIndexLast {
		return true, regIndexFirst
	} else {
		return false, 0
	}
}

func getShuffledSelsByRangeWithoutNull(ap *Shuffle, bat *batch.Batch) [][]int32 {
	sels := ap.clearSels()
	bucketNum := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		if len(ap.ShuffleRangeInt64) > 0 {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		if len(ap.ShuffleRangeInt64) > 0 {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		if len(ap.ShuffleRangeInt64) > 0 {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		if len(ap.ShuffleRangeUint64) > 0 {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		if len(ap.ShuffleRangeUint64) > 0 {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		if len(ap.ShuffleRangeUint64) > 0 {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), bucketNum)
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		if area == nil {
			if len(ap.ShuffleRangeUint64) > 0 {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64Inline(&groupByCol[row])
					regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			} else {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64Inline(&groupByCol[row])
					regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			}
		} else {
			if len(ap.ShuffleRangeUint64) > 0 {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64(&groupByCol[row], area)
					regIndex := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			} else {
				for row := range groupByCol {
					v := plan2.VarlenaToUint64(&groupByCol[row], area)
					regIndex := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			}
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func getShuffledSelsByRangeWithNull(ap *Shuffle, bat *batch.Batch) [][]int32 {
	sels := ap.clearSels()
	bucketNum := uint64(ap.BucketNum)
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	switch groupByVec.GetType().Oid {
	case types.T_int64:
		groupByCol := vector.MustFixedColNoTypeCheck[int64](groupByVec)
		if len(ap.ShuffleRangeInt64) > 0 {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int32:
		groupByCol := vector.MustFixedColNoTypeCheck[int32](groupByVec)
		if len(ap.ShuffleRangeInt64) > 0 {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_int16:
		groupByCol := vector.MustFixedColNoTypeCheck[int16](groupByVec)
		if len(ap.ShuffleRangeInt64) > 0 {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint64:
		groupByCol := vector.MustFixedColNoTypeCheck[uint64](groupByVec)
		if len(ap.ShuffleRangeUint64) > 0 {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint32:
		groupByCol := vector.MustFixedColNoTypeCheck[uint32](groupByVec)
		if len(ap.ShuffleRangeUint64) > 0 {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_uint16:
		groupByCol := vector.MustFixedColNoTypeCheck[uint16](groupByVec)
		if len(ap.ShuffleRangeUint64) > 0 {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		} else {
			for row, v := range groupByCol {
				var regIndex uint64 = 0
				if !groupByVec.IsNull(uint64(row)) {
					regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), bucketNum)
				}
				sels[regIndex] = append(sels[regIndex], int32(row))
			}
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(groupByVec)
		if area == nil {
			if len(ap.ShuffleRangeUint64) > 0 {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64Inline(&groupByCol[row])
						regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					}
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			} else {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64Inline(&groupByCol[row])
						regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
					}
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			}
		} else {
			if len(ap.ShuffleRangeUint64) > 0 {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64(&groupByCol[row], area)
						regIndex = plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					}
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			} else {
				for row := range groupByCol {
					var regIndex uint64 = 0
					if !groupByVec.IsNull(uint64(row)) {
						v := plan2.VarlenaToUint64(&groupByCol[row], area)
						regIndex = plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
					}
					sels[regIndex] = append(sels[regIndex], int32(row))
				}
			}
		}
	default:
		panic("unsupported shuffle type, wrong plan!") //something got wrong here!
	}
	return sels
}

func rangeShuffle(ap *Shuffle, bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	groupByVec := bat.Vecs[ap.ShuffleColIdx]
	if groupByVec.GetSorted() || groupByVec.IsConst() {
		ok, regIndex := allBatchInOneRange(ap, bat)
		if ok {
			return ap.routeSingleBucket(bat, int32(regIndex), proc)
		}
	}
	var sels [][]int32
	if groupByVec.HasNull() {
		sels = getShuffledSelsByRangeWithNull(ap, bat)
	} else {
		sels = getShuffledSelsByRangeWithoutNull(ap, bat)
	}
	for i := range sels {
		if len(sels[i]) > 0 && len(sels[i]) != bat.RowCount() {
			break
		}
		if len(sels[i]) == bat.RowCount() { // all batch in one range
			return ap.routeSingleBucket(bat, int32(i), proc)
		}
	}
	err := ap.enqueueBySels(bat, proc)
	return nil, err
}

// shuffleConstVecByHash computes the bucket index for a constant vector.
func shuffleConstVecByHash(ap *Shuffle, vec *vector.Vector) uint64 {
	lenRegs := uint64(ap.BucketNum)
	switch vec.GetType().Oid {
	case types.T_int64:
		return plan2.SimpleInt64HashToRange(uint64(vector.MustFixedColNoTypeCheck[int64](vec)[0]), lenRegs)
	case types.T_int32:
		return plan2.SimpleInt64HashToRange(uint64(vector.MustFixedColNoTypeCheck[int32](vec)[0]), lenRegs)
	case types.T_int16:
		return plan2.SimpleInt64HashToRange(uint64(vector.MustFixedColNoTypeCheck[int16](vec)[0]), lenRegs)
	case types.T_uint64:
		return plan2.SimpleInt64HashToRange(vector.MustFixedColNoTypeCheck[uint64](vec)[0], lenRegs)
	case types.T_uint32:
		return plan2.SimpleInt64HashToRange(uint64(vector.MustFixedColNoTypeCheck[uint32](vec)[0]), lenRegs)
	case types.T_uint16:
		return plan2.SimpleInt64HashToRange(uint64(vector.MustFixedColNoTypeCheck[uint16](vec)[0]), lenRegs)
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(vec)
		return stringHashToRange(ap, groupByCol[0].GetByteSlice(area), lenRegs)
	default:
		panic("unsupported shuffle type, wrong plan!")
	}
}

// rangeShuffleConstVec computes the range bucket index for a constant vector.
func rangeShuffleConstVec(ap *Shuffle, vec *vector.Vector) uint64 {
	bucketNum := uint64(ap.BucketNum)
	switch vec.GetType().Oid {
	case types.T_int64:
		v := vector.MustFixedColNoTypeCheck[int64](vec)[0]
		if len(ap.ShuffleRangeInt64) > 0 {
			return plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
		}
		return plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, bucketNum)
	case types.T_int32:
		v := int64(vector.MustFixedColNoTypeCheck[int32](vec)[0])
		if len(ap.ShuffleRangeInt64) > 0 {
			return plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
		}
		return plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, bucketNum)
	case types.T_int16:
		v := int64(vector.MustFixedColNoTypeCheck[int16](vec)[0])
		if len(ap.ShuffleRangeInt64) > 0 {
			return plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
		}
		return plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, bucketNum)
	case types.T_uint64:
		v := vector.MustFixedColNoTypeCheck[uint64](vec)[0]
		if len(ap.ShuffleRangeUint64) > 0 {
			return plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
		}
		return plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
	case types.T_uint32:
		v := uint64(vector.MustFixedColNoTypeCheck[uint32](vec)[0])
		if len(ap.ShuffleRangeUint64) > 0 {
			return plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
		}
		return plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
	case types.T_uint16:
		v := uint64(vector.MustFixedColNoTypeCheck[uint16](vec)[0])
		if len(ap.ShuffleRangeUint64) > 0 {
			return plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
		}
		return plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(vec)
		v := plan2.VarlenaToUint64(&groupByCol[0], area)
		if len(ap.ShuffleRangeUint64) > 0 {
			return plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
		}
		return plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
	default:
		panic("unsupported shuffle type, wrong plan!")
	}
}

// rangeShuffleVec computes range bucket assignments for each row of an expression result vector.
func rangeShuffleVec(ap *Shuffle, sels [][]int32, vec *vector.Vector) {
	bucketNum := uint64(ap.BucketNum)
	switch vec.GetType().Oid {
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		if len(ap.ShuffleRangeInt64) > 0 {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, v)
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		} else {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, v, bucketNum)
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		}
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		if len(ap.ShuffleRangeInt64) > 0 {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		} else {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		}
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		if len(ap.ShuffleRangeInt64) > 0 {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexSignedSlice(ap.ShuffleRangeInt64, int64(v))
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		} else {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexSignedMinMax(ap.ShuffleColMin, ap.ShuffleColMax, int64(v), bucketNum)
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		}
	case types.T_uint64:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		if len(ap.ShuffleRangeUint64) > 0 {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		} else {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		}
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		if len(ap.ShuffleRangeUint64) > 0 {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		} else {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), bucketNum)
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		}
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		if len(ap.ShuffleRangeUint64) > 0 {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, uint64(v))
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		} else {
			for row, v := range col {
				if vec.IsNull(uint64(row)) {
					sels[0] = append(sels[0], int32(row))
				} else {
					idx := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), uint64(v), bucketNum)
					sels[idx] = append(sels[idx], int32(row))
				}
			}
		}
	case types.T_char, types.T_varchar, types.T_text:
		groupByCol, area := vector.MustVarlenaRawData(vec)
		if area == nil {
			if len(ap.ShuffleRangeUint64) > 0 {
				for row := range groupByCol {
					if vec.IsNull(uint64(row)) {
						sels[0] = append(sels[0], int32(row))
					} else {
						v := plan2.VarlenaToUint64Inline(&groupByCol[row])
						idx := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
						sels[idx] = append(sels[idx], int32(row))
					}
				}
			} else {
				for row := range groupByCol {
					if vec.IsNull(uint64(row)) {
						sels[0] = append(sels[0], int32(row))
					} else {
						v := plan2.VarlenaToUint64Inline(&groupByCol[row])
						idx := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
						sels[idx] = append(sels[idx], int32(row))
					}
				}
			}
		} else {
			if len(ap.ShuffleRangeUint64) > 0 {
				for row := range groupByCol {
					if vec.IsNull(uint64(row)) {
						sels[0] = append(sels[0], int32(row))
					} else {
						v := plan2.VarlenaToUint64(&groupByCol[row], area)
						idx := plan2.GetRangeShuffleIndexUnsignedSlice(ap.ShuffleRangeUint64, v)
						sels[idx] = append(sels[idx], int32(row))
					}
				}
			} else {
				for row := range groupByCol {
					if vec.IsNull(uint64(row)) {
						sels[0] = append(sels[0], int32(row))
					} else {
						v := plan2.VarlenaToUint64(&groupByCol[row], area)
						idx := plan2.GetRangeShuffleIndexUnsignedMinMax(uint64(ap.ShuffleColMin), uint64(ap.ShuffleColMax), v, bucketNum)
						sels[idx] = append(sels[idx], int32(row))
					}
				}
			}
		}
	default:
		panic("unsupported shuffle type, wrong plan!")
	}
}

func hashShuffleVecWithNull(ap *Shuffle, sels [][]int32, vec *vector.Vector, lenRegs uint64) {
	switch vec.GetType().Oid {
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		for row, v := range col {
			regIndex := uint64(0)
			if !vec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		for row, v := range col {
			regIndex := uint64(0)
			if !vec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		for row, v := range col {
			regIndex := uint64(0)
			if !vec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		for row, v := range col {
			regIndex := uint64(0)
			if !vec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(v, lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		for row, v := range col {
			regIndex := uint64(0)
			if !vec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		for row, v := range col {
			regIndex := uint64(0)
			if !vec.IsNull(uint64(row)) {
				regIndex = plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			}
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		col, area := vector.MustVarlenaRawData(vec)
		appendStringHashSels(ap, sels, vec, col, area, lenRegs, true)
	default:
		panic(fmt.Sprintf("unsupported shuffle type %v, wrong plan!", vec.GetType()))
	}
}

// hashShuffleVecWithoutNull hashes a vector without null values into shuffle bucket selections.
func hashShuffleVecWithoutNull(ap *Shuffle, sels [][]int32, vec *vector.Vector, lenRegs uint64) {
	switch vec.GetType().Oid {
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		for row, v := range col {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		for row, v := range col {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		for row, v := range col {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint64:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		for row, v := range col {
			regIndex := plan2.SimpleInt64HashToRange(v, lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		for row, v := range col {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		for row, v := range col {
			regIndex := plan2.SimpleInt64HashToRange(uint64(v), lenRegs)
			sels[regIndex] = append(sels[regIndex], int32(row))
		}
	case types.T_char, types.T_varchar, types.T_text:
		col, area := vector.MustVarlenaRawData(vec)
		appendStringHashSels(ap, sels, vec, col, area, lenRegs, false)
	default:
		panic(fmt.Sprintf("unsupported shuffle type %v, wrong plan!", vec.GetType()))
	}
}

func appendStringHashSels(
	ap *Shuffle,
	sels [][]int32,
	vec *vector.Vector,
	col []types.Varlena,
	area []byte,
	bucketNum uint64,
	withNull bool,
) {
	for row := range col {
		regIndex := uint64(0)
		if !withNull || !vec.IsNull(uint64(row)) {
			regIndex = stringHashToRange(ap, col[row].GetByteSlice(area), bucketNum)
		}
		sels[regIndex] = append(sels[regIndex], int32(row))
	}
}

func stringHashToRange(ap *Shuffle, value []byte, bucketNum uint64) uint64 {
	if ap.ctr.stableStringHash {
		return plan2.StableCharHashToRange(value, bucketNum)
	}
	return plan2.SimpleCharHashToRange(value, bucketNum)
}
