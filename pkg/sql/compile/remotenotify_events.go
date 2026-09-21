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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	pbpipeline "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// remoteNotifyEventState publishes a dispatch receiver notification without
// parking one event worker in receiveMsgAndForward.  MORPC receives are
// admitted one at a time as external events; forwarding and retry state
// transitions run as short ready tasks.
//
// The no-scheduler helper path in scope.go intentionally keeps the historical
// blocking implementation for tests and callers that do not own a Compile.
type remoteNotifyEventState struct {
	s         *Scope
	scheduler *scopeTaskScheduler

	uuid      uuid.UUID
	fromAddr  string
	newSender notifyMessageSenderFactory
	waitRetry notifyMessageRetryWait
	onResult  func(notifyMessageResult)
	wg        *sync.WaitGroup

	sender     *messageSenderOnClient
	forwardReg *process.WaitRegister
	pending    *batch.Batch
	decoder    remoteBatchDecoder
	attempt    int

	stateMu    sync.Mutex
	finished   bool
	finishOnce sync.Once
}

func newRemoteNotifyEventState(
	s *Scope,
	scheduler *scopeTaskScheduler,
	info *RemoteReceivRegInfo,
	newSender notifyMessageSenderFactory,
	waitRetry notifyMessageRetryWait,
	onResult func(notifyMessageResult),
	wg *sync.WaitGroup,
) *remoteNotifyEventState {
	return &remoteNotifyEventState{
		s:          s,
		scheduler:  scheduler,
		uuid:       info.Uuid,
		fromAddr:   info.FromAddr,
		newSender:  newSender,
		waitRetry:  waitRetry,
		onResult:   onResult,
		wg:         wg,
		forwardReg: s.Proc.Reg.MergeReceivers[info.Idx],
	}
}

func (r *remoteNotifyEventState) start() error {
	return r.scheduler.submitBlockingEvent("remote-notify-open", r.open)
}

func (r *remoteNotifyEventState) open() {
	if err := r.contextError(); err != nil {
		r.finish(err)
		return
	}
	sender, err := r.newSender(
		r.s.Proc.Ctx,
		r.s.Proc.GetService(),
		r.fromAddr,
		r.s.Proc.Mp(),
		nil,
	)
	if err != nil {
		r.finish(err)
		return
	}
	r.sender = sender

	message := cnclient.AcquireMessage()
	message.SetID(sender.streamSender.ID())
	message.SetMessageType(pbpipeline.Method_PrepareDoneNotifyMessage)
	sender.requestStreamProtocols(message)
	message.NeedNotReply = false
	message.Uuid = r.uuid[:]
	sender.markReportingRequestStarted()
	if err = r.scheduler.submitStreamSend(
		"remote-notify-send",
		sender.streamSender,
		sender.ctx,
		message,
		func(sendErr error) {
			if sendErr != nil {
				r.finish(sendErr)
				return
			}
			sender.markStreamActive(pbpipeline.Method_PrepareDoneNotifyMessage)
			if receiveErr := r.scheduler.submitRoot("remote-notify-receive-start", r.receiveNext); receiveErr != nil {
				r.finish(receiveErr)
			}
		},
	); err != nil {
		r.finish(err)
	}
}

func (r *remoteNotifyEventState) receiveNext() {
	if r.isFinished() {
		return
	}
	if err := r.contextError(); err != nil {
		r.finish(err)
		return
	}
	err := r.scheduler.submitChannelEvent(
		"remote-notify-receive",
		r.sender.receiveCh,
		func(message morpc.Message, ok bool) {
			bat, end, receiveErr := r.decoder.consume(r.sender, message, ok)
			if receiveErr != nil || end || bat != nil {
				r.handleReceived(bat, end, receiveErr)
				return
			}
			// A fragmented batch needs another transport event before it can
			// be forwarded to the dispatch receiver.
			if err := r.receiveNextError(); err != nil {
				r.finish(err)
			}
		},
		func(err error) {
			r.finish(err)
		},
	)
	if err != nil {
		r.finish(err)
	}
}

func (r *remoteNotifyEventState) receiveNextError() error {
	if r.isFinished() {
		return nil
	}
	if err := r.contextError(); err != nil {
		r.finish(err)
		return err
	}
	return r.scheduler.submitChannelEvent(
		"remote-notify-receive",
		r.sender.receiveCh,
		func(message morpc.Message, ok bool) {
			bat, end, receiveErr := r.decoder.consume(r.sender, message, ok)
			if receiveErr != nil || end || bat != nil {
				r.handleReceived(bat, end, receiveErr)
				return
			}
			if err := r.receiveNextError(); err != nil {
				r.finish(err)
			}
		},
		func(err error) {
			r.finish(err)
		},
	)
}

func (r *remoteNotifyEventState) handleReceived(
	bat *batch.Batch,
	end bool,
	receiveErr error,
) {
	if r.isFinished() {
		r.cleanBatch(bat)
		return
	}
	if receiveErr != nil {
		r.cleanBatch(bat)
		if isRemoteDispatchNotRegisteredYetError(receiveErr) {
			r.retry()
			return
		}
		r.finish(receiveErr)
		return
	}
	if end || bat == nil {
		r.finish(nil)
		return
	}
	r.pending = bat
	r.forwardPending()
}

func (r *remoteNotifyEventState) forwardPending() {
	if r.isFinished() || r.pending == nil {
		return
	}
	if r.forwardReg == nil {
		r.finish(moerr.NewInternalErrorNoCtx("remote notify receiver is nil"))
		return
	}
	if r.forwardReg.TrySendDataDirect(r.pending, r.s.Proc.Mp()) {
		r.pending = nil
		r.acknowledge(func(err error) {
			if err != nil {
				r.finish(err)
				return
			}
			r.receiveNext()
		})
		return
	}
	if done := r.forwardReg.Done(); done != nil {
		select {
		case <-done:
			r.cleanPending()
			r.acknowledge(func(err error) {
				if err != nil {
					r.finish(err)
					return
				}
				r.finish(nil)
			})
			return
		default:
		}
	}
	if err := r.registerReady(r.forwardReg.RegisterCapacityReady, r.forwardPending); err != nil {
		r.finish(err)
	}
}

func (r *remoteNotifyEventState) acknowledge(done func(error)) {
	if err := r.sender.acknowledgeRemoteBatchAsync(r.scheduler, "remote-notify-ack", done); err != nil {
		r.finish(err)
	}
}

func (r *remoteNotifyEventState) registerReady(
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
			if err := r.scheduler.submitRoot("remote-notify-ready", resume); err != nil {
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

func (r *remoteNotifyEventState) retry() {
	if r.sender != nil {
		r.sender.prepareForLocalCleanup()
		r.sender.close()
		r.sender = nil
	}
	if r.waitRetry == nil {
		attempt := r.attempt
		r.attempt++
		if err := r.scheduler.submitTimer(
			"remote-notify-retry-timer",
			notifyMessageRetryDelay(attempt, r.uuid),
			r.open,
		); err != nil {
			r.finish(err)
		}
		return
	}
	if err := r.scheduler.submitBlockingEvent("remote-notify-retry-wait", func() {
		err := r.waitRetry(r.s.Proc.Ctx, r.attempt, r.uuid)
		r.attempt++
		if submitErr := r.scheduler.submitRoot("remote-notify-retry-ready", func() {
			if err != nil {
				r.finish(err)
				return
			}
			if startErr := r.scheduler.submitBlockingEvent("remote-notify-open", r.open); startErr != nil {
				r.finish(startErr)
			}
		}); submitErr != nil {
			r.finish(submitErr)
		}
	}); err != nil {
		r.finish(err)
	}
}

func (r *remoteNotifyEventState) contextError() error {
	if r.s == nil || r.s.Proc == nil || r.s.Proc.Ctx == nil {
		return nil
	}
	select {
	case <-r.s.Proc.Ctx.Done():
		return remoteRegistrationContextError(r.s.Proc.Ctx)
	default:
		return nil
	}
}

func (r *remoteNotifyEventState) cleanBatch(bat *batch.Batch) {
	if bat != nil {
		bat.Clean(r.s.Proc.Mp())
		if r.sender != nil {
			_ = r.sender.acknowledgeRemoteBatch()
		}
	}
}

func (r *remoteNotifyEventState) cleanPending() {
	if r.pending != nil {
		r.pending.Clean(r.s.Proc.Mp())
		r.pending = nil
	}
}

func (r *remoteNotifyEventState) isFinished() bool {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	return r.finished
}

func (r *remoteNotifyEventState) finish(err error) {
	r.finishOnce.Do(func() {
		r.stateMu.Lock()
		r.finished = true
		r.stateMu.Unlock()
		if submitErr := r.scheduler.submitRoot("remote-notify-finish", func() {
			r.finishReady(err)
		}); submitErr != nil {
			r.finishReady(submitErr)
		}
	})
}

func (r *remoteNotifyEventState) finishReady(err error) {
	r.cleanPending()
	if err != nil {
		err, _ = normalizeScopeRunError(err, r.s.Proc.Ctx, scopeRunQueryContext(r.s.Proc))
		r.s.cancelMergeSiblingsOnError(err)
	}
	reg := r.forwardReg
	sendRemoteNotifyCleanupTerminal(r.s.Proc, reg, err)
	if r.onResult != nil {
		r.onResult(notifyMessageResult{err: err, sender: r.sender})
	}
	if r.wg != nil {
		r.wg.Done()
	}
}
