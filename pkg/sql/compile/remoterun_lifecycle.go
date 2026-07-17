// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var pipelineStreamFinishTimeout = 30 * time.Second

type pipelineStreamLifecycleKey struct {
	session morpc.ClientSession
	id      uint64
}

type pipelineStreamFinishRequest struct {
	token morpc.StreamTerminalToken
}

type pipelineStreamLifecycle struct {
	key       pipelineStreamLifecycleKey
	finishC   chan pipelineStreamFinishRequest
	cleanedC  chan struct{}
	cleanOnce sync.Once
}

var pipelineStreamLifecycles sync.Map

func registerPipelineStreamLifecycle(
	cs morpc.ClientSession,
	id uint64,
) (*pipelineStreamLifecycle, error) {
	lifecycle := &pipelineStreamLifecycle{
		key:      pipelineStreamLifecycleKey{session: cs, id: id},
		finishC:  make(chan pipelineStreamFinishRequest, 1),
		cleanedC: make(chan struct{}),
	}
	if _, loaded := pipelineStreamLifecycles.LoadOrStore(lifecycle.key, lifecycle); loaded {
		_ = cs.Close()
		return nil, moerr.NewInvalidStateNoCtx("duplicate pipeline stream lifecycle")
	}
	v2.PipelineStreamLifecycleGauge.Inc()
	return lifecycle, nil
}

func (lifecycle *pipelineStreamLifecycle) remove() {
	if pipelineStreamLifecycles.CompareAndDelete(lifecycle.key, lifecycle) {
		v2.PipelineStreamLifecycleGauge.Dec()
	}
}

func (lifecycle *pipelineStreamLifecycle) markCleaned() {
	lifecycle.cleanOnce.Do(func() { close(lifecycle.cleanedC) })
}

// waitForFinish starts only after the accepted End response has been flushed.
// A false return means the connection is no longer safe to reuse.
func (lifecycle *pipelineStreamLifecycle) waitForFinish(
	messageCtx context.Context,
	connectionCtx context.Context,
) bool {
	timer := time.NewTimer(pipelineStreamFinishTimeout)
	defer timer.Stop()
	select {
	case <-lifecycle.finishC:
		return true
	case <-messageCtx.Done():
		v2.PipelineStreamTeardownCounter.WithLabelValues("server_message_cancel").Inc()
	case <-connectionCtx.Done():
		v2.PipelineStreamTeardownCounter.WithLabelValues("server_connection_close").Inc()
	case <-timer.C:
		v2.PipelineStreamTeardownCounter.WithLabelValues("server_fin_timeout").Inc()
	}
	lifecycle.remove()
	return false
}

func handlePipelineStreamFinish(
	ctx context.Context,
	message *pipeline.Message,
	cs morpc.ClientSession,
	messageAcquirer func() morpc.Message,
) error {
	poison := func(err error) error {
		_ = cs.Close()
		return err
	}
	if message.GetSid() != pipeline.Status_Last ||
		message.GetRequestedTeardownMode() != pipeline.StreamTeardownMode_FinishAck {
		return poison(moerr.NewInvalidInputNoCtx("pipeline stream FIN must be a Last message"))
	}
	token, ok := morpc.StreamTerminalTokenFromContext(ctx)
	if !ok {
		return poison(moerr.NewInvalidStateNoCtx("pipeline stream FIN has no validated stream token"))
	}
	return handleValidatedPipelineStreamFinish(ctx, message, cs, messageAcquirer, token)
}

// handleValidatedPipelineStreamFinish coordinates teardown after the morpc IO
// loop has authenticated the terminal stream token. Keeping that validation at
// the public handler boundary makes the coordination state machine testable
// without exposing a way for application code to manufacture terminal tokens.
func handleValidatedPipelineStreamFinish(
	ctx context.Context,
	message *pipeline.Message,
	cs morpc.ClientSession,
	messageAcquirer func() morpc.Message,
	token morpc.StreamTerminalToken,
) error {
	poison := func(err error) error {
		_ = cs.Close()
		return err
	}
	key := pipelineStreamLifecycleKey{session: cs, id: message.GetID()}
	value, ok := pipelineStreamLifecycles.Load(key)
	if !ok {
		return poison(moerr.NewInvalidStateNoCtx("pipeline stream FIN has no active lifecycle"))
	}
	lifecycle := value.(*pipelineStreamLifecycle)
	coordinationCtx, coordinationCancel := context.WithTimeout(ctx, pipelineStreamFinishTimeout)
	defer coordinationCancel()
	select {
	case lifecycle.finishC <- pipelineStreamFinishRequest{token: token}:
	case <-cs.SessionCtx().Done():
		return poison(moerr.NewStreamClosedNoCtx())
	case <-coordinationCtx.Done():
		lifecycle.remove()
		return poison(coordinationCtx.Err())
	}

	select {
	case <-lifecycle.cleanedC:
	case <-cs.SessionCtx().Done():
		lifecycle.remove()
		return moerr.NewStreamClosedNoCtx()
	case <-coordinationCtx.Done():
		lifecycle.remove()
		return poison(coordinationCtx.Err())
	}

	finisher, ok := cs.(morpc.StreamFinisher)
	if !ok {
		lifecycle.remove()
		return poison(moerr.NewInvalidStateNoCtx("client session cannot finish a stream"))
	}
	response, ok := messageAcquirer().(*pipeline.Message)
	if !ok {
		lifecycle.remove()
		return poison(moerr.NewInvalidStateNoCtx("pipeline FIN acquired an invalid response"))
	}
	response.SetID(message.GetID())
	response.SetSid(pipeline.Status_MessageEnd)
	response.SetMessageType(pipeline.Method_PipelineStreamFinishAck)
	response.AcceptedTeardownMode = pipeline.StreamTeardownMode_FinishAck

	finishCtx, cancel := context.WithTimeout(context.Background(), pipelineStreamFinishTimeout)
	defer cancel()
	start := time.Now()
	err := finisher.FinishStream(finishCtx, token, response)
	v2.PipelineStreamFinishDurationHistogram.Observe(time.Since(start).Seconds())
	if err == nil {
		v2.PipelineStreamTeardownCounter.WithLabelValues("server_fin_ack").Inc()
	} else {
		v2.PipelineStreamTeardownCounter.WithLabelValues("server_fin_ack_error").Inc()
	}
	lifecycle.remove()
	return err
}
