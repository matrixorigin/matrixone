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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/stretchr/testify/require"
)

type lifecycleTestSession struct {
	morpc.ClientSession
	ctx        context.Context
	closeCalls int
}

func (s *lifecycleTestSession) Close() error {
	s.closeCalls++
	return nil
}

func (s *lifecycleTestSession) SessionCtx() context.Context {
	return s.ctx
}

type lifecycleTestFinisherSession struct {
	*lifecycleTestSession
	finish func(context.Context, morpc.StreamTerminalToken, morpc.Message) error
}

func (s *lifecycleTestFinisherSession) FinishStream(
	ctx context.Context,
	token morpc.StreamTerminalToken,
	response morpc.Message,
) error {
	return s.finish(ctx, token, response)
}

func newPipelineFinishMessage(id uint64) *pipeline.Message {
	return &pipeline.Message{
		Id:                    id,
		Cmd:                   pipeline.Method_PipelineStreamFinish,
		Sid:                   pipeline.Status_Last,
		RequestedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
	}
}

func TestPipelineStreamLifecycleTimeoutRemovesRegistration(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	lifecycle, err := registerPipelineStreamLifecycle(session, 101)
	require.NoError(t, err)
	oldTimeout := pipelineStreamFinishTimeout
	pipelineStreamFinishTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		pipelineStreamFinishTimeout = oldTimeout
		lifecycle.remove()
	})

	require.False(t, lifecycle.waitForFinish(context.Background(), context.Background()))
	_, exists := pipelineStreamLifecycles.Load(lifecycle.key)
	require.False(t, exists)
}

func TestPipelineStreamLifecycleDuplicatePoisonsSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	session.EXPECT().Close().Return(nil)
	first, err := registerPipelineStreamLifecycle(session, 102)
	require.NoError(t, err)
	defer first.remove()
	_, err = registerPipelineStreamLifecycle(session, 102)
	require.Error(t, err)
}

func TestPipelineStreamLifecycleWaitTermination(t *testing.T) {
	tests := []struct {
		name       string
		prepare    func(*pipelineStreamLifecycle) (context.Context, context.Context)
		wantFinish bool
	}{
		{
			name: "finish",
			prepare: func(lifecycle *pipelineStreamLifecycle) (context.Context, context.Context) {
				lifecycle.finishC <- pipelineStreamFinishRequest{}
				return context.Background(), context.Background()
			},
			wantFinish: true,
		},
		{
			name: "message canceled",
			prepare: func(*pipelineStreamLifecycle) (context.Context, context.Context) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, context.Background()
			},
		},
		{
			name: "connection closed",
			prepare: func(*pipelineStreamLifecycle) (context.Context, context.Context) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return context.Background(), ctx
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := &lifecycleTestSession{ctx: context.Background()}
			lifecycle, err := registerPipelineStreamLifecycle(session, uint64(200+i))
			require.NoError(t, err)
			t.Cleanup(lifecycle.remove)
			messageCtx, connectionCtx := tt.prepare(lifecycle)
			require.Equal(t, tt.wantFinish, lifecycle.waitForFinish(messageCtx, connectionCtx))
			_, exists := pipelineStreamLifecycles.Load(lifecycle.key)
			require.Equal(t, tt.wantFinish, exists)
		})
	}
}

func TestPipelineStreamLifecycleMarkCleanedIsIdempotent(t *testing.T) {
	lifecycle := &pipelineStreamLifecycle{cleanedC: make(chan struct{})}
	lifecycle.markCleaned()
	lifecycle.markCleaned()
	select {
	case <-lifecycle.cleanedC:
	default:
		t.Fatal("cleaned lifecycle was not signaled")
	}
}

func TestPipelineStreamFinishRejectsMalformedMessage(t *testing.T) {
	tests := []struct {
		name    string
		message *pipeline.Message
	}{
		{name: "not last", message: &pipeline.Message{Id: 103, RequestedTeardownMode: pipeline.StreamTeardownMode_FinishAck}},
		{name: "mode not negotiated", message: &pipeline.Message{Id: 104, Sid: pipeline.Status_Last}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := &lifecycleTestSession{ctx: context.Background()}
			err := handlePipelineStreamFinish(
				context.Background(), tt.message, session,
				func() morpc.Message { return &pipeline.Message{} },
			)
			require.Error(t, err)
			require.Equal(t, 1, session.closeCalls)
		})
	}
}

func TestPipelineStreamFinishWithoutValidatedTokenPoisonsSession(t *testing.T) {
	session := &lifecycleTestSession{ctx: context.Background()}
	err := handlePipelineStreamFinish(
		context.Background(),
		newPipelineFinishMessage(105),
		session,
		func() morpc.Message { return &pipeline.Message{} },
	)
	require.Error(t, err)
	require.Equal(t, 1, session.closeCalls)
}

func TestValidatedPipelineStreamFinishRequiresLifecycle(t *testing.T) {
	session := &lifecycleTestSession{ctx: context.Background()}
	err := handleValidatedPipelineStreamFinish(
		context.Background(), newPipelineFinishMessage(106), session,
		func() morpc.Message { return &pipeline.Message{} },
		morpc.StreamTerminalToken{},
	)
	require.Error(t, err)
	require.Equal(t, 1, session.closeCalls)
}

func TestValidatedPipelineStreamFinishRequiresFinisher(t *testing.T) {
	session := &lifecycleTestSession{ctx: context.Background()}
	lifecycle, err := registerPipelineStreamLifecycle(session, 107)
	require.NoError(t, err)
	t.Cleanup(lifecycle.remove)
	lifecycle.markCleaned()

	err = handleValidatedPipelineStreamFinish(
		context.Background(), newPipelineFinishMessage(107), session,
		func() morpc.Message { return &pipeline.Message{} },
		morpc.StreamTerminalToken{},
	)
	require.Error(t, err)
	require.Equal(t, 1, session.closeCalls)
	_, exists := pipelineStreamLifecycles.Load(lifecycle.key)
	require.False(t, exists)
}

func TestValidatedPipelineStreamFinishRejectsInvalidResponse(t *testing.T) {
	base := &lifecycleTestSession{ctx: context.Background()}
	session := &lifecycleTestFinisherSession{
		lifecycleTestSession: base,
		finish: func(context.Context, morpc.StreamTerminalToken, morpc.Message) error {
			t.Fatal("FinishStream must not run with an invalid response")
			return nil
		},
	}
	lifecycle, err := registerPipelineStreamLifecycle(session, 108)
	require.NoError(t, err)
	t.Cleanup(lifecycle.remove)
	lifecycle.markCleaned()

	err = handleValidatedPipelineStreamFinish(
		context.Background(), newPipelineFinishMessage(108), session,
		func() morpc.Message { return nil },
		morpc.StreamTerminalToken{},
	)
	require.Error(t, err)
	require.Equal(t, 1, session.closeCalls)
}

func TestValidatedPipelineStreamFinishAck(t *testing.T) {
	wantErr := errors.New("finish failed")
	tests := []struct {
		name    string
		id      uint64
		wantErr error
	}{
		{name: "success", id: 109},
		{name: "finish error", id: 110, wantErr: wantErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := &lifecycleTestSession{ctx: context.Background()}
			response := &pipeline.Message{}
			session := &lifecycleTestFinisherSession{
				lifecycleTestSession: base,
				finish: func(ctx context.Context, _ morpc.StreamTerminalToken, message morpc.Message) error {
					require.NoError(t, ctx.Err())
					require.Same(t, response, message)
					require.Equal(t, tt.id, response.GetID())
					require.Equal(t, pipeline.Status_MessageEnd, response.GetSid())
					require.Equal(t, pipeline.Method_PipelineStreamFinishAck, response.GetCmd())
					require.Equal(t, pipeline.StreamTeardownMode_FinishAck, response.GetAcceptedTeardownMode())
					return tt.wantErr
				},
			}
			lifecycle, err := registerPipelineStreamLifecycle(session, tt.id)
			require.NoError(t, err)
			t.Cleanup(lifecycle.remove)
			lifecycle.markCleaned()

			err = handleValidatedPipelineStreamFinish(
				context.Background(), newPipelineFinishMessage(tt.id), session,
				func() morpc.Message { return response },
				morpc.StreamTerminalToken{},
			)
			require.ErrorIs(t, err, tt.wantErr)
			_, exists := pipelineStreamLifecycles.Load(lifecycle.key)
			require.False(t, exists)
		})
	}
}

func TestValidatedPipelineStreamFinishTimeoutPoisonsSession(t *testing.T) {
	oldTimeout := pipelineStreamFinishTimeout
	pipelineStreamFinishTimeout = 10 * time.Millisecond
	t.Cleanup(func() { pipelineStreamFinishTimeout = oldTimeout })

	base := &lifecycleTestSession{ctx: context.Background()}
	session := &lifecycleTestFinisherSession{
		lifecycleTestSession: base,
		finish: func(context.Context, morpc.StreamTerminalToken, morpc.Message) error {
			t.Fatal("FinishStream must not run before cleanup")
			return nil
		},
	}
	lifecycle, err := registerPipelineStreamLifecycle(session, 111)
	require.NoError(t, err)
	t.Cleanup(lifecycle.remove)

	err = handleValidatedPipelineStreamFinish(
		context.Background(), newPipelineFinishMessage(111), session,
		func() morpc.Message { return &pipeline.Message{} },
		morpc.StreamTerminalToken{},
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, session.closeCalls)
	_, exists := pipelineStreamLifecycles.Load(lifecycle.key)
	require.False(t, exists)
}
