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

package message

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestJoinMapResultDistinguishesSuccessEmptyAndBuildError(t *testing.T) {
	success := NewJoinMapResult(nil)
	require.True(t, success.Finalized())
	require.True(t, success.IsSuccess())
	require.True(t, success.IsEmpty())
	require.Nil(t, success.BuildError())

	baseErr := moerr.NewOOM(context.Background())
	failure := NewJoinMapBuildErrorResult(baseErr)
	require.True(t, failure.Finalized())
	require.True(t, failure.IsBuildError())
	require.Nil(t, failure.JoinMap())
	require.Equal(t, baseErr.ErrorCode(), failure.BuildError().ErrorCode())
	require.Equal(t, baseErr.Error(), failure.BuildError().Error())
	require.NotEqual(t, success.Kind(), failure.Kind())

	var got *moerr.Error
	require.ErrorAs(t, failure.Err(), &got)
	require.Equal(t, baseErr.ErrorCode(), got.ErrorCode())
}

func TestJoinMapBuildErrorPreservesCancellationSemantics(t *testing.T) {
	tests := []struct {
		name         string
		buildErr     error
		wantCanceled bool
		wantDeadline bool
		wantCode     uint16
	}{
		{
			name:         "canceled",
			buildErr:     context.Canceled,
			wantCanceled: true,
			wantCode:     moerr.ErrQueryInterrupted,
		},
		{
			name:         "wrapped canceled",
			buildErr:     fmt.Errorf("hash build stopped: %w", context.Canceled),
			wantCanceled: true,
			wantCode:     moerr.ErrQueryInterrupted,
		},
		{
			name:         "deadline",
			buildErr:     context.DeadlineExceeded,
			wantDeadline: true,
			wantCode:     moerr.ErrQueryTimeout,
		},
		{
			name:         "query interrupted moerr",
			buildErr:     moerr.NewQueryInterrupted(context.Background()),
			wantCanceled: true,
			wantCode:     moerr.ErrQueryInterrupted,
		},
		{
			name:         "query timeout moerr",
			buildErr:     moerr.NewQueryTimeout(context.Background()),
			wantDeadline: true,
			wantCode:     moerr.ErrQueryTimeout,
		},
		{
			name:         "joined cancellation",
			buildErr:     errors.Join(context.Canceled, context.DeadlineExceeded),
			wantCanceled: true,
			wantDeadline: true,
			wantCode:     moerr.ErrQueryTimeout,
		},
		{
			name:         "joined moerr cancellation and deadline",
			buildErr:     errors.Join(moerr.NewQueryInterrupted(context.Background()), context.DeadlineExceeded),
			wantCanceled: true,
			wantDeadline: true,
			wantCode:     moerr.ErrQueryTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buildErr := NewJoinMapBuildError(tt.buildErr)
			got := buildErr.AsError()
			require.Equal(t, tt.buildErr.Error(), got.Error())
			require.Equal(t, tt.wantCanceled, errors.Is(got, context.Canceled))
			require.Equal(t, tt.wantDeadline, errors.Is(got, context.DeadlineExceeded))
			require.Equal(t, tt.wantCode, buildErr.ErrorCode())
			require.True(t, moerr.IsMoErrCode(buildErr.AsMoErr(), tt.wantCode))
			var me *moerr.Error
			require.ErrorAs(t, got, &me)
			require.Equal(t, tt.wantCode, me.ErrorCode())
			require.Empty(t, buildErr.Detail())
		})
	}
}

type emptyJoinedError struct{}

func (emptyJoinedError) Error() string   { return "empty joined error" }
func (emptyJoinedError) Unwrap() []error { return nil }

type emptyWrappedError struct{}

func (emptyWrappedError) Error() string { return "empty wrapped error" }
func (emptyWrappedError) Unwrap() error { return nil }

func TestJoinMapBuildErrorDefensiveCompatibility(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var buildErr *JoinMapBuildError
		require.Equal(t, "hash build failed", buildErr.Error())
		require.Equal(t, uint16(moerr.ErrInternal), buildErr.ErrorCode())
		require.Empty(t, buildErr.Detail())
		require.True(t, moerr.IsMoErrCode(buildErr.AsMoErr(), moerr.ErrInternal))
		require.True(t, moerr.IsMoErrCode(buildErr.AsError(), moerr.ErrInternal))
	})

	t.Run("zero value", func(t *testing.T) {
		buildErr := new(JoinMapBuildError)
		require.Equal(t, "hash build failed", buildErr.Error())
		require.Equal(t, uint16(moerr.ErrInternal), buildErr.ErrorCode())
		require.Empty(t, buildErr.Detail())
		require.True(t, moerr.IsMoErrCode(buildErr.AsMoErr(), moerr.ErrInternal))
		require.True(t, moerr.IsMoErrCode(buildErr.AsError(), moerr.ErrInternal))
	})

	t.Run("context without diagnostic", func(t *testing.T) {
		buildErr := &JoinMapBuildError{contextErr: context.Canceled}
		require.Equal(t, context.Canceled.Error(), buildErr.Error())
		require.ErrorIs(t, buildErr.Unwrap(), context.Canceled)
	})

	t.Run("nil input", func(t *testing.T) {
		kind, ok := contextCancellationTreeKind(nil)
		require.False(t, ok)
		require.Zero(t, kind)
		kind, ok = contextCancellationMoErrKind(nil)
		require.False(t, ok)
		require.Zero(t, kind)
		require.Nil(t, firstSubstantiveMoErr(nil))

		buildErr := NewJoinMapBuildError(nil)
		require.True(t, moerr.IsMoErrCode(buildErr.AsError(), moerr.ErrInternal))
		require.ErrorContains(t, buildErr, "without an error")
		require.False(t, buildErr.As(new(error)))
	})

	t.Run("empty joined error", func(t *testing.T) {
		buildErr := NewJoinMapBuildError(emptyJoinedError{})
		require.True(t, moerr.IsMoErrCode(buildErr.AsError(), moerr.ErrInternal))
		require.NotErrorIs(t, buildErr, context.Canceled)
		require.NotErrorIs(t, buildErr, context.DeadlineExceeded)
	})

	t.Run("empty wrapped error", func(t *testing.T) {
		buildErr := NewJoinMapBuildError(emptyWrappedError{})
		require.True(t, moerr.IsMoErrCode(buildErr.AsError(), moerr.ErrInternal))
		require.NotErrorIs(t, buildErr, context.Canceled)
		require.NotErrorIs(t, buildErr, context.DeadlineExceeded)
	})

	t.Run("cancellation moerr detail", func(t *testing.T) {
		queryInterrupted := moerr.NewQueryInterrupted(context.Background())
		queryInterrupted.SetDetail("canceled by sibling target")
		buildErr := NewJoinMapBuildError(queryInterrupted)

		require.ErrorIs(t, buildErr.AsError(), context.Canceled)
		require.Equal(t, queryInterrupted.Detail(), buildErr.Detail())
		copyErr := buildErr.AsMoErr()
		require.Equal(t, queryInterrupted.Detail(), copyErr.Detail())
		copyErr.SetDetail("mutated consumer copy")
		require.Equal(t, queryInterrupted.Detail(), buildErr.Detail())
	})
}

func TestReceiveJoinMapPreservesCancellationSemantics(t *testing.T) {
	mb := NewMessageBoard()
	require.True(t, FinalizeJoinMapBuildError(mb, 42, false, 0, context.Canceled))

	jm, err := ReceiveJoinMap(42, false, 0, mb, context.Background())
	require.Nil(t, jm)
	require.ErrorIs(t, err, context.Canceled)
}

func TestJoinMapBuildErrorPrefersSubstantiveError(t *testing.T) {
	t.Run("moerr", func(t *testing.T) {
		oom := moerr.NewOOM(context.Background())
		got := NewJoinMapBuildError(errors.Join(
			moerr.NewQueryInterrupted(context.Background()),
			fmt.Errorf("wrapped build failure: %w", oom),
		)).AsError()

		require.True(t, moerr.IsMoErrCode(got, moerr.ErrOOM), got)
		require.NotErrorIs(t, got, context.Canceled)
	})

	t.Run("go error", func(t *testing.T) {
		buildErr := errors.New("build storage failed")
		got := NewJoinMapBuildError(errors.Join(
			moerr.NewQueryInterrupted(context.Background()),
			buildErr,
		)).AsError()

		require.True(t, moerr.IsMoErrCode(got, moerr.ErrInternal), got)
		require.ErrorContains(t, got, buildErr.Error())
		require.NotErrorIs(t, got, context.Canceled)
	})
}

func TestSendJoinMapResultRetainsOwnershipWhenBoardUnavailable(t *testing.T) {
	jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, nil)
	require.False(t, SendJoinMapResult(
		NewJoinMapResult(jm),
		1,
		false,
		0,
		nil,
	))
	require.True(t, jm.IsValid())
	jm.FreeMemory()
}

func TestRuntimeFilterMemoryReleaseIsSharedAcrossMessageCopies(t *testing.T) {
	var releases atomic.Int32
	msg := RuntimeFilterMessage{Data: make([]byte, 128)}
	msg.SetMemoryRelease(func() { releases.Add(1) })
	copy1, copy2 := msg, msg
	copy1.Destroy()
	copy2.Destroy()
	msg.Destroy()
	if releases.Load() != 1 {
		t.Fatalf("release count = %d, want 1", releases.Load())
	}
}

func TestReceiveJoinMapResultBroadcastsOneImmutableBuildError(t *testing.T) {
	const consumers = 8
	mb := NewMessageBoard()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := make([]JoinMapResult, consumers)
	errs := make([]error, consumers)
	var wg sync.WaitGroup
	wg.Add(consumers)
	for i := 0; i < consumers; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = ReceiveJoinMapResult(42, false, 0, mb, ctx)
		}(i)
	}
	// Let every consumer register its waiter before publishing the terminal
	// value.  This is deterministic (no sleep) and still exercises a blocked
	// receiver for each consumer.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mb.rwMutex.RLock()
		waiters := len(mb.waiters)
		mb.rwMutex.RUnlock()
		if waiters == consumers {
			break
		}
		require.Less(t, time.Now(), deadline)
		runtime.Gosched()
	}

	baseErr := moerr.NewOOM(context.Background())
	SendJoinMapResult(NewJoinMapBuildErrorResult(baseErr), 42, false, 0, mb)
	wg.Wait()

	var first *JoinMapBuildError
	for i := range results {
		require.NoError(t, errs[i])
		require.True(t, results[i].IsBuildError())
		require.Nil(t, results[i].JoinMap(), "failed dependency must not expose a successful map")
		if first == nil {
			first = results[i].BuildError()
		} else {
			require.Same(t, first, results[i].BuildError())
		}
		require.Equal(t, baseErr.ErrorCode(), results[i].BuildError().ErrorCode())
	}
}

func TestFinalizeRuntimeFilterOnBuildErrorPasses(t *testing.T) {
	mb := NewMessageBoard()
	spec := &plan.RuntimeFilterSpec{Tag: 99}
	FinalizeRuntimeFilterOnBuildError(spec, mb)

	r := NewMessageReceiver([]int32{spec.Tag}, AddrBroadCastOnCurrentCN(), mb)
	msgs, done, err := r.ReceiveMessage(false, context.Background())
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, msgs, 1)
	rt, ok := msgs[0].(RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(RuntimeFilter_PASS), rt.Typ)
}
