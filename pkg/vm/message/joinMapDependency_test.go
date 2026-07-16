package message

import (
	"context"
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
