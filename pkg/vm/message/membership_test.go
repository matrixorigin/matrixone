// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func membershipJoinMap(t *testing.T, mp *mpool.MPool, refs int32) *JoinMap {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	jm := NewJoinMap(GroupSels{}, nil, nil, nil, []*batch.Batch{bat}, mp)
	jm.IncRef(refs)
	t.Cleanup(jm.FreeMemory)
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int32{1, 2}, nil, mp))
	bat.SetRowCount(2)
	return jm
}

func TestMembershipConcurrentConsumers(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
	const workers = 16
	jm := membershipJoinMap(t, mp, workers)
	base := mp.AllocatedBytes()
	indexes := make([]*hashmap.MembershipIndex, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			indexes[i], errs[i] = jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType())
		}()
	}
	close(start)
	wg.Wait()
	require.NotNil(t, indexes[0])
	for i := range workers {
		require.NoError(t, errs[i])
		require.Same(t, indexes[0], indexes[i])
	}
	require.Equal(t, base+indexes[0].Size(), mp.AllocatedBytes())
	require.Equal(t, indexes[0].Size(), jm.Size())
	matches := make([]bool, workers)
	unknowns := make([]bool, workers)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer jm.Free()
			matches[i], unknowns[i], errs[i] = indexes[i].Probe(context.Background(), jm.GetBatches()[0].Vecs[0], 0)
		}()
	}
	wg.Wait()
	for i := range workers {
		require.NoError(t, errs[i])
		require.True(t, matches[i])
		require.False(t, unknowns[i])
	}
	require.Zero(t, mp.AllocatedBytes())
	require.Zero(t, jm.Size())
	jm.FreeMemory() // subsequent message-board cleanup has nothing left to free
	require.Zero(t, mp.AllocatedBytes())
}

// Stop the elected builder inside NewMembershipIndex, after the descriptor
// has been published but while the JoinMap mutex must no longer be held.
type membershipBarrierContext struct {
	context.Context
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (ctx *membershipBarrierContext) Err() error {
	ctx.calls++
	if ctx.calls == 2 {
		close(ctx.entered)
		<-ctx.release
	}
	return ctx.Context.Err()
}

type membershipWaitContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
}

func (ctx *membershipWaitContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(func() { close(ctx.entered) })
	return err
}

func TestMembershipWaitCancellationAndBuilderFailure(t *testing.T) {
	for _, cancelBuilder := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "builder-cancel"}[cancelBuilder], func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
			jm := membershipJoinMap(t, mp, 1)
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &membershipBarrierContext{Context: base, entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(ctx.release) })
			built := make(chan error, 1)
			done := make(chan struct{})
			t.Cleanup(func() {
				release.Do(func() { close(ctx.release) })
				<-done
			})
			go func() {
				defer close(done)
				_, err := jm.GetMembershipIndex(ctx, 0, types.T_int32.ToType())
				built <- err
			}()
			<-ctx.entered
			waitCtx, stopWaiting := context.WithCancel(context.Background())
			defer stopWaiting()
			waiter := &membershipWaitContext{Context: waitCtx, entered: make(chan struct{})}
			waiting := make(chan error, 1)
			go func() {
				_, err := jm.GetMembershipIndex(waiter, 0, types.T_int32.ToType())
				waiting <- err
			}()
			<-waiter.entered // initial Err() observed nil; exercise the cancellable wait
			stopWaiting()
			require.ErrorIs(t, <-waiting, context.Canceled)
			// A different key cannot start a second index or wait for this one.
			other, err := jm.GetMembershipIndex(context.Background(), 1, types.T_int32.ToType())
			require.NoError(t, err)
			require.Nil(t, other)
			if cancelBuilder {
				cancel()
			}
			release.Do(func() { close(ctx.release) })
			err = <-built
			index, again := jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType())
			if cancelBuilder {
				require.ErrorIs(t, err, context.Canceled)
				require.NoError(t, again, "a live sibling must not inherit the builder's cancellation")
				require.Nil(t, index)
				require.Equal(t, int32(2), vector.GetFixedAtNoTypeCheck[int32](jm.GetBatches()[0].Vecs[0], 1))
				_, ownCancel := jm.GetMembershipIndex(base, 0, types.T_int32.ToType())
				require.ErrorIs(t, ownCancel, context.Canceled)
			} else {
				require.NoError(t, err)
				require.NoError(t, again)
				require.NotNil(t, index)
			}
			jm.Free()
		})
	}
}

func TestMembershipDisabledIndexKeepsBatches(t *testing.T) {
	mp, err := mpool.NewMPool("membership-disabled", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
	jm := membershipJoinMap(t, mp, 1)
	blocker, err := mp.Alloc(1<<20, true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(blocker) })
	index, err := jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType())
	mp.Free(blocker)
	blocker = nil
	require.NoError(t, err)
	require.Nil(t, index)
	index, err = jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType())
	require.NoError(t, err)
	require.Nil(t, index, "do not retry a disabled index in the same generation")
	require.Equal(t, int32(2), vector.GetFixedAtNoTypeCheck[int32](jm.GetBatches()[0].Vecs[0], 1))
	jm.Free()
	fresh := membershipJoinMap(t, mp, 1)
	index, err = fresh.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType())
	require.NoError(t, err)
	require.NotNil(t, index, "a new JoinMap must not inherit the disabled state")
	fresh.Free()
}
