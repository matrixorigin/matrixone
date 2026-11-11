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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
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
	jm.SetRowCount(2)
	return jm
}

func TestMembershipConcurrentConsumers(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
	const workers = 16
	jm := membershipJoinMap(t, mp, workers)
	base := mp.AllocatedBytes()
	indexes := make([]*MembershipIndex, workers)
	charges := make([]int64, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			indexes[i], charges[i], errs[i] = jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), 64)
		}()
	}
	close(start)
	wg.Wait()
	require.NotNil(t, indexes[0])
	var totalCharge int64
	chargedBuilders := 0
	for i := range workers {
		require.NoError(t, errs[i])
		require.Same(t, indexes[0], indexes[i])
		totalCharge += charges[i]
		if charges[i] != 0 {
			require.Equal(t, indexes[0].Size(), charges[i])
			chargedBuilders++
		}
	}
	require.Equal(t, 1, chargedBuilders)
	require.Equal(t, indexes[0].Size(), totalCharge)
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
	waiting chan struct{}
}

func (ctx *membershipWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
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
			var wg sync.WaitGroup
			t.Cleanup(func() {
				release.Do(func() { close(ctx.release) })
				wg.Wait()
			})
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(done)
				_, _, err := jm.GetMembershipIndex(ctx, 0, types.T_int32.ToType(), 64)
				built <- err
			}()
			<-ctx.entered
			waitCtx, stopWaiting := context.WithCancel(context.Background())
			defer stopWaiting()
			waiter := &membershipWaitContext{Context: waitCtx, waiting: make(chan struct{})}
			waiting := make(chan error, 1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, bytes, err := jm.GetMembershipIndex(waiter, 0, types.T_int32.ToType(), 0)
				if bytes != 0 {
					err = moerr.NewInternalErrorNoCtx("waiter charged for shared index")
				}
				waiting <- err
			}()
			<-waiter.waiting // Done is evaluated at the ready/context select, not the entry Err check.
			stopWaiting()
			require.ErrorIs(t, <-waiting, context.Canceled)
			select {
			case <-done:
				t.Fatal("waiter cancellation must return while the builder is still blocked")
			default:
			}
			// A different key cannot start a second index or wait for this one.
			other, bytes, err := jm.GetMembershipIndex(context.Background(), 1, types.T_int32.ToType(), 64)
			require.Zero(t, bytes)
			require.NoError(t, err)
			require.Nil(t, other)
			if cancelBuilder {
				cancel()
			}
			release.Do(func() { close(ctx.release) })
			err = <-built
			index, bytes, again := jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), 0)
			require.Zero(t, bytes)
			if cancelBuilder {
				require.ErrorIs(t, err, context.Canceled)
				require.NoError(t, again, "a live sibling must not inherit the builder's cancellation")
				require.Nil(t, index)
				require.Equal(t, int32(2), vector.GetFixedAtNoTypeCheck[int32](jm.GetBatches()[0].Vecs[0], 1))
				_, _, ownCancel := jm.GetMembershipIndex(base, 0, types.T_int32.ToType(), 0)
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

func TestMembershipNativeAllocationFailurePublishes(t *testing.T) {
	for _, failure := range []string{"oom", "unrelated native failure"} {
		t.Run(failure, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
			jm := membershipJoinMap(t, mp, 2)
			allocated, charged := mp.AllocatedBytes(), mp.CurrNB()
			fault.EnableDomain(fault.DomainTest)
			t.Cleanup(func() { fault.DisableDomain(fault.DomainTest) })
			require.NoError(t, fault.AddFaultPointInDomain(context.Background(), fault.DomainTest,
				"simple_c_allocator_calloc", ":::", "echo", 32, failure, false))
			ctx := &membershipBarrierContext{Context: context.Background(), entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			type outcome struct {
				index *MembershipIndex
				bytes int64
				err   error
			}
			builder, waiter := make(chan outcome, 1), make(chan outcome, 1)
			var wg sync.WaitGroup
			t.Cleanup(func() { release.Do(func() { close(ctx.release) }); wg.Wait() })
			wg.Add(1)
			go func() {
				defer wg.Done()
				index, bytes, err := jm.GetMembershipIndex(ctx, 0, types.T_int32.ToType(), 64)
				builder <- outcome{index, bytes, err}
			}()
			<-ctx.entered
			waitCtx := &membershipWaitContext{Context: context.Background(), waiting: make(chan struct{})}
			wg.Add(1)
			go func() {
				defer wg.Done()
				index, bytes, err := jm.GetMembershipIndex(waitCtx, 0, types.T_int32.ToType(), 0)
				waiter <- outcome{index, bytes, err}
			}()
			<-waitCtx.waiting // waiter reached the ready/context select while the builder is blocked
			release.Do(func() { close(ctx.release) })
			for _, result := range []outcome{<-builder, <-waiter} {
				require.Nil(t, result.index)
				require.Zero(t, result.bytes)
				if failure == "oom" {
					require.NoError(t, result.err, "optional allocator OOM must select the evaluator")
				} else {
					require.True(t, moerr.IsMoErrCode(result.err, moerr.ErrInternal))
					require.ErrorContains(t, result.err, failure)
				}
			}
			select {
			case <-jm.membership.ready:
			default:
				t.Fatal("builder failed without publishing a terminal outcome")
			}
			require.Equal(t, allocated, mp.AllocatedBytes())
			require.Equal(t, charged, mp.CurrNB())
			require.Equal(t, int32(2), vector.GetFixedAtNoTypeCheck[int32](jm.GetBatches()[0].Vecs[0], 1))
			jm.Free()
			jm.Free()
			require.Zero(t, mp.AllocatedBytes())
			fault.DisableDomain(fault.DomainTest)
			fresh := membershipJoinMap(t, mp, 1)
			index, bytes, err := fresh.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), 64)
			require.Equal(t, int64(32), bytes)
			require.NoError(t, err)
			require.NotNil(t, index)
			fresh.Free()
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
	index, bytes, err := jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), 64)
	require.Zero(t, bytes)
	mp.Free(blocker)
	blocker = nil
	require.NoError(t, err)
	require.Nil(t, index)
	index, bytes, err = jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), 64)
	require.Zero(t, bytes)
	require.NoError(t, err)
	require.Nil(t, index, "do not retry a disabled index in the same generation")
	require.Equal(t, int32(2), vector.GetFixedAtNoTypeCheck[int32](jm.GetBatches()[0].Vecs[0], 1))
	jm.Free()
	fresh := membershipJoinMap(t, mp, 1)
	index, bytes, err = fresh.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), 64)
	require.Equal(t, int64(32), bytes)
	require.NoError(t, err)
	require.NotNil(t, index, "a new JoinMap must not inherit the disabled state")
	fresh.Free()
}
