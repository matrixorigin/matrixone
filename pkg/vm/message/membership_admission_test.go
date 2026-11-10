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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestMembershipWorkAdmission(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
	jm := membershipJoinMap(t, mp, 1)
	baseline := mp.AllocatedBytes()
	original := jm.batches
	defer func() { jm.batches = original }()
	jm.batches = []*batch.Batch{nil} // Observing must not even walk RHS metadata.
	for _, work := range []uint64{0, 63} {
		index, bytes, err := jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), work)
		require.NoError(t, err)
		require.Nil(t, index)
		require.Zero(t, bytes)
		require.Nil(t, jm.membership.ready)
		require.Equal(t, baseline, mp.AllocatedBytes())
	}
	_, bytes, err := jm.GetMembershipIndex(context.Background(), 1, types.T_int32.ToType(), ^uint64(0))
	require.NoError(t, err)
	require.Zero(t, bytes)
	require.Equal(t, uint64(63), jm.membership.work)
	jm.batches = original
	index, bytes, err := jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), ^uint64(0))
	require.NoError(t, err)
	require.NotNil(t, index)
	require.Equal(t, uint64(64), jm.membership.work, "saturate without wrapping")
	require.Equal(t, index.Size(), bytes)
	again, bytes, err := jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), 1)
	require.NoError(t, err)
	require.Same(t, index, again)
	require.Zero(t, bytes)
}

func TestMembershipUntrustedCardinalityDisablesOptimization(t *testing.T) {
	for _, rows := range []int64{-1, 0, 1, (64<<20)/16 + 1, 1 << 62} {
		t.Run(strconv.FormatInt(rows, 10), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
			jm := membershipJoinMap(t, mp, 1)
			jm.SetRowCount(rows)
			baseline := mp.AllocatedBytes()
			for range 2 {
				index, bytes, err := jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), ^uint64(0))
				require.NoError(t, err)
				require.Nil(t, index)
				require.Zero(t, bytes)
				require.Equal(t, baseline, mp.AllocatedBytes())
			}
		})
	}
}

func TestMembershipWorkBoundaryAndUnsupportedDomain(t *testing.T) {
	for _, work := range []uint64{64, 65} {
		mp := mpool.MustNewZero()
		t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
		jm := membershipJoinMap(t, mp, 1)
		index, bytes, err := jm.GetMembershipIndex(context.Background(), 0, types.T_varchar.ToType(), ^uint64(0))
		require.NoError(t, err)
		require.Nil(t, index)
		require.Zero(t, bytes)
		require.Zero(t, jm.membership.threshold)
		index, bytes, err = jm.GetMembershipIndex(context.Background(), 0, types.T_int32.ToType(), work)
		require.NoError(t, err)
		require.NotNil(t, index)
		require.Equal(t, int64(32), bytes)
		jm.Free()
		require.Zero(t, jm.membership.work)
		require.Zero(t, jm.Size())
	}
}
