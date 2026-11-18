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

package blockio

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestMembershipMetadataRefusalRunsBorrowedPredicate(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	types_ := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(mp) })
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewVec(types_[i])
	}
	for row := range 8 {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(row), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 4096)), row == 6, mp))
	}
	input.SetRowCount(8)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	// Two eight-row maps consume the available capacity; nullable view
	// metadata is optional, while a temporary wide-value copy cannot fit.
	account, err := registry.Open(128)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	output := batch.NewOffHeapWithSize(2)
	require.NoError(t, output.SetAllocationAccount(allocation))
	for i := range output.Vecs {
		output.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(types_[i], allocation)
		require.NoError(t, err)
	}
	original := append([]*vector.Vector(nil), output.Vecs...)
	request := &engine.ReaderFilterRequest{
		OutputAllocation: allocation,
		ColumnUses:       []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnOwned},
		Membership: &engine.ReaderMembershipFilter{
			Columns: []int{0}, PredicateColumns: []int{1}, PredicatesTotal: true,
			ColumnUses: []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnUnused},
		},
	}
	request.Apply = func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
		t.Fatal("borrowed selection must not use the mutating Apply callback")
		return engine.ReaderFilterResult{}, nil
	}
	cache := containers.NewVectors(4)
	t.Cleanup(func() { request.FreeMembership(mp); output.Clean(mp); cache.Free(mp) })
	memberCalls, predicateCalls := 0, 0
	request.Membership.Select = func(bat *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
		memberCalls++
		require.Equal(t, 8, bat.RowCount())
		require.EqualValues(t, 128, account.Snapshot().Used)
		return engine.ReaderFilterResult{Sels: []int64{0, 6}}, nil
	}
	request.Select = func(bat *batch.Batch, columns []int) (engine.ReaderFilterResult, error) {
		predicateCalls++
		require.True(t, request.Membership.Bypass)
		require.Zero(t, account.Snapshot().Used, "optional row maps must be released before predicate evaluation")
		require.Equal(t, []int{1}, columns)
		require.Equal(t, 8, bat.RowCount(), "refusal preserves the ordinary full-domain predicate route")
		require.NotSame(t, original[1], bat.Vecs[1], "evaluate the borrowed cache input, not a copied temporary")
		require.True(t, bat.Vecs[1].IsNull(6))
		return engine.ReaderFilterResult{}, nil
	}
	_, err = BlockDataReadWithFilter(ctx, &info, &blockReadTestDataSource{}, []uint16{0, 1}, types_, -1,
		timestamp.Timestamp{}, nil, nil, objectio.BlockReadFilter{}, nil, fileservice.Policy(0), "membership-capacity", output, cache, mp, fs, []int{0, 1}, request)
	require.NoError(t, err)
	require.Equal(t, 1, memberCalls)
	require.Equal(t, 1, predicateCalls)
	require.Zero(t, output.RowCount())
	for i, v := range output.Vecs {
		require.Same(t, original[i], v)
		require.False(t, v.HasBorrowedBacking())
	}
	require.Equal(t, []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnOwned}, request.ColumnUses)
}
