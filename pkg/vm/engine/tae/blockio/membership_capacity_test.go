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
	// The selected two-row map consumes the available capacity; nullable view
	// metadata is optional, while a temporary wide-value copy cannot fit.
	account, err := registry.Open(16)
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
		require.Zero(t, account.Snapshot().Used, "identity inputs need no row map")
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

func TestMembershipScratchDoesNotStarveOutput(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	typs := []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}
	input := batch.NewWithSchema(false, nil, typs)
	t.Cleanup(func() { input.Clean(mp) })
	for row := range 8 {
		for col, v := range input.Vecs {
			require.NoError(t, vector.AppendFixed(v, int32(row+100*col), false, mp))
		}
	}
	input.SetRowCount(8)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	for _, tc := range []struct {
		name           string
		limit          uint64
		selective      bool
		bypass         bool
		predicateError bool
		ordinary       bool
		tombstone      bool
		exact          bool
		retained       bool
		sourceError    bool
	}{
		{name: "identity", limit: 128},
		{name: "selective output fits", limit: 128, selective: true},
		{name: "early output pressure", limit: 64, selective: true, bypass: true},
		{name: "late output pressure", limit: 96, selective: true, bypass: true},
		{name: "callback capacity error", limit: 128, predicateError: true},
		{name: "generic ordinary fallback", limit: 128, ordinary: true},
		{name: "tombstoned ordinary fallback", limit: 128, ordinary: true, tombstone: true},
		{name: "native exact ordinary fallback", limit: 128, ordinary: true, exact: true},
		{name: "ordinary releases retained scratch", limit: 128, ordinary: true, retained: true},
		{name: "ordinary callback capacity error", limit: 128, ordinary: true, predicateError: true},
		{name: "ordinary source error", limit: 128, ordinary: true, sourceError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, enabled := range []bool{false, true} {
				registry, err := mpool.NewAllocationAccountRegistry(1, 128)
				require.NoError(t, err)
				account, err := registry.Open(tc.limit)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.Zero(t, account.Snapshot().Used)
					_, _, err := registry.CompleteTerminal(account)
					require.NoError(t, err)
				})
				allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
				require.NoError(t, err)
				output := batch.NewWithSchema(true, nil, typs)
				t.Cleanup(func() { output.Clean(mp) })
				require.NoError(t, output.SetAllocationAccount(allocation))
				original := append([]*vector.Vector(nil), output.Vecs...)
				request := &engine.ReaderFilterRequest{
					OutputAllocation: allocation,
					ColumnUses:       []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnOwned},
				}
				if tc.exact {
					request.Exact = objectio.NewExactScanState(objectio.NewExactScanFilter([]objectio.ExactScanPredicate{{Column: 0, Type: typs[0], Op: objectio.ExactScanGE, Value: 0}}))
					t.Cleanup(request.Exact.Close)
				}
				request.Apply = func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
					t.Fatal("scoped reads must use the borrowed Select route")
					return engine.ReaderFilterResult{}, nil
				}
				cache := containers.NewVectors(4)
				t.Cleanup(func() { request.FreeMembership(mp); cache.Free(mp) })
				memberCalls, predicateCalls := 0, 0
				var memberUsed uint64
				request.Select = func(bat *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
					predicateCalls++
					if tc.predicateError && enabled {
						return engine.ReaderFilterResult{}, mpool.ErrAllocationAccountCapacity
					}
					return engine.ReaderFilterResult{All: true}, nil
				}
				if enabled {
					request.Membership = &engine.ReaderMembershipFilter{
						Columns: []int{0}, PredicateColumns: []int{0}, PredicatesTotal: !tc.ordinary,
						Select: func(bat *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
							memberCalls++
							memberUsed = account.Snapshot().Used
							if tc.selective {
								return engine.ReaderFilterResult{Sels: []int64{0, 1, 2, 3, 4, 5, 6}}, nil
							}
							return engine.ReaderFilterResult{All: true}, nil
						},
					}
					if tc.retained {
						scratch, err := request.MembershipScratch(len(typs))
						require.NoError(t, err)
						require.NoError(t, scratch.Physical.PreExtend(16, mp))
					}
				}
				ds := &blockReadTestDataSource{}
				if tc.sourceError {
					ds.err = context.Canceled
				}
				if tc.tombstone {
					ds.deleted = []uint64{6}
				}
				_, err = BlockDataReadWithFilter(ctx, &info, ds, []uint16{0, 1}, typs, -1,
					timestamp.Timestamp{}, nil, nil, objectio.BlockReadFilter{}, nil, fileservice.Policy(0), "membership-output-capacity", output, cache, mp, fs, []int{0}, request)
				if tc.sourceError {
					require.ErrorIs(t, err, context.Canceled)
					require.Zero(t, memberCalls)
					require.Zero(t, predicateCalls)
					continue
				}
				if enabled && tc.predicateError {
					require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
					require.Equal(t, 1, predicateCalls)
					require.False(t, request.Membership.Bypass)
					continue
				}
				require.NoError(t, err, "membership=%v", enabled)
				wantRows, wantCalls := 8, 1
				if tc.tombstone {
					wantRows--
				}
				if tc.exact {
					wantCalls = 0
				}
				if enabled {
					wantMember := 1
					if tc.ordinary {
						wantMember = 0
					}
					require.Equal(t, wantMember, memberCalls)
					require.Zero(t, memberUsed, "identity inputs must not reserve optional row maps")
					require.Equal(t, tc.bypass, request.Membership.Bypass)
					if tc.bypass {
						wantCalls++
					} else if tc.selective {
						wantRows--
					}
				}
				require.Equal(t, wantCalls, predicateCalls)
				require.Equal(t, wantRows, output.RowCount())
				for col, v := range output.Vecs {
					require.Same(t, original[col], v)
					require.False(t, v.HasBorrowedBacking())
					for row := range wantRows {
						physical := row
						if tc.tombstone && row >= 6 {
							physical++
						}
						require.Equal(t, int32(physical+100*col), vector.GetFixedAtNoTypeCheck[int32](v, row))
					}
				}
			}
		})
	}
}
