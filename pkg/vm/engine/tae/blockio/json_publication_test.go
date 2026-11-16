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

package blockio

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestJSONStoragePublicationMapsDeletedRows(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	typ := types.T_json.ToType()
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(typ)
	defer input.Clean(mp)
	for _, text := range []string{`{"keep":1}`, `{"keep":2}`} {
		value, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(input.Vecs[0], value, false, mp))
	}
	input.SetRowCount(2)
	writer := ioutil.ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	for _, deleted := range []bool{false, true} {
		func() {
			output := batch.NewWithSize(1)
			output.Vecs[0] = vector.NewOffHeapVecWithType(typ)
			defer output.Clean(mp)
			publication := objectio.NewJSONReadSet([]int{0})
			defer publication.Close()
			publication.BindColumns([]uint16{0})
			readCtx := objectio.WithJSONReadSet(ctx, publication)
			source := &blockReadTestDataSource{}
			if deleted {
				source.deleted = []uint64{1}
			}
			cached := containers.NewVectors(2)
			defer cached.Free(mp)
			require.NoError(t, BlockDataReadInner(readCtx, &info, source, []uint16{0}, []types.Type{typ}, -1, types.TS{}, nil, nil, 0, output, cached, mp, fs))
			if deleted {
				require.NotNil(t, publication.Readers()[0])
				require.Equal(t, 1, publication.Readers()[0].Length())
				require.Equal(t, 1, output.Vecs[0].Length())
				path, err := types.ParseStringToPath("$.keep")
				require.NoError(t, err)
				result, err := publication.Readers()[0].Query(0, &path)
				require.NoError(t, err)
				text, err := result.Text()
				result.Close()
				require.NoError(t, err)
				require.Equal(t, "1", text.Text)
				text.Close()
			} else {
				require.NotNil(t, publication.Readers()[0])
				path, err := types.ParseStringToPath("$.keep")
				require.NoError(t, err)
				result, err := publication.Readers()[0].Query(1, &path)
				require.NoError(t, err)
				text, err := result.Text()
				result.Close()
				require.NoError(t, err)
				require.Equal(t, "2", text.Text)
				text.Close()
			}
		}()
	}
}

func TestScopedJSONPublicationRejectsMalformedDescendant(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	typ := types.T_json.ToType()
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(typ)
	input.Vecs[1] = vector.NewVec(types.T_bool.ToType())
	defer input.Clean(mp)
	for _, value := range []string{`{"keep":1}`, `{"keep":2,"hidden":"unselected-descendant"}`} {
		jsonValue, err := types.ParseStringToByteJson(value)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(input.Vecs[0], jsonValue, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], true, false, mp))
	}
	input.SetRowCount(2)
	payload := input.Vecs[0].GetBytesAt(1)
	pos := bytes.Index(payload, []byte("unselected-descendant"))
	require.GreaterOrEqual(t, pos, 0)
	payload[pos] = 0xff
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	output := batch.NewOffHeapWithSize(2)
	output.Vecs[0] = vector.NewOffHeapVecWithType(typ)
	output.Vecs[1] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
	require.NoError(t, output.SetAllocationAccount(allocation))
	publication := objectio.NewJSONReadSet([]int{0}, allocation)
	publication.BindColumns([]uint16{0, 1})
	cached := containers.NewVectors(2)
	defer func() {
		publication.Close()
		output.Clean(mp)
		cached.Free(mp)
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	}()
	filter := &engine.ReaderFilterRequest{
		OutputAllocation: allocation,
		ColumnUses:       []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnOwned},
		Select: func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
			return engine.ReaderFilterResult{Sels: []int64{1}}, nil
		},
	}
	readCtx := objectio.WithJSONReadSet(ctx, publication)
	err = blockDataReadWithFilter(readCtx, &info, &blockReadTestDataSource{}, []uint16{0, 1}, []types.Type{typ, types.T_bool.ToType()}, -1, types.TS{}, nil, 0, output, cached, mp, fs, []int{1}, filter, nil, nil)
	require.ErrorContains(t, err, "invalid JSON vector payload", "a malformed unselected JSON field must be rejected before publication")
	require.Empty(t, publication.Readers())
}

func TestJSONStoragePublicationComposesLateFilterRows(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	typ := types.T_json.ToType()
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(typ)
	input.Vecs[1] = vector.NewVec(types.T_bool.ToType())
	defer input.Clean(mp)
	for _, text := range []string{`{"keep":1}`, `{"keep":2}`, `{"keep":3}`} {
		value, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(input.Vecs[0], value, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], true, false, mp))
	}
	input.SetRowCount(3)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	defer func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	}()
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	for _, selectedOwned := range []bool{false, true} {
		for _, earlyColumn := range []int{0, 1} {
			func() {
				output := batch.NewOffHeapWithSize(2)
				output.Vecs[0] = vector.NewOffHeapVecWithType(typ)
				output.Vecs[1] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
				defer output.Clean(mp)
				if selectedOwned {
					require.NoError(t, output.SetAllocationAccount(allocation))
				}
				publication := objectio.NewJSONReadSet([]int{0})
				defer publication.Close()
				publication.BindColumns([]uint16{0, 1})
				readCtx := objectio.WithJSONReadSet(ctx, publication)
				source := &blockReadTestDataSource{deleted: []uint64{0}}
				if selectedOwned {
					source.deleted = nil
				}
				cached := containers.NewVectors(2)
				defer cached.Free(mp)
				filter := &engine.ReaderFilterRequest{Apply: func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
					require.Equal(t, 2, bat.RowCount())
					for _, column := range loaded {
						if err := bat.Vecs[column].Shrink([]int64{1}, false, mp); err != nil {
							return engine.ReaderFilterResult{}, err
						}
					}
					bat.SetRowCount(1)
					return engine.ReaderFilterResult{Sels: []int64{1}}, nil
				}}
				if selectedOwned {
					filter.OutputAllocation = allocation
					filter.ColumnUses = []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnOwned}
					filter.Select = func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
						require.Equal(t, 3, bat.RowCount())
						return engine.ReaderFilterResult{Sels: []int64{2}}, nil
					}
				}
				require.NoError(t, blockDataReadWithFilter(readCtx, &info, source, []uint16{0, 1}, []types.Type{typ, types.T_bool.ToType()}, -1, types.TS{}, nil, 0, output, cached, mp, fs, []int{earlyColumn}, filter, nil, nil))
				version := publication.Readers()[0]
				require.NotNil(t, version)
				require.Equal(t, 1, version.Length())
				path, err := types.ParseStringToPath("$.keep")
				require.NoError(t, err)
				result, err := version.Query(0, &path)
				require.NoError(t, err)
				text, err := result.Text()
				result.Close()
				require.NoError(t, err)
				require.Equal(t, "3", text.Text)
				text.Close()
			}()
		}
	}

	for _, tc := range []struct {
		name      string
		selection engine.ReaderFilterResult
		selectErr error
		want      []string
	}{
		{"scoped predicate publishes all JSON rows", engine.ReaderFilterResult{All: true}, nil, []string{"1", "2", "3"}},
		{"scoped predicate publishes selected JSON rows", engine.ReaderFilterResult{Sels: []int64{0, 2}}, nil, []string{"1", "3"}},
		{"scoped predicate publishes no JSON rows", engine.ReaderFilterResult{}, nil, nil},
		{"scoped predicate propagates cancellation", engine.ReaderFilterResult{}, context.Canceled, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := batch.NewOffHeapWithSize(2)
			output.Vecs[0] = vector.NewOffHeapVecWithType(typ)
			output.Vecs[1] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
			require.NoError(t, output.SetAllocationAccount(allocation))
			publication := objectio.NewJSONReadSet([]int{0})
			publication.BindColumns([]uint16{0, 1})
			cached := containers.NewVectors(2)
			defer func() {
				publication.Close()
				output.Clean(mp)
				cached.Free(mp)
				require.Zero(t, account.Snapshot().Used)
			}()
			observations := 0
			filter := &engine.ReaderFilterRequest{
				OutputAllocation: allocation,
				ColumnUses:       []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnOwned},
				Select: func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
					require.Equal(t, []int{1}, loaded)
					require.Equal(t, 3, bat.RowCount())
					return tc.selection, tc.selectErr
				},
				ObserveSelection: func(rows, columns int, logicalBytes int64) {
					observations++
					require.Equal(t, len(tc.want), rows)
				},
			}
			readCtx := objectio.WithJSONReadSet(ctx, publication)
			err := blockDataReadWithFilter(readCtx, &info, &blockReadTestDataSource{}, []uint16{0, 1}, []types.Type{typ, types.T_bool.ToType()}, -1, types.TS{}, nil, 0, output, cached, mp, fs, []int{1}, filter, nil, nil)
			if tc.selectErr != nil {
				require.ErrorIs(t, err, tc.selectErr)
				require.Empty(t, publication.Readers())
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, observations, "the scoped reader observes even an all-row selection")
			require.Equal(t, len(tc.want), output.RowCount())
			if len(tc.want) == 0 {
				require.Empty(t, publication.Readers())
				return
			}
			reader := publication.Readers()[0]
			require.NotNil(t, reader)
			require.Equal(t, len(tc.want), reader.Length())
			path, err := types.ParseStringToPath("$.keep")
			require.NoError(t, err)
			for row, want := range tc.want {
				result, err := reader.Query(row, &path)
				require.NoError(t, err)
				text, err := result.Text()
				result.Close()
				require.NoError(t, err)
				if row == len(tc.want)-1 {
					publication.Close()
					output.Clean(mp)
				}
				require.Equal(t, want, text.Text)
				text.Close()
			}
		})
	}

	t.Run("scoped predicate propagates budget failure", func(t *testing.T) {
		registry, err := mpool.NewAllocationAccountRegistry(1, 64)
		require.NoError(t, err)
		account, err := registry.Open(1)
		require.NoError(t, err)
		allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
		require.NoError(t, err)
		output := batch.NewOffHeapWithSize(2)
		output.Vecs[0] = vector.NewOffHeapVecWithType(typ)
		output.Vecs[1] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
		require.NoError(t, output.SetAllocationAccount(allocation))
		publication := objectio.NewJSONReadSet([]int{0}, allocation)
		publication.BindColumns([]uint16{0, 1})
		cached := containers.NewVectors(2)
		defer func() {
			publication.Close()
			output.Clean(mp)
			cached.Free(mp)
			require.Zero(t, account.Snapshot().Used)
			_, _, err := registry.CompleteTerminal(account)
			require.NoError(t, err)
		}()
		filter := &engine.ReaderFilterRequest{
			OutputAllocation: allocation,
			ColumnUses:       []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnOwned},
			Select: func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
				return engine.ReaderFilterResult{Sels: []int64{2}}, nil
			},
		}
		readCtx := objectio.WithJSONReadSet(ctx, publication)
		err = blockDataReadWithFilter(readCtx, &info, &blockReadTestDataSource{}, []uint16{0, 1}, []types.Type{typ, types.T_bool.ToType()}, -1, types.TS{}, nil, 0, output, cached, mp, fs, []int{1}, filter, nil, nil)
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		require.Empty(t, publication.Readers())
	})

	t.Run("published row mappings retain the existing path", func(t *testing.T) {
		publication := objectio.NewJSONReadSet([]int{0})
		defer publication.Close()
		publication.BindColumns([]uint16{0, 1})
		require.False(t, scopedJSONPublicationSafe(publication, []types.Type{typ, types.T_bool.ToType()}, []int{0}))
		reader, err := vector.NewJSONReaderSnapshot(input.Vecs[0], mp)
		require.NoError(t, err)
		publication.Publish(0, reader)
		require.False(t, scopedJSONPublicationSafe(publication, []types.Type{typ, types.T_bool.ToType()}, []int{1}))
		require.Equal(t, 3, publication.Readers()[0].Length())
	})
}
