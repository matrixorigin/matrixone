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

package ioutil

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpillBoundedRunsReadBack(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, sortKey := range []int{0, -1} {
			for _, shape := range []string{"disjoint", "constant", "constant_short", "aliased"} {
				t.Run(fmt.Sprintf("pipeline=%t/sortKey=%d/%s", pipeline, sortKey, shape), func(t *testing.T) {
					ctx := context.Background()
					fs := testutil.NewSharedFS()
					t.Cleanup(func() { fs.Close(ctx) })
					mp := mpool.MustNewZero()
					attrs := []string{"id", "payload"}
					typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
					input := batch.NewWithSchema(false, attrs, typs)
					defer input.Clean(mp)
					const rows = 35
					payload := strings.Repeat("x", 200)
					valueForID := func(id int32) (string, bool) {
						if shape != "disjoint" {
							return payload, false
						}
						if id%11 == 0 {
							return "", true
						}
						if id%7 == 0 {
							return fmt.Sprintf("%d", id), false
						}
						return fmt.Sprintf("%d:%s", id, payload), false
					}
					for i := 0; i < rows; i++ {
						id := int32(rows - 1 - i)
						require.NoError(t, vector.AppendFixed(input.Vecs[0], id, false, mp))
						if shape == "disjoint" {
							value, isNull := valueForID(id)
							require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(value), isNull, mp))
						}
					}
					if shape != "disjoint" {
						constRows := rows
						if shape == "constant_short" {
							constRows = 1
						}
						constant, err := vector.NewConstBytes(typs[1], []byte(payload), constRows, mp)
						require.NoError(t, err)
						if shape == "constant" || shape == "constant_short" {
							input.Vecs[1].Free(mp)
							input.Vecs[1] = constant
						} else {
							require.NoError(t, input.Vecs[1].UnionBatch(constant, 0, rows, nil, mp))
							constant.Free(mp)
							require.False(t, input.Vecs[1].VarlenaAreaIsDisjoint())
						}
					}
					input.SetRowCount(rows)
					const limit = 1024
					opts := []SinkerOption{WithMemorySizeThreshold(limit), WithBoundedRuns(), WithTailSizeCap(0)}
					if pipeline {
						opts = append(opts, WithPipelineFlush())
					}
					sink := NewSinker(sortKey, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, sortKey, sortKey >= 0, false, 0), mp, fs, opts...)
					t.Cleanup(func() { require.NoError(t, sink.Close()) })
					require.NoError(t, sink.Write(ctx, input))
					require.NoError(t, sink.Sync(ctx))
					stats, tail := sink.GetResult()
					require.Empty(t, tail)
					require.Greater(t, len(stats), 1)
					seen := make(map[int32]bool)
					for _, stat := range stats {
						objectBytes := 0
						objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
							loc := objectio.Location(info.MetaLoc[:])
							reader, err := NewObjectReader(fs, loc)
							require.NoError(t, err)
							got, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
							require.NoError(t, err)
							defer release()
							require.Equal(t, int(loc.Rows()), got.RowCount(), "stats-only block row count must match persisted block")
							ids := vector.MustFixedColNoTypeCheck[int32](got.Vecs[0])
							for i, id := range ids {
								objectBytes += typs[0].TypeSize() + typs[1].TypeSize()
								if i > 0 && sortKey >= 0 {
									require.Less(t, ids[i-1], id)
								}
								require.False(t, seen[id], "duplicate row %d", id)
								seen[id] = true
								want, isNull := valueForID(id)
								require.Equal(t, isNull, got.Vecs[1].GetNulls().Contains(uint64(i)))
								if !isNull {
									require.Equal(t, want, string(got.Vecs[1].GetBytesAt(i)))
									if len(want) > types.VarlenaInlineSize {
										objectBytes += len(want)
									}
								}
							}
							return true
						}, stat)
						require.LessOrEqual(t, objectBytes, limit, "each complete object must fit the run budget")
					}
					require.Len(t, seen, rows)
					for i := int32(0); i < rows; i++ {
						require.True(t, seen[i])
					}
				})
			}
		}
	}
}

func TestSortedRunSizeAccountsForAliasesAndDeadArea(t *testing.T) {
	mp := mpool.MustNewZero()
	attrs := []string{"id", "dead", "alias"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(false, attrs, typs)
	defer input.Clean(mp)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(0), false, mp))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(1), false, mp))
	require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("d", 1000)), false, mp))
	require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("short"), false, mp))
	require.NoError(t, vector.SetBytesAt(input.Vecs[1], 0, []byte("short"), mp))
	require.True(t, input.Vecs[1].VarlenaAreaIsDisjoint())
	require.Equal(t, 1000, len(input.Vecs[1].GetArea()))
	alias, err := vector.NewConstBytes(typs[2], []byte(strings.Repeat("a", 700)), 2, mp)
	require.NoError(t, err)
	defer alias.Free(mp)
	require.NoError(t, input.Vecs[2].UnionBatch(alias, 0, 2, nil, mp))
	input.SetRowCount(2)
	descriptors := 2 * (typs[0].TypeSize() + typs[1].TypeSize() + typs[2].TypeSize())
	require.Equal(t, descriptors+1000+1400, materializedBatchSize(input))
	require.NoError(t, input.Vecs[1].Shuffle([]int64{1, 0}, mp))
	require.NoError(t, input.Vecs[2].Shuffle([]int64{1, 0}, mp))
	require.Equal(t, descriptors+1000+1400, materializedBatchSize(input), "sorting cannot change accounting")
}

func TestSortedRunAvoidsCopyingDeadArea(t *testing.T) {
	for _, rows := range []int{1, 2} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			mp := mpool.MustNewZero()
			attrs := []string{"id", "payload"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			input := batch.NewWithSchema(false, attrs, typs)
			defer input.Clean(mp)
			for i := 0; i < rows; i++ {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
				require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 4096)), false, mp))
				require.NoError(t, vector.SetBytesAt(input.Vecs[1], i, []byte("ok"), mp))
			}
			input.SetRowCount(rows)
			sink := NewSinker(0, attrs, typs, nil, mp, nil, WithMemorySizeThreshold(256), WithBoundedRuns())
			t.Cleanup(func() { require.NoError(t, sink.Close()) })
			require.NoError(t, sink.Write(context.Background(), input))
			require.Len(t, sink.staged.inMemory, 1)
			got := sink.staged.inMemory[0]
			require.Equal(t, rows, got.RowCount())
			for i := 0; i < rows; i++ {
				require.Equal(t, "ok", string(got.Vecs[1].GetBytesAt(i)))
			}
			require.Empty(t, got.Vecs[1].GetArea(), "short values must not copy unused input area")
			require.Less(t, sink.StagedSize(), 256)
		})
	}
}

func TestRunKeepsFullBlocksBeforeLastPartial(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, sortKey := range []int{0, -1} {
			for _, payloadSize := range []int{100, 300 << 10} {
				t.Run(fmt.Sprintf("pipeline=%t/sortKey=%d/payload=%d", pipeline, sortKey, payloadSize), func(t *testing.T) {
					ctx := context.Background()
					fs := testutil.NewSharedFS()
					t.Cleanup(func() { fs.Close(ctx) })
					mp := mpool.MustNewZero()
					attrs := []string{"id", "payload"}
					typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
					input := batch.NewWithSchema(false, attrs, typs)
					defer input.Clean(mp)
					for i := 0; i < objectio.BlockMaxRows; i++ {
						require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
						require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("x"), false, mp))
					}
					require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(objectio.BlockMaxRows), false, mp))
					payload := strings.Repeat("z", payloadSize)
					require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(payload), false, mp))
					input.SetRowCount(objectio.BlockMaxRows + 1)
					opts := []SinkerOption{WithMemorySizeThreshold(256 << 10), WithBoundedRuns(), WithTailSizeCap(0)}
					if pipeline {
						opts = append(opts, WithPipelineFlush())
					}
					sink := NewSinker(sortKey, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, sortKey, sortKey >= 0, false, 0), mp, fs, opts...)
					t.Cleanup(func() { require.NoError(t, sink.Close()) })
					require.NoError(t, sink.Write(ctx, input))
					require.NoError(t, sink.Sync(ctx))
					stats, _ := sink.GetResult()
					if payloadSize > 256<<10 {
						require.Len(t, stats, 2)
						for _, stat := range stats {
							require.Equal(t, uint32(1), stat.BlkCnt())
						}
					} else {
						require.Len(t, stats, 1)
						require.Equal(t, uint32(2), stats[0].BlkCnt())
					}
					var blockRows []int
					foundLast := false
					objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
						loc := objectio.Location(info.MetaLoc[:])
						reader, err := NewObjectReader(fs, loc)
						require.NoError(t, err)
						got, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
						require.NoError(t, err)
						defer release()
						require.Equal(t, int(loc.Rows()), got.RowCount())
						blockRows = append(blockRows, got.RowCount())
						for i, id := range vector.MustFixedColNoTypeCheck[int32](got.Vecs[0]) {
							if id == int32(objectio.BlockMaxRows) {
								foundLast = true
								require.Equal(t, payload, string(got.Vecs[1].GetBytesAt(i)))
							}
						}
						return true
					}, stats...)
					require.True(t, foundLast)
					require.ElementsMatch(t, []int{objectio.BlockMaxRows, 1}, blockRows)
				})
			}
		}
	}
}

func TestBoundedRunMergesDisjointAndOverlappingBlocks(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, disjoint := range []bool{false, true} {
			t.Run(fmt.Sprintf("pipeline=%v/disjoint=%v", pipeline, disjoint), func(t *testing.T) {
				ctx := context.Background()
				fs := testutil.NewSharedFS()
				t.Cleanup(func() { fs.Close(ctx) })
				mp := mpool.MustNewZero()
				attrs := []string{"id", "value"}
				typs := []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}
				opts := []SinkerOption{WithMemorySizeThreshold(1 << 20), WithTailSizeCap(0), WithBoundedRuns()}
				if pipeline {
					opts = append(opts, WithPipelineFlush())
				}
				sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs, opts...)
				t.Cleanup(func() { require.NoError(t, sink.Close()) })
				for part := 0; part < 2; part++ {
					func() {
						input := batch.NewWithSchema(false, attrs, typs)
						defer input.Clean(mp)
						for row := 0; row < objectio.BlockMaxRows; row++ {
							id := int32((objectio.BlockMaxRows-1-row)*2 + part)
							if disjoint {
								id = int32(part*objectio.BlockMaxRows + objectio.BlockMaxRows - 1 - row)
							}
							require.NoError(t, vector.AppendFixed(input.Vecs[0], id, false, mp))
							require.NoError(t, vector.AppendFixed(input.Vecs[1], id*10, false, mp))
						}
						input.SetRowCount(objectio.BlockMaxRows)
						require.NoError(t, sink.Write(ctx, input))
					}()
				}
				require.NoError(t, sink.Sync(ctx))
				stats, _ := sink.GetResult()
				require.Len(t, stats, 1)
				require.Equal(t, uint32(2), stats[0].BlkCnt())
				next := int32(0)
				objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
					loc := objectio.Location(info.MetaLoc[:])
					reader, err := NewObjectReader(fs, loc)
					require.NoError(t, err)
					got, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
					require.NoError(t, err)
					defer release()
					require.Equal(t, objectio.BlockMaxRows, got.RowCount())
					values := vector.MustFixedColNoTypeCheck[int32](got.Vecs[1])
					for row, id := range vector.MustFixedColNoTypeCheck[int32](got.Vecs[0]) {
						require.Equal(t, next, id)
						require.Equal(t, id*10, values[row])
						next++
					}
					return true
				}, stats...)
				require.Equal(t, int32(2*objectio.BlockMaxRows), next)
			})
		}
	}
}

func TestRunByteArithmeticRejectsOverflow(t *testing.T) {
	require.Equal(t, math.MaxInt, addRunBytes(math.MaxInt-1, 2))
	require.Equal(t, math.MaxInt, multiplyRunBytes(math.MaxInt, 2))
	require.Equal(t, math.MaxInt, addRunBytes(1, -1))
	require.Equal(t, math.MaxInt, multiplyRunBytes(1, -1))
}

func TestBoundedRunCopiesBorrowedWindowWithoutParentArea(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	attrs := []string{"id", "value"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	parent := batch.NewWithSchema(false, attrs, typs)
	defer func() {
		if parent != nil {
			parent.Clean(mp)
		}
	}()
	for i, value := range []string{strings.Repeat("a", 4096), "one", "two", strings.Repeat("b", 4096)} {
		require.NoError(t, vector.AppendFixed(parent.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(parent.Vecs[1], []byte(value), false, mp))
	}
	parent.SetRowCount(4)
	window, err := parent.Window(1, 3)
	require.NoError(t, err)
	defer func() {
		if window != nil {
			window.Clean(nil)
		}
	}()
	sink := NewSinker(-1, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, -1, false, false, 0), mp, fs,
		WithMemorySizeThreshold(256), WithTailSizeCap(0), WithBoundedRuns())
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	require.NoError(t, sink.Write(ctx, window))
	require.Len(t, sink.GetInMemoryData(), 1)
	require.Empty(t, sink.GetInMemoryData()[0].Vecs[1].GetArea())
	window.Clean(nil)
	window = nil
	parent.Clean(mp)
	parent = nil
	require.NoError(t, sink.Sync(ctx))
	stats, _ := sink.GetResult()
	require.Len(t, stats, 1)
	objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
		loc := objectio.Location(info.MetaLoc[:])
		reader, err := NewObjectReader(fs, loc)
		require.NoError(t, err)
		got, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
		require.NoError(t, err)
		defer release()
		require.Equal(t, []int32{1, 2}, vector.MustFixedColNoTypeCheck[int32](got.Vecs[0]))
		require.Equal(t, "one", string(got.Vecs[1].GetBytesAt(0)))
		require.Equal(t, "two", string(got.Vecs[1].GetBytesAt(1)))
		return true
	}, stats...)
}
