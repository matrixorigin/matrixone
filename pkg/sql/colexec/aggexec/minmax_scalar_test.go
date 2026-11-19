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

package aggexec

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestFixedMinMaxScalarFill(t *testing.T) {
	for _, tc := range []struct {
		name           string
		typ            types.Type
		low, mid, high any
	}{
		{"int64", types.T_int64.ToType(), int64(-3), int64(8), int64(12)},
		{"decimal256", types.New(types.T_decimal256, 65, 0),
			types.Decimal256FromInt64(-3), types.Decimal256FromInt64(8), types.Decimal256FromInt64(12)},
	} {
		for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, id), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
				input := vector.NewVec(tc.typ)
				t.Cleanup(func() { input.Free(mp) })
				for _, value := range []any{tc.mid, tc.low, tc.high, nil} {
					require.NoError(t, vector.AppendAny(input, value, value == nil, mp))
				}
				constant := vector.NewVec(tc.typ)
				t.Cleanup(func() { constant.Free(mp) })
				require.NoError(t, vector.AppendAny(constant, tc.mid, false, mp))
				constant.ToConst()
				constant.SetLength(8)
				null := vector.NewConstNull(tc.typ, 8, mp)
				t.Cleanup(func() { null.Free(mp) })
				scalar := makeMinMaxExec(mp, id, id == AggIdOfMin, tc.typ)
				t.Cleanup(scalar.Free)
				batch := makeMinMaxExec(mp, id, id == AggIdOfMin, tc.typ)
				t.Cleanup(batch.Free)
				require.NoError(t, scalar.GroupGrow(AggBatchSize+1))
				require.NoError(t, batch.GroupGrow(AggBatchSize+1))
				for _, step := range []struct {
					group, row int
					vec        *vector.Vector
				}{
					{-1, 2, input}, {2 * AggBatchSize, 7, null},
					{0, 0, input}, {0, 1, input}, {0, 2, input}, {0, 1, input}, {0, 2, input},
					{AggBatchSize, 7, constant}, {AggBatchSize, 1, input}, {AggBatchSize, 2, input},
					{1, 3, input}, {0, 7, null},
				} {
					require.NoError(t, scalar.Fill(step.group, step.row, []*vector.Vector{step.vec}))
					require.NoError(t, batch.BatchFill(step.row, []uint64{uint64(step.group + 1)}, []*vector.Vector{step.vec}))
					for chunk := range 2 {
						var got, want bytes.Buffer
						require.NoError(t, scalar.SaveIntermediateResultOfChunk(chunk, &got))
						require.NoError(t, batch.SaveIntermediateResultOfChunk(chunk, &want))
						require.Equal(t, want.Bytes(), got.Bytes())
					}
				}
				results, err := scalar.Flush()
				require.NoError(t, err)
				for _, result := range results {
					t.Cleanup(func() { result.Free(mp) })
				}
				require.Len(t, results, 2)
				want := tc.low
				if id == AggIdOfMax {
					want = tc.high
				}
				for _, result := range results {
					require.False(t, result.IsNull(0))
					if tc.typ.Oid == types.T_int64 {
						require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int64](result, 0))
					} else {
						require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal256](result, 0))
					}
					require.True(t, result.HasPrepareParamKind(), "ordinary winners observe None")
					require.Equal(t, vector.PrepareParamNone, result.GetPrepareParamKind())
					require.Nil(t, result.GetPrepareParamKinds())
					require.False(t, result.HasStringSourceMetadata())
				}
				require.True(t, results[0].IsNull(1))
			})
		}
	}
}

func TestFixedMinMaxScalarFillMetadataAdmission(t *testing.T) {
	for _, metadata := range []string{"kind", "source"} {
		t.Run(metadata, func(t *testing.T) {
			f := newFixedMetadataPreflightFixture(t, AggIdOfMin, types.T_int64.ToType())
			batch := newFixedMetadataPreflightFixture(t, AggIdOfMin, types.T_int64.ToType())
			require.NoError(t, f.exec.GroupGrow(2))
			require.NoError(t, batch.exec.GroupGrow(2))
			plain := f.intInput(t, []int64{8, 5, 3}, nil)
			used, calls := f.account.Snapshot().Used, f.controller.calls
			f.controller.failAt = calls + 1
			require.NoError(t, f.exec.PreflightBatchFill(0, []uint64{1}, []*vector.Vector{plain}))
			require.NoError(t, f.exec.Fill(0, 0, []*vector.Vector{plain}))
			require.NoError(t, f.exec.Fill(0, 0, []*vector.Vector{plain}), "ordinary tie")
			require.NoError(t, batch.exec.BatchFill(0, []uint64{1}, []*vector.Vector{plain}))
			require.NoError(t, batch.exec.BatchFill(0, []uint64{1}, []*vector.Vector{plain}))
			require.Equal(t, snapshotFixedMetadataPreflight(batch), snapshotFixedMetadataPreflight(f))
			require.Equal(t, calls, f.controller.calls)
			require.Equal(t, used, f.account.Snapshot().Used)
			f.controller.failAt = 0

			rich := f.intInput(t, []int64{5, 5}, nil)
			if metadata == "kind" {
				rich.SetPrepareParamKind(vector.PrepareParamFloat)
			} else {
				require.NoError(t, rich.SetStringSource(types.StringSourceLiteral))
			}
			// Rich input must use the existing metadata path, including admission
			// before the representative changes. Then an ordinary input must also
			// fall back when only the destination carries rich metadata.
			for _, step := range []struct {
				input *vector.Vector
				row   int
			}{{rich, 0}, {plain, 1}, {plain, 2}} {
				vectors := []*vector.Vector{step.input}
				require.NoError(t, f.exec.PreflightBatchFill(step.row, []uint64{1}, vectors))
				require.NoError(t, batch.exec.PreflightBatchFill(step.row, []uint64{1}, vectors))
				calls = f.controller.calls
				f.controller.failAt = calls + 1
				require.NoError(t, f.exec.Fill(0, step.row, vectors))
				require.Equal(t, calls, f.controller.calls, "Fill cannot allocate after admission")
				require.NoError(t, batch.exec.BatchFill(step.row, []uint64{1}, vectors))
				require.Equal(t, snapshotFixedMetadataPreflight(batch), snapshotFixedMetadataPreflight(f))
				f.controller.failAt = 0
			}
			target := f.base.state[0].vecs[0]
			require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](target, 0))
			require.Equal(t, vector.PrepareParamNone, target.GetPrepareParamKindAt(0))
			require.Equal(t, types.StringSourceExpression, target.GetStringSourceAt(0))

			if metadata == "source" {
				// A skipped NULL still finalizes a retained source reservation.
				require.NoError(t, target.SetStringSource(types.StringSourceLiteral))
				require.NoError(t, target.PreflightSetStringSourceAt(0, types.StringSourceExpression, f.mp))
				target.RetainStringSourcePreflight()
				require.NotNil(t, target.GetStringSources())
				null := f.intInput(t, []int64{0}, []bool{true})
				require.NoError(t, f.exec.Fill(0, 0, []*vector.Vector{null}))
				require.Nil(t, target.GetStringSources())
				require.Equal(t, types.StringSourceLiteral, target.GetStringSourceAt(0))
			}
		})
	}
}

func TestFixedMinMaxScalarMerge(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    types.Type
		values []any
	}{
		{"int64", types.T_int64.ToType(), []any{int64(8), int64(-3), int64(12), nil}},
		{"decimal256", types.New(types.T_decimal256, 65, 0), []any{
			types.Decimal256FromInt64(8), types.Decimal256FromInt64(-3), types.Decimal256FromInt64(12), nil}},
		{"float64", types.T_float64.ToType(), []any{
			math.Copysign(0, -1), float64(0), math.Float64frombits(0x7ff8000000000001), nil,
			math.Float64frombits(0x7ff8000000000002)}},
	} {
		for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, id), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
				input := vector.NewVec(tc.typ)
				t.Cleanup(func() { input.Free(mp) })
				for _, value := range tc.values {
					require.NoError(t, vector.AppendAny(input, value, value == nil, mp))
				}
				makeState := func() AggFuncExec {
					exec := makeMinMaxExec(mp, id, id == AggIdOfMin, tc.typ)
					t.Cleanup(exec.Free)
					return exec
				}
				scalar, batched, source := makeState(), makeState(), makeState()
				for _, exec := range []AggFuncExec{scalar, batched, source} {
					require.NoError(t, exec.GroupGrow(AggBatchSize+1))
				}
				for row := range tc.values {
					require.NoError(t, source.Fill(row, row, []*vector.Vector{input}))
				}
				require.NoError(t, source.Fill(AggBatchSize, 1, []*vector.Vector{input}))
				snapshot := func(exec AggFuncExec, chunks int) []byte {
					var buf bytes.Buffer
					for chunk := range chunks {
						require.NoError(t, exec.SaveIntermediateResultOfChunk(chunk, &buf))
					}
					return buf.Bytes()
				}
				sourceBefore := snapshot(source, 2)
				for _, step := range []struct {
					target, source int
					self           bool
				}{
					{-1, 0, false}, {0, 3, false}, // Skip and NULL.
					{0, 0, false}, {0, 1, false}, {0, 2, false}, {0, 1, false},
					{0, len(tc.values) - 1, false},
					{AggBatchSize, 2, false}, {1, AggBatchSize, false},
					{2, 0, true}, {0, 0, true}, {AggBatchSize, 1, true},
				} {
					scalarSource, batchSource := source, source
					if step.self {
						scalarSource, batchSource = scalar, batched
					}
					require.NoError(t, scalar.Merge(scalarSource, step.target, step.source))
					require.NoError(t, batched.BatchMerge(batchSource, step.source, []uint64{uint64(step.target + 1)}))
					require.Equal(t, snapshot(batched, 2), snapshot(scalar, 2))
				}
				require.Equal(t, sourceBefore, snapshot(source, 2), "merge borrows its source")

				// Spill sources have compact logical rows, not destination-sized
				// arrays. Scalar merge must retain the chunkRows source contract.
				var encoded bytes.Buffer
				require.NoError(t, source.(SpillStateCodec).SaveSpillIntermediateRows(0, []int32{0, 1, 2, 3}, &encoded))
				compact := makeState()
				require.NoError(t, compact.(SpillStateCodec).UnmarshalSpillFromReader(&encoded, mp))
				compactBefore := snapshot(compact, 1)
				require.NoError(t, scalar.Merge(compact, 4, 2))
				require.NoError(t, batched.BatchMerge(compact, 2, []uint64{5}))
				require.Equal(t, snapshot(batched, 2), snapshot(scalar, 2))
				require.Equal(t, compactBefore, snapshot(compact, 1))
			})
		}
	}
}

func TestFixedMinMaxScalarClosesSourceReservation(t *testing.T) {
	for _, op := range []string{"fill", "merge"} {
		t.Run(op, func(t *testing.T) {
			f := newFixedMetadataPreflightFixture(t, AggIdOfMin, types.T_int64.ToType())
			require.NoError(t, f.exec.GroupGrow(2))
			input := f.intInput(t, []int64{7, 0}, []bool{false, true})
			source := f.mergeSource(t, []int64{7, 0}, []bool{false, true})
			for _, collapsed := range []bool{false, true} {
				for _, null := range []bool{false, true} {
					t.Run(fmt.Sprintf("collapsed=%t/null=%t", collapsed, null), func(t *testing.T) {
						target := f.base.state[0].vecs[0]
						require.NoError(t, target.SetStringSource(types.StringSourceLiteral))
						require.NoError(t, target.PreflightSetStringSourceAt(0, types.StringSourceExpression, f.mp))
						target.RetainStringSourcePreflight()
						require.NotNil(t, target.GetStringSources())
						if collapsed {
							// SetStringSource releases the sidecar but does not close
							// its retained preflight flag. The work unit still must.
							require.NoError(t, target.SetStringSource(types.StringSourceExpression))
							require.True(t, hasDefaultFixedMinMaxMetadata(target))
						}
						row := 0
						if null {
							row = 1
						}
						calls := f.controller.calls
						f.controller.failAt = calls + 1
						defer func() { f.controller.failAt = 0 }()
						if op == "fill" {
							require.NoError(t, f.exec.Fill(0, row, []*vector.Vector{input}))
						} else {
							require.NoError(t, f.exec.Merge(source.exec, 0, row))
						}
						require.Equal(t, calls, f.controller.calls, "reserved publication does not allocate")
						require.False(t, f.controller.rejected)
						f.controller.failAt = 0
						if collapsed {
							// Subsequent setters must normalize normally; a stale
							// preflight flag would retain this now-uniform sidecar.
							require.NoError(t, target.SetStringSourceAtWithMP(0, types.StringSourceLiteral, f.mp))
							require.NoError(t, target.SetStringSourceAtWithMP(0, types.StringSourceExpression, f.mp))
							require.Nil(t, target.GetStringSources())
						} else if null {
							require.Nil(t, target.GetStringSources())
							require.Equal(t, types.StringSourceLiteral, target.GetStringSourceAt(0))
						} else {
							require.Equal(t, types.StringSourceExpression, target.GetStringSourceAt(0))
							require.Equal(t, types.StringSourceLiteral, target.GetStringSourceAt(1))
						}
					})
				}
			}
		})
	}
}

func BenchmarkFixedMinMaxScalarFill(b *testing.B) {
	for _, tc := range []struct {
		name  string
		typ   types.Type
		value any
	}{
		{"int64", types.T_int64.ToType(), int64(7)},
		{"decimal256", types.New(types.T_decimal256, 65, 0), types.Decimal256FromInt64(7)},
	} {
		for _, scalar := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/scalar=%t", tc.name, scalar), func(b *testing.B) {
				mp := mpool.MustNewZero()
				b.Cleanup(func() { require.Zero(b, mp.CurrNB()) })
				input := vector.NewVec(tc.typ)
				b.Cleanup(func() { input.Free(mp) })
				require.NoError(b, vector.AppendAny(input, tc.value, false, mp))
				exec := makeMinMaxExec(mp, AggIdOfMin, true, tc.typ)
				b.Cleanup(exec.Free)
				require.NoError(b, exec.GroupGrow(1))
				vectors := []*vector.Vector{input}
				groups := []uint64{1}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					var err error
					if scalar {
						err = exec.Fill(0, 0, vectors)
					} else {
						err = exec.BatchFill(0, groups, vectors)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
