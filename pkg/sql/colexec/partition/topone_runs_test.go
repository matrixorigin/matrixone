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

package partition

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type topOneRunRow struct {
	group                               int32
	score, secondary, id                int64
	groupNull, scoreNull, secondaryNull bool
}

// This oracle deliberately uses stable sorting of Go values, without the
// operator's hash IDs, retained slots, heaps or SQL comparison implementation.
// The source position breaks complete ORDER BY peers, as strict replacement
// must preserve the first peer, including its payload.
func topOneRunOracle(rows []topOneRunRow, desc, nullsLast bool) []topOneRunRow {
	type key struct {
		value int32
		null  bool
	}
	groups := make(map[key][]topOneRunRow)
	for _, row := range rows {
		k := key{row.group, row.groupNull}
		groups[k] = append(groups[k], row)
	}
	want := make([]topOneRunRow, 0, len(groups))
	for _, group := range groups {
		sort.SliceStable(group, func(i, j int) bool {
			a, b := group[i], group[j]
			if cmp := topOneNullableInt(a.score, b.score, a.scoreNull, b.scoreNull, desc, nullsLast); cmp != 0 {
				return cmp < 0
			}
			return topOneNullableInt(a.secondary, b.secondary, a.secondaryNull, b.secondaryNull, !desc, true) < 0
		})
		want = append(want, group[0])
	}
	sort.Slice(want, func(i, j int) bool { return want[i].id < want[j].id })
	return want
}

func topOneNullableInt(a, b int64, an, bn, desc, nullsLast bool) int {
	if an || bn {
		if an == bn {
			return 0
		}
		if an == nullsLast {
			return 1
		}
		return -1
	}
	if a == b {
		return 0
	}
	if (a < b) != desc {
		return -1
	}
	return 1
}

func topOneRunArgument(desc, nullsLast bool) *Partition {
	arg := newTopNArgument(1)
	arg.OrderBySpecs[1].Flag = plan.OrderBySpec_NULLS_FIRST
	if nullsLast {
		arg.OrderBySpecs[1].Flag = plan.OrderBySpec_NULLS_LAST
	}
	if desc {
		arg.OrderBySpecs[1].Flag |= plan.OrderBySpec_DESC
	}
	arg.OrderBySpecs[2].Flag = plan.OrderBySpec_NULLS_LAST
	if !desc {
		arg.OrderBySpecs[2].Flag |= plan.OrderBySpec_DESC
	}
	return arg
}

func topOneRunBatch(t testing.TB, proc *process.Process, rows []topOneRunRow) (result *batch.Batch) {
	t.Helper()
	bat := batch.NewWithSize(4)
	defer func() {
		if result == nil {
			bat.Clean(proc.Mp())
		}
	}()
	for i, typ := range []types.T{types.T_int32, types.T_int64, types.T_int64, types.T_int64} {
		bat.Vecs[i] = vector.NewVec(typ.ToType())
	}
	for _, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row.group, row.groupNull, proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], row.score, row.scoreNull, proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], row.secondary, row.secondaryNull, proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[3], row.id, false, proc.Mp()))
	}
	bat.SetRowCount(len(rows))
	return bat
}

func topOneRunCollect(t testing.TB, arg *Partition, proc *process.Process) []topOneRunRow {
	t.Helper()
	var got []topOneRunRow
	for {
		result, err := arg.Call(proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		bat := result.Batch
		for i := 0; i < bat.RowCount(); i++ {
			row := topOneRunRow{
				group:         vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[0], i),
				score:         vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[1], i),
				secondary:     vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[2], i),
				id:            vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[3], i),
				groupNull:     bat.Vecs[0].IsNull(uint64(i)),
				scoreNull:     bat.Vecs[1].IsNull(uint64(i)),
				secondaryNull: bat.Vecs[2].IsNull(uint64(i)),
			}
			if row.groupNull {
				row.group = 0
			}
			if row.scoreNull {
				row.score = 0
			}
			if row.secondaryNull {
				row.secondary = 0
			}
			got = append(got, row)
		}
	}
	sort.Slice(got, func(i, j int) bool { return got[i].id < got[j].id })
	return got
}

func TestPartitionTopOneRunsIndependentOracle(t *testing.T) {
	for _, layout := range []string{"single", "grouped", "interleaved", "unique"} {
		for _, pattern := range []string{"increasing", "decreasing", "random"} {
			for _, count := range []int{255, 256, 257, 773} {
				for _, split := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/n%d/split%t", layout, pattern, count, split), func(t *testing.T) {
						proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
						t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
						t.Cleanup(proc.Free)
						child := colexec.NewMockOperator()
						t.Cleanup(func() { child.Free(proc, false, nil) })
						arg := topOneRunArgument(true, true)
						t.Cleanup(func() { arg.Free(proc, false, nil) })
						arg.AppendChild(child)
						rows := make([]topOneRunRow, count)
						for i := range rows {
							group := 1
							switch layout {
							case "grouped":
								group = i/257 + 1
							case "interleaved":
								group = i%3 + 1
							case "unique":
								group = i + 1
							}
							score := i / 2 // adjacent equal keys carry different payloads
							switch pattern {
							case "decreasing":
								score = (count - i) / 2
							case "random":
								score = ((i*7919 + 17) % 101) / 2
							}
							rows[i] = topOneRunRow{group: int32(group), score: int64(score), id: int64(i)}
						}
						for start := 0; start < count; {
							end := count
							if split {
								end = min(count, start+255)
							}
							child.WithBatchs([]*batch.Batch{topOneRunBatch(t, proc, rows[start:end])})
							start = end
						}
						require.NoError(t, arg.Prepare(proc))
						require.Equal(t, topOneRunOracle(rows, true, true), topOneRunCollect(t, arg, proc))
					})
				}
			}
		}
	}
}

func TestPartitionTopOneRunsNullableCompositeReset(t *testing.T) {
	for _, desc := range []bool{false, true} {
		for _, nullsLast := range []bool{false, true} {
			t.Run(fmt.Sprintf("desc%t/nullsLast%t", desc, nullsLast), func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
				t.Cleanup(proc.Free)
				arg := topOneRunArgument(desc, nullsLast)
				t.Cleanup(func() { arg.Free(proc, false, nil) })
				for generation := 0; generation < 2; generation++ {
					child := colexec.NewMockOperator()
					t.Cleanup(func() { child.Free(proc, false, nil) })
					arg.Children = nil
					arg.AppendChild(child)
					rows := make([]topOneRunRow, 1031)
					for i := range rows {
						row := topOneRunRow{group: int32((i / 257) % 3), score: int64((i + generation) % 11), secondary: int64((i * 7) % 5), id: int64(i)}
						row.groupNull, row.scoreNull, row.secondaryNull = row.group == 0, i%13 == 0, i%17 == 0
						if row.scoreNull {
							row.score = 0
						}
						if row.secondaryNull {
							row.secondary = 0
						}
						rows[i] = row
					}
					for start := 0; start < len(rows); start += 257 {
						child.WithBatchs([]*batch.Batch{topOneRunBatch(t, proc, rows[start:min(start+257, len(rows))])})
					}
					require.NoError(t, arg.Prepare(proc))
					require.Equal(t, topOneRunOracle(rows, desc, nullsLast), topOneRunCollect(t, arg, proc))
					arg.Reset(proc, false, nil)
					child.Free(proc, false, nil)
				}
			})
		}
	}
}

func TestPartitionTopOneRunsTwoStageOracle(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	t.Cleanup(proc.Free)
	globalChild := colexec.NewMockOperator()
	t.Cleanup(func() { globalChild.Free(proc, false, nil) })
	var rows []topOneRunRow
	for shard := 0; shard < 3; shard++ {
		child := colexec.NewMockOperator()
		t.Cleanup(func() { child.Free(proc, false, nil) })
		local := topOneRunArgument(true, true)
		local.PreReduce = true
		t.Cleanup(func() { local.Free(proc, false, nil) })
		local.AppendChild(child)
		part := make([]topOneRunRow, 773)
		for i := range part {
			part[i] = topOneRunRow{group: int32(i / 257), score: int64((i*17 + shard) % 101), secondary: int64(i % 3), id: int64(shard*773 + i)}
		}
		rows = append(rows, part...)
		child.WithBatchs([]*batch.Batch{topOneRunBatch(t, proc, part[:256]), topOneRunBatch(t, proc, part[256:])})
		require.NoError(t, local.Prepare(proc))
		for {
			result, err := local.Call(proc)
			require.NoError(t, err)
			if result.Batch == nil || result.Status == vm.ExecStop {
				break
			}
			copyBatch, err := result.Batch.Dup(proc.Mp())
			require.NoError(t, err)
			globalChild.WithBatchs([]*batch.Batch{copyBatch})
		}
	}
	global := topOneRunArgument(true, true)
	t.Cleanup(func() { global.Free(proc, false, nil) })
	global.AppendChild(globalChild)
	require.NoError(t, global.Prepare(proc))
	require.Equal(t, topOneRunOracle(rows, true, true), topOneRunCollect(t, global, proc))
}

func TestPartitionTopOneRunsWinnerMetadata(t *testing.T) {
	// Row zero exercises all-literal ORDER BY (no runtime comparator). Other
	// rows cover initial retention and replacement for all three metadata kinds.
	for _, winner := range []int{0, 100, 333, 334, 335} {
		t.Run(fmt.Sprintf("winner%d", winner), func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
			t.Cleanup(proc.Free)
			child := colexec.NewMockOperator()
			t.Cleanup(func() { child.Free(proc, false, nil) })
			arg := topOneRunArgument(true, true)
			arg.OrderBySpecs = arg.OrderBySpecs[:2]
			if winner == 0 {
				arg.OrderBySpecs[1].Expr = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 7}}}}
			}
			t.Cleanup(func() { arg.Free(proc, false, nil) })
			arg.AppendChild(child)
			var wantText string
			var wantKind vector.PrepareParamKind
			var wantDomain types.RuntimeStringDomain
			var wantSource types.StringSource
			for start := 0; start < 515; start += 257 {
				end := min(start+257, 515)
				rows := make([]topOneRunRow, end-start)
				for i := range rows {
					id := start + i
					rows[i] = topOneRunRow{group: 1, score: int64(id), id: int64(id)}
					if id == winner || id == winner+1 {
						rows[i].score = 10000
					}
				}
				bat := topOneRunBatch(t, proc, rows)
				child.WithBatchs([]*batch.Batch{bat})
				payload := vector.NewVec(types.T_varchar.ToType())
				bat.Vecs = append(bat.Vecs, payload)
				kinds := make([]vector.PrepareParamKind, len(rows))
				domains := make([]types.RuntimeStringDomain, len(rows))
				sources := make([]types.StringSource, len(rows))
				for i := range rows {
					id := start + i
					text := fmt.Sprintf("%d:%s", id, strings.Repeat("payload", 12))
					require.NoError(t, vector.AppendBytes(payload, []byte(text), false, proc.Mp()))
					kinds[i] = []vector.PrepareParamKind{vector.PrepareParamNone, vector.PrepareParamInteger, vector.PrepareParamFloat}[id%3]
					domains[i] = []types.RuntimeStringDomain{types.RuntimeStringInherit, types.RuntimeStringText, types.RuntimeStringBinary}[id%3]
					sources[i] = []types.StringSource{types.StringSourceExpression, types.StringSourceCOMStmt, types.StringSourceUserVariable}[id%3]
					if id == winner {
						wantText, wantKind, wantDomain, wantSource = text, kinds[i], domains[i], sources[i]
					}
				}
				require.NoError(t, payload.SetPrepareParamKindsWithMP(kinds, proc.Mp()))
				require.NoError(t, payload.SetRuntimeStringDomainsWithMP(domains, proc.Mp()))
				require.NoError(t, payload.SetStringSourcesWithMP(sources, proc.Mp()))
			}
			require.NoError(t, arg.Prepare(proc))
			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.NotNil(t, result.Batch)
			require.Equal(t, 1, result.Batch.RowCount())
			require.Equal(t, int64(winner), vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[3], 0))
			payload := result.Batch.Vecs[4]
			require.Equal(t, wantText, string(payload.GetBytesAt(0)))
			require.Equal(t, wantKind, payload.GetPrepareParamKindAt(0))
			require.Equal(t, wantDomain, payload.GetRuntimeStringDomainAt(0))
			require.Equal(t, wantSource, payload.GetStringSourceAt(0))
			result, err = arg.Call(proc)
			require.NoError(t, err)
			require.True(t, result.Batch == nil || result.Status == vm.ExecStop)
		})
	}
}

func TestPartitionTopOneRunsFloatAndConstantPeers(t *testing.T) {
	for _, typ := range []types.T{types.T_float32, types.T_float64} {
		for _, desc := range []bool{false, true} {
			for _, constant := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/desc%t/constant%t", typ, desc, constant), func(t *testing.T) {
					proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
					t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
					t.Cleanup(proc.Free)
					child := colexec.NewMockOperator()
					t.Cleanup(func() { child.Free(proc, false, nil) })
					arg := newTopNArgument(1)
					arg.OrderBySpecs = arg.OrderBySpecs[:2]
					arg.OrderBySpecs[1].Expr = topNCol(1, typ)
					if desc {
						arg.OrderBySpecs[1].Flag = plan.OrderBySpec_DESC
					}
					t.Cleanup(func() { arg.Free(proc, false, nil) })
					arg.AppendChild(child)
					// Each group has a 257-row run. NaNs never outrank finite
					// values; +/-0 and NaN peers retain their first payload.
					for group := 0; group < 4; group++ {
						bat := batch.NewWithSize(3)
						child.WithBatchs([]*batch.Batch{bat})
						bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
						bat.Vecs[1] = vector.NewVec(typ.ToType())
						bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
						for i := 0; i < 257; i++ {
							value := math.Float64frombits(0x7ff8000000000001 + uint64(i%2))
							switch group {
							case 0:
								if i > 0 {
									value = float64(i)
								}
							case 1:
								value = 0
								if i == 0 {
									value = math.Copysign(0, -1)
								}
							case 3:
								value = 0
								if i == 256 {
									if desc {
										value = math.Inf(1)
									} else {
										value = math.Inf(-1)
									}
								}
							}
							if constant {
								value = 7
							}
							require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(group), false, proc.Mp()))
							if typ == types.T_float32 {
								require.NoError(t, vector.AppendFixed(bat.Vecs[1], float32(value), false, proc.Mp()))
							} else {
								require.NoError(t, vector.AppendFixed(bat.Vecs[1], value, false, proc.Mp()))
							}
							require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(i), false, proc.Mp()))
						}
						if constant {
							bat.Vecs[1].Free(proc.Mp())
							var err error
							if typ == types.T_float32 {
								bat.Vecs[1], err = vector.NewConstFixed(typ.ToType(), float32(7), 257, proc.Mp())
							} else {
								bat.Vecs[1], err = vector.NewConstFixed(typ.ToType(), float64(7), 257, proc.Mp())
							}
							require.NoError(t, err)
						}
						bat.SetRowCount(257)
					}
					require.NoError(t, arg.Prepare(proc))
					seen := make(map[int32]int64)
					for {
						result, err := arg.Call(proc)
						require.NoError(t, err)
						if result.Batch == nil || result.Status == vm.ExecStop {
							break
						}
						for i := 0; i < result.Batch.RowCount(); i++ {
							group := vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[0], i)
							_, exists := seen[group]
							require.False(t, exists)
							seen[group] = vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[2], i)
							if group == 1 && !constant {
								if typ == types.T_float32 {
									require.True(t, math.Signbit(float64(vector.GetFixedAtWithTypeCheck[float32](result.Batch.Vecs[1], i))))
								} else {
									require.True(t, math.Signbit(vector.GetFixedAtWithTypeCheck[float64](result.Batch.Vecs[1], i)))
								}
							}
						}
					}
					want := map[int32]int64{0: 1, 1: 0, 2: 0, 3: 256}
					if desc {
						want[0] = 256
					}
					if constant {
						want = map[int32]int64{0: 0, 1: 0, 2: 0, 3: 0}
					}
					require.Equal(t, want, seen)
				})
			}
		}
	}
}
