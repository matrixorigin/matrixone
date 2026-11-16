// Copyright 2022 Matrix Origin
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

package partitionprune

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	p "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestPrunePartitionByExpr(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := process.NewTopProcess(
		context.Background(),
		mp,
		nil, // no txn client
		nil, // no txn operator
		nil, // no file service
		nil, // no lock service
		nil, // no query client
		nil, // no hakeeper
		nil, // no udf service
		nil, // no auto increase
		nil,
	)
	defer proc.Free()

	bat := batch.NewWithSize(2)
	defer bat.Clean(mp)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(2), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(3), false, proc.Mp()))

	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("a"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("b"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("c"), false, proc.Mp()))

	bat.SetRowCount(3)

	t.Run("normal filter", func(t *testing.T) {
		// hash 2
		// 1%2=1, 2%2=0, 3%2=1
		expr := &plan.Expr{
			Typ: plan.Type{Id: 10},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						Obj:     0,
						ObjName: "=",
					},
					Args: []*plan.Expr{
						{
							Typ: plan.Type{Id: 28},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{
										Obj:     64424509440,
										ObjName: "%",
									},
									Args: []*plan.Expr{
										{
											Typ: plan.Type{Id: 28},
											Expr: &plan.Expr_F{
												F: &plan.Function{
													Func: &plan.ObjectRef{
														Obj:     90194313216,
														ObjName: "cast",
													},
													Args: []*plan.Expr{
														{
															Typ: plan.Type{Id: 22},
															Expr: &plan.Expr_Col{
																Col: &plan.ColRef{
																	RelPos: 0,
																	ColPos: 0,
																},
															},
														},
														{
															Typ:  plan.Type{Id: 28},
															Expr: &plan.Expr_T{T: &plan.TargetType{}},
														},
													},
												},
											},
										},
										{
											Typ: plan.Type{Id: 28},
											Expr: &plan.Expr_Lit{
												Lit: &plan.Literal{
													Value: &plan.Literal_U64Val{U64Val: 2},
												},
											},
										},
									},
								},
							},
						},
						{
							Typ: plan.Type{Id: 28},
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_U64Val{U64Val: 0},
								},
							},
						},
					},
				},
			},
		}

		partition := partition.Partition{
			Expr: expr,
		}

		result, err := PrunePartitionByExpr(proc, bat, partition, -1)
		require.NoError(t, err)
		defer result.Clean(mp)
		require.Equal(t, 1, result.RowCount())
	})
}

func TestPrune(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := process.NewTopProcess(
		context.Background(),
		mp,
		nil, // no txn client
		nil, // no txn operator
		nil, // no file service
		nil, // no lock service
		nil, // no query client
		nil, // no hakeeper
		nil, // no udf service
		nil, // no auto increase
		nil,
	)
	defer proc.Free()

	bat := batch.NewWithSize(2)
	defer bat.Clean(mp)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(2), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(3), false, proc.Mp()))

	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("a"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("b"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("c"), false, proc.Mp()))

	bat.SetRowCount(3)

	t.Run("multiple partitions", func(t *testing.T) {
		metadata := partition.PartitionMetadata{
			Partitions: []partition.Partition{
				{
					Expr: &plan.Expr{
						Typ: plan.Type{Id: 10},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{
									Obj:     0,
									ObjName: "=",
								},
								Args: []*plan.Expr{
									{
										Typ: plan.Type{Id: 28},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{
													Obj:     64424509440,
													ObjName: "%",
												},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{Id: 28},
														Expr: &plan.Expr_F{
															F: &plan.Function{
																Func: &plan.ObjectRef{
																	Obj:     90194313216,
																	ObjName: "cast",
																},
																Args: []*plan.Expr{
																	{
																		Typ: plan.Type{Id: 22},
																		Expr: &plan.Expr_Col{
																			Col: &plan.ColRef{
																				RelPos: 1,
																				ColPos: 0,
																				Name:   "name",
																			},
																		},
																	},
																	{
																		Typ:  plan.Type{Id: 28},
																		Expr: &plan.Expr_T{T: &plan.TargetType{}},
																	},
																},
															},
														},
													},
													{
														Typ: plan.Type{Id: 28},
														Expr: &plan.Expr_Lit{
															Lit: &plan.Literal{
																Value: &plan.Literal_U64Val{U64Val: 2},
															},
														},
													},
												},
											},
										},
									},
									{
										Typ: plan.Type{Id: 28},
										Expr: &plan.Expr_Lit{
											Lit: &plan.Literal{
												Value: &plan.Literal_U64Val{U64Val: 0},
											},
										},
									},
								},
							},
						},
					},
				},
				{
					Expr: &plan.Expr{
						Typ: plan.Type{Id: 10},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{
									Obj:     0,
									ObjName: "=",
								},
								Args: []*plan.Expr{
									{
										Typ: plan.Type{Id: 28},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{
													Obj:     64424509440,
													ObjName: "%",
												},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{Id: 28},
														Expr: &plan.Expr_F{
															F: &plan.Function{
																Func: &plan.ObjectRef{
																	Obj:     90194313216,
																	ObjName: "cast",
																},
																Args: []*plan.Expr{
																	{
																		Typ: plan.Type{Id: 22},
																		Expr: &plan.Expr_Col{
																			Col: &plan.ColRef{
																				RelPos: 1,
																				ColPos: 0,
																				Name:   "name",
																			},
																		},
																	},
																	{
																		Typ:  plan.Type{Id: 28},
																		Expr: &plan.Expr_T{T: &plan.TargetType{}},
																	},
																},
															},
														},
													},
													{
														Typ: plan.Type{Id: 28},
														Expr: &plan.Expr_Lit{
															Lit: &plan.Literal{
																Value: &plan.Literal_U64Val{U64Val: 2},
															},
														},
													},
												},
											},
										},
									},
									{
										Typ: plan.Type{Id: 28},
										Expr: &plan.Expr_Lit{
											Lit: &plan.Literal{
												Value: &plan.Literal_U64Val{U64Val: 1},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		result, err := Prune(proc, bat, metadata, -1)
		require.NoError(t, err)
		defer result.Close()
		require.Equal(t, 2, batchCount(result))
	})

	t.Run("empty partitions", func(t *testing.T) {
		metadata := partition.PartitionMetadata{
			Partitions: []partition.Partition{},
		}

		_, err := Prune(proc, bat, metadata, -1)
		require.Error(t, err)
	})

	t.Run("all rows miss partitions returns invalid input before empty result", func(t *testing.T) {
		metadata := partition.PartitionMetadata{
			Partitions: []partition.Partition{
				{
					Expr: &plan.Expr{
						Typ: plan.Type{Id: int32(types.T_bool)},
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}},
						},
					},
				},
			},
		}

		_, err := Prune(proc, bat, metadata, -1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Table has no partition for value from column_list")
	})
}

func batchCount(result partitionservice.PruneResult) int {
	count := 0
	result.Iter(func(partition partition.Partition, bat *batch.Batch) bool {
		count++
		return true
	})
	return count
}

// Bind real overloads so these tests exercise evaluation with incompatible
// incoming and resolved identities, rather than only inspecting rewritten ASTs.
func bindPartitionTestExpr(t *testing.T, name string, args ...*plan.Expr) *plan.Expr {
	t.Helper()
	expr, err := p.BindFuncExprImplByPlanExpr(context.Background(), name, args)
	require.NoError(t, err)
	return expr
}

func partitionTestColumn(pos int32) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos, Name: "id"}}}
}

func TestPruneRepeatedResolvedIdentity(t *testing.T) {
	for _, shape := range []string{"sum", "square", "nested"} {
		for _, rowID := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rowid=%v", shape, rowID), func(t *testing.T) {
				mp := mpool.MustNewZeroNoFixed()
				proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				defer proc.Free()
				build := func(pos int32) *plan.Expr {
					value := bindPartitionTestExpr(t, "+", partitionTestColumn(pos), partitionTestColumn(pos))
					if shape == "square" {
						value = bindPartitionTestExpr(t, "*", partitionTestColumn(pos), partitionTestColumn(pos))
					}
					if shape == "nested" {
						value = bindPartitionTestExpr(t, "*", partitionTestColumn(pos), value)
					}
					zero := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}}
					return bindPartitionTestExpr(t, ">=", value, zero)

				}
				expr, shifted := build(0), build(1)
				original := p.DeepCopyExpr(expr)
				originalShifted := p.DeepCopyExpr(shifted)
				bat := batch.NewWithSize(3)
				index := 2
				if rowID {
					bat = batch.NewWithSize(4)
					bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.Rowid{}, false, mp))
					index++
				}
				defer bat.Clean(mp)
				offset := 0
				if rowID {
					offset = 1
				}
				for i, v := range []int64{-1, 42, 1} {
					bat.Vecs[i+offset] = vector.NewVec(types.T_int64.ToType())
					require.NoError(t, vector.AppendFixed(bat.Vecs[i+offset], v, false, mp))
				}
				bat.SetRowCount(1)
				// partitionIndex excludes RowID, while index selects the actual
				// resolved vector in the batch.
				result, err := PrunePartitionByExpr(proc, bat, partition.Partition{Expr: expr, ExprWithRowID: shifted}, 2)
				require.NoError(t, err)
				defer result.Clean(mp)
				require.Equal(t, 1, result.RowCount())
				require.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](result.Vecs[index]))
				require.Equal(t, original, expr)
				require.Equal(t, originalShifted, shifted)
			})
		}
	}
}

func TestPartitionColumnTraversal(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%v", reverse), func(t *testing.T) {
			direct := partitionTestColumn(0)
			nested := bindPartitionTestExpr(t, "+", partitionTestColumn(0), partitionTestColumn(1))
			args := []*plan.Expr{direct, nested}
			if reverse {
				args[0], args[1] = args[1], args[0]
			}
			expr := bindPartitionTestExpr(t, "+", args...)
			// Select the first schema column, preserving the other column.
			mustReplaceColPos(expr, 3)
			require.Equal(t, int32(3), direct.GetCol().ColPos)
			require.Equal(t, int32(3), nested.GetF().Args[0].GetCol().ColPos)
			require.Equal(t, int32(1), nested.GetF().Args[1].GetCol().ColPos)
		})
	}
	root := partitionTestColumn(2)
	mustReplaceColPos(root, 4)
	require.Equal(t, int32(4), root.GetCol().ColPos)

}

func TestPrunePreservesOtherPartitionColumns(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer proc.Free()
	build := func(offset int32) *plan.Expr {
		other := partitionTestColumn(offset + 1)
		other.GetCol().Name = "other"
		sum := bindPartitionTestExpr(t, "+", partitionTestColumn(offset), other)
		zero := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}}
		return bindPartitionTestExpr(t, "<", sum, zero)

	}
	expr, shifted := build(0), build(1)
	for _, rowID := range []bool{false, true} {
		for _, explicit := range []int32{-1, 2} {
			t.Run(fmt.Sprintf("rowid=%v/index=%d", rowID, explicit), func(t *testing.T) {
				offset := 0
				if rowID {
					offset = 1
				}
				bat := batch.NewWithSize(3 + offset)
				defer bat.Clean(mp)
				if rowID {
					bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.Rowid{}, false, mp))
				}
				for i, v := range []int64{-1, -100, 1} {
					bat.Vecs[i+offset] = vector.NewVec(types.T_int64.ToType())
					require.NoError(t, vector.AppendFixed(bat.Vecs[i+offset], v, false, mp))
				}
				bat.SetRowCount(1)
				before, beforeShifted := p.DeepCopyExpr(expr), p.DeepCopyExpr(shifted)
				result, err := PrunePartitionByExpr(proc, bat, partition.Partition{Expr: expr, ExprWithRowID: shifted}, explicit)
				require.NoError(t, err)
				defer result.Clean(mp)
				require.Equal(t, 1, result.RowCount(), "unrelated column must remain -100")
				require.Equal(t, []int64{-100}, vector.MustFixedColWithTypeCheck[int64](result.Vecs[1+offset]))
				require.Equal(t, before, expr)
				require.Equal(t, beforeShifted, shifted)
			})
		}
	}
}

// All regressions enter through Prune so the original prune.go and types.go
// can be overlaid together without adapting these tests to a new constructor.
func TestPruneBatchOwnership(t *testing.T) {
	for _, scenario := range []string{
		"success", "uncovered-row", "later-constructor-error", "later-eval-error",
		"later-copy-error", "later-eval-panic", "later-copy-panic",
	} {
		t.Run(scenario, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(mp)
			proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			defer proc.Free()
			input := batch.NewWithSize(4)
			defer input.Clean(mp)
			input.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
			input.Vecs[1] = vector.NewVec(types.T_bool.ToType())
			input.Vecs[2] = vector.NewVec(types.T_bool.ToType())
			input.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			values := [][]byte{[]byte("first payload exceeding inline capacity"), []byte("second payload exceeding inline capacity")}
			if scenario == "later-copy-error" {
				values[1] = []byte{0xff}
			}
			for i := range 2 {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(i+1), false, mp))
				require.NoError(t, vector.AppendFixed(input.Vecs[1], i == 0, false, mp))
				require.NoError(t, vector.AppendFixed(input.Vecs[2], i == 1, false, mp))
				require.NoError(t, vector.AppendBytes(input.Vecs[3], values[i], false, mp))
			}
			input.SetRowCount(2)
			if scenario == "later-copy-error" {
				// Admit the raw fixture first, then give it a native Unicode
				// type: UnionOne rejects the second value after allocating its
				// descriptor and copying the preceding columns. No test hook.
				input.Vecs[3].SetType(types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB4UnicodeCI))
			}
			if scenario == "later-copy-panic" {
				// A truncated source exposes a panic inside UnionOne after the
				// earlier columns and earlier partition have already allocated.
				input.Vecs[3].Free(mp)
				short, err := vector.NewVecWithDataCopy(types.T_int64.ToType(), 1, make([]byte, 8), nil, mp)
				require.NoError(t, err)
				input.Vecs[3] = short
			}
			column := func(pos int32) *plan.Expr {
				return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
			}
			metadata := partition.PartitionMetadata{Partitions: []partition.Partition{
				{PartitionID: 10, Expr: column(1)},
				{PartitionID: 11, Expr: column(2)},
			}}
			switch scenario {
			case "uncovered-row":
				metadata.Partitions = metadata.Partitions[:1]
			case "later-constructor-error":
				metadata.Partitions[1].Expr = &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}}
			case "later-eval-error":
				metadata.Partitions[1].Expr = &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_V{V: &plan.VarRef{Name: "prune_missing_resolver"}}}
			case "later-eval-panic":
				metadata.Partitions[1].Expr = column(99)
			}
			beforeExpr := make([]*plan.Expr, len(metadata.Partitions))
			for i, part := range metadata.Partitions {
				beforeExpr[i] = p.DeepCopyExpr(part.Expr)
			}
			beforeData := append([]byte(nil), input.Vecs[3].GetData()...)
			beforeArea := append([]byte(nil), input.Vecs[3].GetArea()...)
			beforeType := *input.Vecs[3].GetType()
			beforeLength := input.Vecs[3].Length()
			baseline := mp.CurrNB()
			require.Positive(t, baseline, "input remains allocated throughout the prune lifetime")
			heapBytes, heapObjects := mp.OnHeapOutstanding()
			if scenario == "later-eval-panic" || scenario == "later-copy-panic" {
				require.Panics(t, func() {
					result, err := Prune(proc, input, metadata, -1)
					defer result.Close()
					require.NoError(t, err)
				})
			} else {
				result, err := Prune(proc, input, metadata, -1)
				defer result.Close()
				if scenario == "success" {
					require.NoError(t, err)
					require.False(t, result.Empty())
					require.Greater(t, mp.OnHeapCurrNB(), heapBytes, "nonempty copies must actually allocate")
					alias := result
					var got []int64
					alias.Iter(func(part partition.Partition, b *batch.Batch) bool {
						require.Equal(t, uint64(10+len(got)), part.PartitionID)
						require.NotSame(t, input.Vecs[0], b.Vecs[0])
						got = append(got, vector.MustFixedColWithTypeCheck[int64](b.Vecs[0])...)
						require.Equal(t, values[len(got)-1], b.Vecs[3].GetBytesAt(0))
						return true
					})
					require.Equal(t, []int64{1, 2}, got)
					alias.Close()
					result.Close()
					alias.Close()
				} else {
					require.Error(t, err)
					require.True(t, result.Empty())
					switch scenario {
					case "uncovered-row":
						require.Contains(t, err.Error(), "Table has no partition for value from column_list")
					case "later-constructor-error":
						require.Contains(t, err.Error(), "unsupported expression executor")
					case "later-eval-error":
						require.Contains(t, err.Error(), "resolve variable function is not set")
					case "later-copy-error":
						require.Contains(t, err.Error(), "invalid UTF-8")
					}
				}
			}
			// Keep the input, process and pool alive for all three oracles.
			require.Equal(t, baseline, mp.CurrNB())
			bytes, objects := mp.OnHeapOutstanding()
			require.Equal(t, heapBytes, bytes)
			require.Equal(t, heapObjects, objects)
			require.Equal(t, 2, input.RowCount())
			require.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](input.Vecs[0]))
			require.Equal(t, []bool{true, false}, vector.MustFixedColWithTypeCheck[bool](input.Vecs[1]))
			require.Equal(t, []bool{false, true}, vector.MustFixedColWithTypeCheck[bool](input.Vecs[2]))
			require.Equal(t, beforeData, input.Vecs[3].GetData())
			require.Equal(t, beforeArea, input.Vecs[3].GetArea())
			require.Equal(t, beforeType, *input.Vecs[3].GetType())
			require.Equal(t, beforeLength, input.Vecs[3].Length())
			if scenario != "later-copy-panic" {
				for i, value := range values {
					require.Equal(t, value, input.Vecs[3].GetBytesAt(i))
				}
			}
			for i, part := range metadata.Partitions {
				require.Equal(t, beforeExpr[i], part.Expr)
			}
		})
	}
}
