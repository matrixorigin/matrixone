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
