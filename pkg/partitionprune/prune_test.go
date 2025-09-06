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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
}

func batchCount(result partitionservice.PruneResult) int {
	count := 0
	result.Iter(func(partition partition.Partition, bat *batch.Batch) bool {
		count++
		return true
	})
	return count
}
