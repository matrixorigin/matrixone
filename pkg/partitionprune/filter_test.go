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
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
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

	tests := []struct {
		name     string
		filters  []*plan.Expr
		metadata partition.PartitionMetadata
		want     []int
		wantErr  bool
	}{
		{
			name:    "empty filters",
			filters: []*plan.Expr{},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Range,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestRangeExpr("a", 0)},
					{Position: 1, Expr: newTestRangeExpr("a", 1)},
					{Position: 2, Expr: newTestRangeExpr("a", 2)},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// a = 1
			// a < 1
			// 1 <= a < 2
			// 2 <= a < 3
			name: "range filter - equal condition",
			filters: []*plan.Expr{
				makeEqualExpr(0, 1),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Range,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestRangeExpr("a", 0)},
					{Position: 1, Expr: newTestRangeExpr("a", 1)},
					{Position: 2, Expr: newTestRangeExpr("a", 2)},
				},
			},
			want:    []int{1},
			wantErr: false,
		},
		{
			// Remote execution replaces scalar constants with evaluated Fold
			// values before storage pruning. Range pruning must treat that wire
			// representation exactly like the equivalent literal predicate.
			name: "range filter - folded equal condition",
			filters: []*plan.Expr{
				makeFoldEqualExprInt32(0, 1),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Range,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestRangeExpr("a", 0)},
					{Position: 1, Expr: newTestRangeExpr("a", 1)},
					{Position: 2, Expr: newTestRangeExpr("a", 2)},
				},
			},
			want:    []int{1},
			wantErr: false,
		},
		{
			name: "range filter - folded disjunction",
			filters: []*plan.Expr{
				makeOrExpr(
					makeFoldEqualExprInt32(0, 0),
					makeFoldEqualExprInt32(0, 2),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Range,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestRangeExpr("a", 0)},
					{Position: 1, Expr: newTestRangeExpr("a", 1)},
					{Position: 2, Expr: newTestRangeExpr("a", 2)},
				},
			},
			want:    []int{0, 2},
			wantErr: false,
		},
		{
			// A scalar Fold with nil data is SQL NULL, not an empty literal.
			// Pruning must fail open because evaluating a nil Literal would panic.
			name: "range filter - folded null scans all partitions",
			filters: []*plan.Expr{
				makeFoldEqualExprInt32WithData(0, nil),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Range,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestRangeExpr("a", 0)},
					{Position: 1, Expr: newTestRangeExpr("a", 1)},
					{Position: 2, Expr: newTestRangeExpr("a", 2)},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// Fixed-width decoders use unsafe loads. Truncated runtime data must
			// disable pruning instead of panicking or selecting a wrong partition.
			name: "hash filter - malformed folded scalar scans all partitions",
			filters: []*plan.Expr{
				makeFoldEqualExprInt32WithData(0, []byte{1}),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Hash,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestHashExpr("a", 0)},
					{Position: 1, Expr: newTestHashExpr("a", 1)},
					{Position: 2, Expr: newTestHashExpr("a", 2)},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			name: "list filter - folded null scans all partitions",
			filters: []*plan.Expr{
				makeFoldEqualExprInt32WithData(0, nil),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_List,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestValuesInExpr("a")},
					{Position: 1, Expr: newTestValuesInExpr("a")},
					{Position: 2, Expr: newTestValuesInExpr("a")},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// A nullable folded IN vector cannot be sorted safely: the generic
			// vector sorter does not keep the null bitmap aligned with the value
			// payload. Pruning is optional, so retain all partitions instead of
			// risking a false negative. The value-before-NULL order exercises the
			// payload permutation that previously changed {1, NULL} into
			// {0, NULL}.
			name: "list filter - nullable folded vector scans all partitions",
			filters: []*plan.Expr{
				makeFoldInExprInt32(t, 0, false),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_List,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestValuesInExprWithValue("a", 0)},
					{Position: 1, Expr: newTestValuesInExprWithValue("a", 1)},
					{Position: 2, Expr: newTestValuesInExprWithValue("a", 2)},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			name: "list filter - nullable folded vector reverse order scans all partitions",
			filters: []*plan.Expr{
				makeFoldInExprInt32(t, 0, true),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_List,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestValuesInExprWithValue("a", 0)},
					{Position: 1, Expr: newTestValuesInExprWithValue("a", 1)},
					{Position: 2, Expr: newTestValuesInExprWithValue("a", 2)},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// a = 5
			// a % 3
			name: "hash filter - equal condition",
			filters: []*plan.Expr{
				makeEqualExpr(0, 5),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Hash,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestHashExpr("a", 0)},
					{Position: 1, Expr: newTestHashExpr("a", 1)},
					{Position: 2, Expr: newTestHashExpr("a", 2)},
				},
			},
			want:    []int{2},
			wantErr: false,
		},
		{
			// a = 1, 2, 3 -> int32
			// a in (1, 2)
			name: "list filter - in condition",
			filters: []*plan.Expr{
				makeInExpr(0, []int32{1, 2, 3}),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_List,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestValuesInExpr("a")},
					{Position: 1, Expr: newTestValuesInExpr("a")},
					{Position: 2, Expr: newTestValuesInExpr("a")},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// a = 1 or a = 2
			// a in (1, 2)
			name: "list filter - or condition",
			filters: []*plan.Expr{
				makeOrExpr(
					makeEqualExprInt32(0, 1),
					makeEqualExprInt32(0, 2),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_List,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestValuesInExpr("a")},
					{Position: 1, Expr: newTestValuesInExpr("a")},
					{Position: 2, Expr: newTestValuesInExpr("a")},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// a = 1 and a = 2
			// a in (1, 2)
			name: "list filter - and condition",
			filters: []*plan.Expr{
				makeAndExpr(
					makeEqualExprInt32(0, 1),
					makeEqualExprInt32(0, 2),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_List,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestValuesInExpr("a")},
					{Position: 1, Expr: newTestValuesInExpr("a")},
					{Position: 2, Expr: newTestValuesInExpr("a")},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			name: "range filter - and condition",
			filters: []*plan.Expr{
				makeAndExpr(
					makeGreaterEqualExpr(0, 15),
					makeLessEqualExpr(0, 25),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Range,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestRangeExpr("a", 0)},
					{Position: 1, Expr: newTestRangeExpr("a", 1)},
					{Position: 2, Expr: newTestRangeExpr("a", 2)},
				},
			},
			want:    []int{0, 1, 2},
			wantErr: false,
		},
		{
			// a = 5 or a = 8
			// a % 3
			name: "hash filter - or condition",
			filters: []*plan.Expr{
				makeOrExpr(
					makeEqualExpr(0, 5),
					makeEqualExpr(0, 8),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Hash,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestHashExpr("a", 0)},
					{Position: 1, Expr: newTestHashExpr("a", 1)},
					{Position: 2, Expr: newTestHashExpr("a", 2)},
				},
			},
			want:    []int{2},
			wantErr: false,
		},
		{
			// a = 5 and a = 8
			// a % 3
			name: "hash filter - and condition",
			filters: []*plan.Expr{
				makeAndExpr(
					makeEqualExpr(0, 5),
					makeEqualExpr(0, 8),
				),
			},
			metadata: partition.PartitionMetadata{
				Method: partition.PartitionMethod_Hash,
				Partitions: []partition.Partition{
					{Position: 0, Expr: newTestHashExpr("a", 0)},
					{Position: 1, Expr: newTestHashExpr("a", 1)},
					{Position: 2, Expr: newTestHashExpr("a", 2)},
				},
			},
			want:    []int{2},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Filter(proc, tt.filters, tt.metadata)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}

// BenchmarkListFilterExprLargeOr isolates the nested predicate shape involved
// in pruning a large CDC batch. Every recursive level must not clone the entire
// remaining expression tree.
func BenchmarkListFilterExprLargeOr(b *testing.B) {
	mp := mpool.MustNewZeroNoFixed()
	proc := process.NewTopProcess(
		context.Background(),
		mp,
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	defer proc.Free()

	const rowsPerBatch = 512
	expr := makeEqualExprInt32(0, 0)
	for i := 1; i < rowsPerBatch; i++ {
		expr = makeOrExpr(expr, makeEqualExprInt32(0, int32(i)))
	}
	metadata := partition.PartitionMetadata{
		Method: partition.PartitionMethod_List,
		Partitions: []partition.Partition{
			{Position: 0, Expr: newTestValuesInExpr("a")},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := listFilterExpr(proc, 0, expr, metadata)
		require.NoError(b, err)
	}
}

// BenchmarkListFilterCompositeKeyDelete models the DELETE generated by the
// Flink CDC upsert path before its multi-row INSERT:
//
//	DELETE FROM t WHERE (pk1=? AND pk2=?) OR (pk1=? AND pk2=?) ...
func BenchmarkListFilterCompositeKeyDelete(b *testing.B) {
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

	expr := makeAndExpr(makeEqualExprInt32(0, 0), makeEqualExprInt32(1, 0))
	for i := int32(1); i < 512; i++ {
		row := makeAndExpr(makeEqualExprInt32(0, i), makeEqualExprInt32(1, i))
		expr = makeOrExpr(expr, row)
	}
	metadata := partition.PartitionMetadata{
		Method: partition.PartitionMethod_List,
		Partitions: []partition.Partition{
			{Position: 0, Expr: newTestValuesInExpr("a")},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Filter(proc, []*plan.Expr{expr}, metadata)
		require.NoError(b, err)
	}
}

// Helper functions to create test expressions
func makeEqualExpr(colPos int32, value int64) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "=",
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: int32(types.T_int64)},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value: &plan.Literal_I64Val{
									I64Val: value,
								},
							},
						},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
			},
		},
	}
}

func makeEqualExprInt32(colPos int32, value int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "=",
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: int32(types.T_int32)},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value: &plan.Literal_I32Val{
									I32Val: value,
								},
							},
						},
						Typ: plan.Type{
							Id: int32(types.T_int32),
						},
					},
				},
			},
		},
	}
}

func makeFoldEqualExprInt32(colPos int32, value int32) *plan.Expr {
	return makeFoldEqualExprInt32WithData(colPos, types.EncodeInt32(&value))
}

func makeFoldEqualExprInt32WithData(colPos int32, data []byte) *plan.Expr {
	expr := makeEqualExprInt32(colPos, 0)
	expr.GetF().Args[1].Expr = &plan.Expr_Fold{
		Fold: &plan.FoldVal{
			IsConst: true,
			Data:    data,
		},
	}
	return expr
}

func makeInExpr(colPos int32, values []int32) *plan.Expr {
	list := make([]*plan.Expr, len(values))
	for i, v := range values {
		list[i] = &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_I32Val{
						I32Val: v,
					},
				},
			},
			Typ: plan.Type{
				Id: int32(types.T_int32),
			},
		}
	}
	return &plan.Expr{
		Typ: plan.Type{Id: 10},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "in",
				},
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_List{
							List: &plan.ExprList{
								List: list,
							},
						},
					},
				},
			},
		},
	}
}

func makeFoldInExprInt32(t *testing.T, colPos int32, nullFirst bool) *plan.Expr {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	vec := vector.NewVec(types.T_int32.ToType())
	appendValue := func(value int32, isNull bool) {
		require.NoError(t, vector.AppendFixed(vec, value, isNull, mp))
	}
	if nullFirst {
		appendValue(0, true)
		appendValue(1, false)
	} else {
		appendValue(1, false)
		appendValue(0, true)
	}
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(mp)

	expr := makeInExpr(colPos, []int32{1})
	expr.GetF().Args[1] = &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
			IsConst: false,
			Data:    data,
		}},
	}
	return expr
}

func newTestHashExpr(col string, id uint64) *plan.Expr {
	return &plan.Expr{
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
																Name:   col,
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
												Value: &plan.Literal_U64Val{U64Val: 3},
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
								Value: &plan.Literal_U64Val{U64Val: id},
							},
						},
					},
				},
			},
		},
	}
}

func newTestRangeExpr(col string, partitionNum int64) *plan.Expr {
	if partitionNum == 0 {
		// a < 1
		return &plan.Expr{
			Typ: plan.Type{Id: 10},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						ObjName: "<",
						Obj:     17179869184,
					},
					Args: []*plan.Expr{
						{
							Typ: plan.Type{Id: 22},
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: col},
							},
						},
						{
							Typ: plan.Type{Id: 22},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{
										Obj:     90194313216,
										ObjName: "cast",
									},
									Args: []*plan.Expr{
										{
											Typ: plan.Type{Id: 23},
											Expr: &plan.Expr_Lit{
												Lit: &plan.Literal{
													Value: &plan.Literal_I64Val{I64Val: 1},
												},
											},
										},
										{
											Typ:  plan.Type{Id: 22},
											Expr: &plan.Expr_T{T: &plan.TargetType{}},
										},
									},
								},
							},
						},
					},
				},
			},
		}
	} else {
		// partitionNum <= a < partitionNum + 1
		return &plan.Expr{
			Typ: plan.Type{Id: 10},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						ObjName: "and",
						Obj:     73014444032,
					},
					Args: []*plan.Expr{
						// partitionNum <= a
						{
							Typ: plan.Type{Id: 10},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{
										ObjName: ">=",
										Obj:     12884901888,
									},
									Args: []*plan.Expr{
										{
											Typ: plan.Type{Id: 22},
											Expr: &plan.Expr_Col{
												Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: col},
											},
										},
										{
											Typ: plan.Type{Id: 22},
											Expr: &plan.Expr_F{
												F: &plan.Function{
													Func: &plan.ObjectRef{
														Obj:     90194313216,
														ObjName: "cast",
													},
													Args: []*plan.Expr{
														{
															Typ: plan.Type{Id: 23},
															Expr: &plan.Expr_Lit{
																Lit: &plan.Literal{
																	Value: &plan.Literal_I64Val{I64Val: partitionNum},
																},
															},
														},
														{
															Typ:  plan.Type{Id: 22},
															Expr: &plan.Expr_T{T: &plan.TargetType{}},
														},
													},
												},
											},
										},
									},
								},
							},
						},
						// a < partitionNum + 1
						{
							Typ: plan.Type{Id: 10},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{
										ObjName: "<",
										Obj:     17179869184,
									},
									Args: []*plan.Expr{
										{
											Typ: plan.Type{Id: 22},
											Expr: &plan.Expr_Col{
												Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: col},
											},
										},
										{
											Typ: plan.Type{Id: 22},
											Expr: &plan.Expr_F{
												F: &plan.Function{
													Func: &plan.ObjectRef{
														Obj:     90194313216,
														ObjName: "cast",
													},
													Args: []*plan.Expr{
														{
															Typ: plan.Type{Id: 23},
															Expr: &plan.Expr_Lit{
																Lit: &plan.Literal{
																	Value: &plan.Literal_I64Val{I64Val: partitionNum + 1},
																},
															},
														},
														{
															Typ:  plan.Type{Id: 22},
															Expr: &plan.Expr_T{T: &plan.TargetType{}},
														},
													},
												},
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
	}
}

func newTestValuesInExpr(col string) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: 10},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "in",
					Obj:     506806140934,
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: 22},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: col},
						},
					},
					{
						Typ: plan.Type{Id: 202},
						Expr: &plan.Expr_List{
							List: &plan.ExprList{
								List: []*plan.Expr{
									{
										Typ: plan.Type{Id: 22},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{
													Obj:     90194313216,
													ObjName: "cast",
												},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{Id: 23},
														Expr: &plan.Expr_Lit{
															Lit: &plan.Literal{
																Value: &plan.Literal_I64Val{I64Val: 1},
															},
														},
													},
													{
														Typ:  plan.Type{Id: 22},
														Expr: &plan.Expr_T{T: &plan.TargetType{}},
													},
												},
											},
										},
									},
									{
										Typ: plan.Type{Id: 22},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{ObjName: "cast", Obj: 90194313216},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{Id: 23},
														Expr: &plan.Expr_Lit{
															Lit: &plan.Literal{
																Value: &plan.Literal_I64Val{I64Val: 2},
															},
														},
													},
													{
														Typ:  plan.Type{Id: 22},
														Expr: &plan.Expr_T{T: &plan.TargetType{}},
													},
												},
											},
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
}

func newTestValuesInExprWithValue(col string, value int64) *plan.Expr {
	expr := newTestValuesInExpr(col)
	values := expr.GetF().Args[1].GetList().List
	values[0].GetF().Args[0].GetLit().Value = &plan.Literal_I64Val{I64Val: value}
	expr.GetF().Args[1].GetList().List = values[:1]
	return expr
}

func makeOrExpr(left, right *plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "or",
				},
				Args: []*plan.Expr{left, right},
			},
		},
	}
}

func makeAndExpr(left, right *plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "and",
				},
				Args: []*plan.Expr{left, right},
			},
		},
	}
}

func makeGreaterEqualExpr(colPos int32, value int64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: ">=",
				},
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value: &plan.Literal_I64Val{
									I64Val: value,
								},
							},
						},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
			},
		},
	}
}

func makeLessEqualExpr(colPos int32, value int64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "<=",
				},
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Isnull: false,
								Value: &plan.Literal_I64Val{
									I64Val: value,
								},
							},
						},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
			},
		},
	}
}

func TestConvertFoldExprToNormal(t *testing.T) {
	tests := []struct {
		name string
		expr *plan.Expr
		want *plan.Expr
	}{
		{
			name: "constant fold expression - int64",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    types.EncodeInt64(&[]int64{42}[0]),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{I64Val: 42},
					},
				},
			},
		},
		{
			name: "constant fold expression - int32",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    types.EncodeInt32(&[]int32{42}[0]),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I32Val{I32Val: 42},
					},
				},
			},
		},
		{
			name: "constant fold expression - float64",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    types.EncodeFloat64(&[]float64{42.5}[0]),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Dval{Dval: 42.5},
					},
				},
			},
		},
		{
			name: "constant fold expression - bool",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    types.EncodeBool(&[]bool{true}[0]),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Bval{Bval: true},
					},
				},
			},
		},
		{
			name: "constant fold expression - varchar",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: true,
						Data:    []byte("test"),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Sval{Sval: "test"},
					},
				},
			},
		},
		{
			name: "function expression",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{ColPos: 0},
								},
							},
							{
								Expr: &plan.Expr_Lit{
									Lit: &plan.Literal{
										Value: &plan.Literal_I64Val{I64Val: 42},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "vector fold expression",
			expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Fold{
					Fold: &plan.FoldVal{
						IsConst: false,
						Data: func() []byte {
							mp := mpool.MustNewZeroNoFixed()
							vec := vector.NewVec(types.T_int64.ToType())
							require.NoError(t, vector.AppendFixed[int64](vec, int64(1), false, mp))
							data, err := vec.MarshalBinary()
							require.NoError(t, err)
							vec.Free(mp)
							return data
						}(),
					},
				},
			},
			want: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Vec{
					Vec: &plan.LiteralVec{
						Len: 1,
						Data: func() []byte {
							mp := mpool.MustNewZeroNoFixed()
							vec := vector.NewVec(types.T_int64.ToType())
							require.NoError(t, vector.AppendFixed[int64](vec, int64(1), false, mp))
							vec.InplaceSortAndCompact()
							data, err := vec.MarshalBinary()
							require.NoError(t, err)
							vec.Free(mp)
							return data
						}(),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertFoldExprToNormal(tt.expr)
			require.NoError(t, err)
			if tt.want != nil {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestNormalizePartitionValuePreservesEmptyScalar(t *testing.T) {
	original := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
			IsConst: true,
			Data:    []byte{},
		}},
	}

	normalized, canPrune := normalizePartitionValue(original)
	require.True(t, canPrune)
	require.NotNil(t, original.GetFold().Data)
	require.Empty(t, original.GetFold().Data)
	require.Empty(t, normalized.GetLit().GetSval())
}

func TestConvertFoldExprToNormalRejectsAmbiguousScalars(t *testing.T) {
	tests := []struct {
		name string
		typ  types.T
		data []byte
	}{
		{name: "null", typ: types.T_varchar, data: nil},
		{name: "truncated fixed width", typ: types.T_int64, data: []byte{1}},
		{name: "oversized fixed width", typ: types.T_int32, data: make([]byte, 8)},
		{name: "producer-unsupported enum", typ: types.T_enum, data: make([]byte, types.T_enum.TypeLen())},
		{name: "unsupported UUID literal", typ: types.T_uuid, data: make([]byte, types.T_uuid.TypeLen())},
		{name: "unknown type", typ: types.T(255), data: []byte{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(tt.typ)},
				Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
					IsConst: true,
					Data:    tt.data,
				}},
			}
			got, canPrune := convertFoldExprToNormal(expr)
			require.False(t, canPrune)
			require.Nil(t, got)
		})
	}

	_, err := ConvertFoldExprToNormal(&plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true}},
	})
	require.Error(t, err)
}

func TestConvertFoldExprToNormalRejectsInvalidVectors(t *testing.T) {
	t.Run("malformed encoding", func(t *testing.T) {
		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
				Data: []byte{1},
			}},
		}
		got, canPrune := convertFoldExprToNormal(expr)
		require.False(t, canPrune)
		require.Nil(t, got)
	})

	t.Run("payload type mismatch", func(t *testing.T) {
		mp := mpool.MustNewZeroNoFixed()
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
		data, err := vec.MarshalBinary()
		require.NoError(t, err)
		vec.Free(mp)

		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
				Data: data,
			}},
		}
		got, canPrune := convertFoldExprToNormal(expr)
		require.False(t, canPrune)
		require.Nil(t, got)
	})
}

func TestConvertFoldExprToNormalFailsOpenOnNullableVectors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		typ       types.Type
		nullFirst bool
	}{
		{name: "fixed value then null", typ: types.T_int32.ToType()},
		{name: "fixed null then value", typ: types.T_int32.ToType(), nullFirst: true},
		{name: "varlen value then null", typ: types.T_varchar.ToType()},
		{name: "varlen null then value", typ: types.T_varchar.ToType(), nullFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			vec := vector.NewVec(tc.typ)
			appendValue := func(isNull bool) {
				if tc.typ.Oid == types.T_varchar {
					require.NoError(t, vector.AppendBytes(vec, []byte("keep"), isNull, mp))
					return
				}
				require.NoError(t, vector.AppendFixed(vec, int32(1), isNull, mp))
			}
			appendValue(tc.nullFirst)
			appendValue(!tc.nullFirst)
			data, err := vec.MarshalBinary()
			require.NoError(t, err)
			vec.Free(mp)

			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(tc.typ.Oid)},
				Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
					Data: data,
				}},
			}
			got, canPrune := convertFoldExprToNormal(expr)
			require.False(t, canPrune)
			require.Nil(t, got)
		})
	}

	t.Run("constant null vector", func(t *testing.T) {
		mp := mpool.MustNewZeroNoFixed()
		vec := vector.NewConstNull(types.T_int32.ToType(), 2, mp)
		data, err := vec.MarshalBinary()
		require.NoError(t, err)
		vec.Free(mp)

		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
				Data: data,
			}},
		}
		got, canPrune := convertFoldExprToNormal(expr)
		require.False(t, canPrune)
		require.Nil(t, got)
	})
}

func TestGetConstantFromBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		typ      plan.Type
		expected *plan.Literal
	}{
		{
			name: "bool type",
			data: types.EncodeBool(&[]bool{true}[0]),
			typ: plan.Type{
				Id: int32(types.T_bool),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: true},
			},
		},
		{
			name: "bit type",
			data: types.EncodeUint64(&[]uint64{7}[0]),
			typ: plan.Type{
				Id: int32(types.T_bit),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_U64Val{U64Val: 7},
			},
		},
		{
			name: "int8 type",
			data: types.EncodeInt8(&[]int8{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_int8),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_I32Val{I32Val: 42},
			},
		},
		{
			name: "int16 type",
			data: types.EncodeInt16(&[]int16{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_int16),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_I32Val{I32Val: 42},
			},
		},
		{
			name: "int32 type",
			data: types.EncodeInt32(&[]int32{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_int32),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_I32Val{I32Val: 42},
			},
		},
		{
			name: "int64 type",
			data: types.EncodeInt64(&[]int64{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_int64),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 42},
			},
		},
		{
			name: "uint8 type",
			data: types.EncodeUint8(&[]uint8{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_uint8),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_U32Val{U32Val: 42},
			},
		},
		{
			name: "uint16 type",
			data: types.EncodeUint16(&[]uint16{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_uint16),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_U32Val{U32Val: 42},
			},
		},
		{
			name: "uint32 type",
			data: types.EncodeUint32(&[]uint32{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_uint32),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_U32Val{U32Val: 42},
			},
		},
		{
			name: "uint64 type",
			data: types.EncodeUint64(&[]uint64{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_uint64),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_U64Val{U64Val: 42},
			},
		},
		{
			name: "float32 type",
			data: types.EncodeFloat32(&[]float32{42.5}[0]),
			typ: plan.Type{
				Id: int32(types.T_float32),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Fval{Fval: 42.5},
			},
		},
		{
			name: "float64 type",
			data: types.EncodeFloat64(&[]float64{42.5}[0]),
			typ: plan.Type{
				Id: int32(types.T_float64),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Dval{Dval: 42.5},
			},
		},
		{
			name: "date type",
			data: types.EncodeDate(&[]types.Date{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_date),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Dateval{Dateval: 42},
			},
		},
		{
			name: "time type",
			data: types.EncodeTime(&[]types.Time{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_time),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Timeval{Timeval: 42},
			},
		},
		{
			name: "datetime type",
			data: types.EncodeDatetime(&[]types.Datetime{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_datetime),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Datetimeval{Datetimeval: 42},
			},
		},
		{
			name: "timestamp type",
			data: types.EncodeTimestamp(&[]types.Timestamp{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_timestamp),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Timestampval{Timestampval: 42},
			},
		},
		{
			name: "decimal64 type",
			data: types.EncodeDecimal64(&[]types.Decimal64{42}[0]),
			typ: plan.Type{
				Id: int32(types.T_decimal64),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: 42}},
			},
		},
		{
			name: "decimal128 type",
			data: types.EncodeDecimal128(&[]types.Decimal128{{B0_63: 42, B64_127: 0}}[0]),
			typ: plan.Type{
				Id: int32(types.T_decimal128),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: 42, B: 0}},
			},
		},
		{
			name: "varchar type",
			data: []byte("test"),
			typ: plan.Type{
				Id: int32(types.T_varchar),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: "test"},
			},
		},
		{
			name: "varbinary type",
			data: []byte{'a', 0, 'b'},
			typ: plan.Type{
				Id: int32(types.T_varbinary),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: "a\x00b"},
			},
		},
		{
			name: "empty fixed-width data",
			data: []byte{},
			typ: plan.Type{
				Id: int32(types.T_int64),
			},
			expected: nil,
		},
		{
			name: "empty varchar",
			data: []byte{},
			typ: plan.Type{
				Id: int32(types.T_varchar),
			},
			expected: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: ""},
			},
		},
		{
			name: "null varchar",
			data: nil,
			typ: plan.Type{
				Id: int32(types.T_varchar),
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := getConstantFromBytes(tt.data, tt.typ)
			require.Equal(t, tt.expected != nil, ok)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("getConstantFromBytes() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMergeSortedSlices(t *testing.T) {
	tests := []struct {
		name   string
		slice1 []int
		slice2 []int
		want   []int
	}{
		{
			name:   "empty slices",
			slice1: []int{},
			slice2: []int{},
			want:   []int{},
		},
		{
			name:   "one empty slice",
			slice1: []int{1, 2, 3},
			slice2: []int{},
			want:   []int{1, 2, 3},
		},
		{
			name:   "no duplicates",
			slice1: []int{1, 3, 5},
			slice2: []int{2, 4, 6},
			want:   []int{1, 2, 3, 4, 5, 6},
		},
		{
			name:   "with duplicates",
			slice1: []int{1, 2, 3},
			slice2: []int{2, 3, 4},
			want:   []int{1, 2, 3, 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeSortedSlices(tt.slice1, tt.slice2)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIntersectSortedSlices(t *testing.T) {
	tests := []struct {
		name   string
		slice1 []int
		slice2 []int
		want   []int
	}{
		{
			name:   "empty slices",
			slice1: []int{},
			slice2: []int{},
			want:   []int{},
		},
		{
			name:   "one empty slice",
			slice1: []int{1, 2, 3},
			slice2: []int{},
			want:   []int{},
		},
		{
			name:   "no intersection",
			slice1: []int{1, 3, 5},
			slice2: []int{2, 4, 6},
			want:   []int{},
		},
		{
			name:   "with intersection",
			slice1: []int{1, 2, 3},
			slice2: []int{2, 3, 4},
			want:   []int{2, 3},
		},
		{
			name:   "with duplicates",
			slice1: []int{1, 2, 2, 3},
			slice2: []int{2, 2, 3, 4},
			want:   []int{2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := intersectSortedSlices(tt.slice1, tt.slice2)
			require.Equal(t, tt.want, got)
		})
	}
}
