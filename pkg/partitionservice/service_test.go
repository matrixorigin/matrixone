// Copyright 2021-2024 Matrix Origin
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

package partitionservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T) {
	num := uint64(2)
	tableID := uint64(1)
	columns := []string{"a"}

	runTestPartitionServiceTest(
		func(
			ctx context.Context,
			txnOp client.TxnOperator,
			s *Service,
			store PartitionStorage,
		) {
			def := newTestTablePartitionDefine(1, columns, []types.T{types.T_int8}, num, partition.PartitionMethod_Hash)
			memStore := store.(*memStorage)
			memStore.addUncommittedTable(def)

			stmt := newTestHashOption(t, columns[0], num)
			assert.NoError(t, s.Create(ctx, tableID, stmt, txnOp))

			require.NoError(t, txnOp.Commit(ctx))

			require.NoError(t, s.Delete(ctx, tableID, nil))
		},
	)
}

func TestIterResult(t *testing.T) {
	res := PruneResult{
		batches:    make([]*batch.Batch, 10),
		partitions: make([]partition.Partition, 10),
	}

	n := 0
	res.Iter(
		func(partition partition.Partition, bat *batch.Batch) bool {
			n++
			return false
		},
	)
	require.Equal(t, 1, n)
}

func runTestPartitionServiceTest(
	fn func(
		ctx context.Context,
		txnOp client.TxnOperator,
		s *Service,
		store PartitionStorage,
	),
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txnOp, close := client.NewTestTxnOperator(ctx)
	defer close()

	store := newMemPartitionStorage()
	s := NewService(Config{}, store)
	fn(ctx, txnOp, s, store)
}

func newTestTablePartitionDefine(
	id uint64,
	columns []string,
	types []types.T,
	num uint64,
	method partition.PartitionMethod,
) *plan.TableDef {
	def := &plan.TableDef{
		TblId:     id,
		Partition: &plan.Partition{},
	}

	for idx, col := range columns {
		def.Cols = append(
			def.Cols,
			&plan.ColDef{
				Name: col,
				Typ:  plan.Type{Id: int32(types[idx])},
			},
		)
		def.Partition.PartitionDefs = append(
			def.Partition.PartitionDefs,
			&plan.PartitionDef{Def: newTestValuesInExpr(col)},
		)
	}

	var fn func(string) *plan.Expr

	switch method {
	case partition.PartitionMethod_List:
		fn = newTestValuesInExpr
	case partition.PartitionMethod_Range:
		fn = newTestRangeExpr
	case partition.PartitionMethod_Hash,
		partition.PartitionMethod_Key:
		fn = newTestHashExpr
	}
	for i := uint64(0); i < num; i++ {
		def.Partition.PartitionDefs = append(
			def.Partition.PartitionDefs,
			&plan.PartitionDef{Def: fn(columns[0])},
		)
	}
	return def
}

func newTestHashExpr(col string) *plan.Expr {
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
	}
}

func newTestRangeExpr(col string) *plan.Expr {
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
														Typ:  plan.Type{Id: 23},
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

func getCreateTableStatement(
	t *testing.T,
	sql string,
) *tree.CreateTable {
	stmt, err := parsers.ParseOne(
		context.TODO(),
		dialect.MYSQL,
		sql,
		1,
	)
	require.NoError(t, err)
	return stmt.(*tree.CreateTable)
}

func TestGetService(t *testing.T) {
	require.Nil(t, GetService(""))
}
