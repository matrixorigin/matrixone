// Copyright 2021 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// wantObj: expected overload id after rewrite; 0 means do not assert (e.g. count unchanged).
var wantCountStarObj = function.EncodeOverloadID(int32(function.STARCOUNT), 0)

func TestCheckAggOptimize_CountNotNull(t *testing.T) {
	tests := []struct {
		name              string
		node              *plan.Node
		wantObjName       string
		wantObj           int64 // 0 = skip assert (e.g. count unchanged)
		wantColumnMapSize int
	}{
		{
			name: "COUNT(not_null_col) should rewrite to starcount and set Obj to CountStar",
			node: &plan.Node{
				TableDef: &plan.TableDef{
					Cols: []*plan.ColDef{
						{Name: "id", Seqnum: 0, Typ: plan.Type{NotNullable: true}},
						{Name: "name", Seqnum: 1, Typ: plan.Type{NotNullable: false}},
					},
				},
				AggList: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "count"},
								Args: []*plan.Expr{
									{
										Expr: &plan.Expr_Col{
											Col: &plan.ColRef{ColPos: 0},
										},
									},
								},
							},
						},
					},
				},
			},
			wantObjName:       "starcount",
			wantObj:           wantCountStarObj,
			wantColumnMapSize: 0,
		},
		{
			name: "COUNT(lit) should rewrite to starcount and set Obj to CountStar",
			node: &plan.Node{
				TableDef: &plan.TableDef{Cols: []*plan.ColDef{}},
				AggList: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "count"},
								Args: []*plan.Expr{
									{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}},
								},
							},
						},
					},
				},
			},
			wantObjName:       "starcount",
			wantObj:           wantCountStarObj,
			wantColumnMapSize: 0,
		},
		{
			name: "COUNT(nullable_col) should not rewrite",
			node: &plan.Node{
				TableDef: &plan.TableDef{
					Cols: []*plan.ColDef{
						{Name: "id", Seqnum: 0, Typ: plan.Type{NotNullable: true}},
						{Name: "name", Seqnum: 1, Typ: plan.Type{NotNullable: false}},
					},
				},
				AggList: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "count"},
								Args: []*plan.Expr{
									{
										Expr: &plan.Expr_Col{
											Col: &plan.ColRef{ColPos: 1},
										},
									},
								},
							},
						},
					},
				},
			},
			wantObjName:       "count",
			wantObj:           0,
			wantColumnMapSize: 1,
		},
		{
			name: "COUNT(NULL) should not rewrite to starcount",
			node: &plan.Node{
				TableDef: &plan.TableDef{Cols: []*plan.ColDef{}},
				AggList: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "count"},
								Args: []*plan.Expr{
									{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}},
								},
							},
						},
					},
				},
			},
			wantObjName:       "",
			wantObj:           0,
			wantColumnMapSize: 0,
		},
		{
			name: "COUNT(DISTINCT not_null_col) should not optimize",
			node: &plan.Node{
				TableDef: &plan.TableDef{
					Cols: []*plan.ColDef{
						{Name: "id", Seqnum: 0, Typ: plan.Type{NotNullable: true}},
					},
				},
				AggList: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{
									ObjName: "count",
									Obj:     0,
								},
								Args: []*plan.Expr{
									{
										Expr: &plan.Expr_Col{
											Col: &plan.ColRef{ColPos: 0},
										},
									},
								},
							},
						},
					},
				},
			},
			wantObjName:       "",
			wantObj:           0,
			wantColumnMapSize: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set Distinct flag if test name contains "DISTINCT"
			if tt.name == "COUNT(DISTINCT not_null_col) should not optimize" {
				tt.node.AggList[0].Expr.(*plan.Expr_F).F.Func.Obj = -9223372036854775808
			}

			partialResults, partialResultTypes, columnMap := checkAggOptimize(tt.node)

			if tt.wantObjName == "" {
				require.Nil(t, partialResults)
				require.Nil(t, partialResultTypes)
				require.Nil(t, columnMap)
			} else {
				require.NotNil(t, partialResults)
				require.NotNil(t, partialResultTypes)
				require.NotNil(t, columnMap)

				actualObjName := tt.node.AggList[0].Expr.(*plan.Expr_F).F.Func.ObjName
				require.Equal(t, tt.wantObjName, actualObjName)
				if tt.wantObj != 0 {
					actualObj := tt.node.AggList[0].Expr.(*plan.Expr_F).F.Func.Obj
					require.Equal(t, tt.wantObj, actualObj, "Obj (overload id) must be CountStar so runtime uses countStarExec")
				}
				require.Equal(t, tt.wantColumnMapSize, len(columnMap))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Zonemap MIN/MAX eligibility for the aggregate metadata fast path.
//
// tae zonemaps intentionally skip T_decimal256 maintenance (zm.go UpdateZM /
// BatchUpdateZM), so they can never supply the block MIN/MAX that
// evalAggOptimize would substitute for real rows. The complete AggList must be
// validated, in every ordering, before the fast path is admitted for any
// aggregate in the list.

func zmAggColDef(name string, seq uint32, colType types.T, notNullable bool) *plan.ColDef {
	return &plan.ColDef{
		Name:   name,
		Seqnum: seq,
		Typ:    plan.Type{Id: int32(colType), NotNullable: notNullable},
	}
}

func zmAggNode(cols []*plan.ColDef, aggs ...*plan.Expr) *plan.Node {
	return &plan.Node{
		TableDef: &plan.TableDef{Cols: cols},
		AggList:  aggs,
	}
}

func zmAggColExpr(colPos int32) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: colPos}}}
}

func zmAggLitExpr(val int64) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
		Value: &plan.Literal_I64Val{I64Val: val},
	}}}
}

func zmAggNullLitExpr() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
}

// starcount ignores its argument, but checkAggOptimize reads Args[0].
func zmAggConstExpr() *plan.Expr {
	return zmAggLitExpr(0)
}

func zmAggFuncExpr(name string, arg *plan.Expr, obj int64) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: name, Obj: obj},
		Args: []*plan.Expr{arg},
	}}}
}

func TestCheckAggOptimize_ZmMinMaxEligibility(t *testing.T) {
	d256 := []*plan.ColDef{zmAggColDef("d256", 0, types.T_decimal256, false)}
	d64 := []*plan.ColDef{zmAggColDef("d64", 0, types.T_decimal64, false)}
	d128 := []*plan.ColDef{zmAggColDef("d128", 0, types.T_decimal128, false)}
	i64Nullable := []*plan.ColDef{zmAggColDef("i", 0, types.T_int64, false)}
	i64NotNull := []*plan.ColDef{zmAggColDef("i", 0, types.T_int64, true)}
	i64PlusD256 := []*plan.ColDef{
		zmAggColDef("i", 0, types.T_int64, false),
		zmAggColDef("d256", 1, types.T_decimal256, false),
	}
	notNullPlusD128 := []*plan.ColDef{
		zmAggColDef("i", 0, types.T_int64, true),
		zmAggColDef("d128", 1, types.T_decimal128, false),
	}
	twoNullable := []*plan.ColDef{
		zmAggColDef("d128", 0, types.T_decimal128, false),
		zmAggColDef("d64", 1, types.T_decimal64, false),
	}

	tests := []struct {
		name         string
		node         *plan.Node
		wantEligible bool
		wantColMap   int
		wantStarAt   int // agg index rewritten to starcount; -1 = assert none
	}{
		{
			name:         "min over decimal256 declines",
			node:         zmAggNode(d256, zmAggFuncExpr("min", zmAggColExpr(0), 0)),
			wantEligible: false,
			wantStarAt:   -1,
		},
		{
			name:         "max over decimal256 declines",
			node:         zmAggNode(d256, zmAggFuncExpr("max", zmAggColExpr(0), 0)),
			wantEligible: false,
			wantStarAt:   -1,
		},
		{
			name: "count literal before min over decimal256 declines",
			node: zmAggNode(d256,
				zmAggFuncExpr("count", zmAggLitExpr(1), 0),
				zmAggFuncExpr("min", zmAggColExpr(0), 0)),
			wantEligible: false,
			wantStarAt:   0,
		},
		{
			name: "min over decimal256 before count literal declines",
			node: zmAggNode(d256,
				zmAggFuncExpr("min", zmAggColExpr(0), 0),
				zmAggFuncExpr("count", zmAggLitExpr(1), 0)),
			wantEligible: false,
			wantStarAt:   -1,
		},
		{
			name: "starcount beside max over decimal256 declines",
			node: zmAggNode(d256,
				zmAggFuncExpr("starcount", zmAggConstExpr(), 0),
				zmAggFuncExpr("max", zmAggColExpr(0), 0)),
			wantEligible: false,
			wantStarAt:   -1,
		},
		{
			name: "count nullable column before min over decimal256 declines",
			node: zmAggNode(i64PlusD256,
				zmAggFuncExpr("count", zmAggColExpr(0), 0),
				zmAggFuncExpr("min", zmAggColExpr(1), 0)),
			wantEligible: false,
			wantStarAt:   -1,
		},
		{
			name: "count distinct column after count literal declines whole node",
			node: zmAggNode(i64Nullable,
				zmAggFuncExpr("count", zmAggLitExpr(1), 0),
				zmAggFuncExpr("count", zmAggColExpr(0),
					int64(uint64(function.EncodeOverloadID(function.COUNT, 0))|uint64(function.Distinct)))),
			wantEligible: false,
			wantStarAt:   0,
		},
		{
			name: "count null literal before min over decimal64 still declines",
			node: zmAggNode(d64,
				zmAggFuncExpr("count", zmAggNullLitExpr(), 0),
				zmAggFuncExpr("min", zmAggColExpr(0), 0)),
			wantEligible: false,
			wantStarAt:   -1,
		},
		{
			name:         "min over int64 admits",
			node:         zmAggNode(i64Nullable, zmAggFuncExpr("min", zmAggColExpr(0), 0)),
			wantEligible: true,
			wantColMap:   1,
			wantStarAt:   -1,
		},
		{
			name:         "min over decimal64 admits",
			node:         zmAggNode(d64, zmAggFuncExpr("min", zmAggColExpr(0), 0)),
			wantEligible: true,
			wantColMap:   1,
			wantStarAt:   -1,
		},
		{
			name:         "max over decimal128 admits",
			node:         zmAggNode(d128, zmAggFuncExpr("max", zmAggColExpr(0), 0)),
			wantEligible: true,
			wantColMap:   1,
			wantStarAt:   -1,
		},
		{
			name: "count literal before min over decimal64 admits with min column mapped",
			node: zmAggNode(d64,
				zmAggFuncExpr("count", zmAggLitExpr(1), 0),
				zmAggFuncExpr("min", zmAggColExpr(0), 0)),
			wantEligible: true,
			wantColMap:   1,
			wantStarAt:   0,
		},
		{
			name: "min over decimal64 before count literal admits",
			node: zmAggNode(d64,
				zmAggFuncExpr("min", zmAggColExpr(0), 0),
				zmAggFuncExpr("count", zmAggLitExpr(1), 0)),
			wantEligible: true,
			wantColMap:   1,
			wantStarAt:   1,
		},
		{
			name: "count not-null column rewritten beside max over decimal128 admits",
			node: zmAggNode(notNullPlusD128,
				zmAggFuncExpr("count", zmAggColExpr(0), 0),
				zmAggFuncExpr("max", zmAggColExpr(1), 0)),
			wantEligible: true,
			wantColMap:   1,
			wantStarAt:   0,
		},
		{
			name: "count nullable column with min over decimal64 admits both mapped",
			node: zmAggNode(twoNullable,
				zmAggFuncExpr("count", zmAggColExpr(0), 0),
				zmAggFuncExpr("min", zmAggColExpr(1), 0)),
			wantEligible: true,
			wantColMap:   2,
			wantStarAt:   -1,
		},
		{
			// Supported metadata fast path stays eligible for a not-null count
			// plus a min that no columnMap entry is needed for.
			name:         "count not-null column alone admits",
			node:         zmAggNode(i64NotNull, zmAggFuncExpr("count", zmAggColExpr(0), 0)),
			wantEligible: true,
			wantColMap:   0,
			wantStarAt:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partialResults, partialResultTypes, columnMap := checkAggOptimize(tt.node)

			if !tt.wantEligible {
				require.Nil(t, partialResults)
				require.Nil(t, partialResultTypes)
				require.Nil(t, columnMap)
			} else {
				require.NotNil(t, partialResults)
				require.NotNil(t, partialResultTypes)
				require.NotNil(t, columnMap)
				require.Equal(t, tt.wantColMap, len(columnMap))
				require.Len(t, partialResults, len(tt.node.AggList))
			}

			if tt.wantStarAt >= 0 {
				agg := tt.node.AggList[tt.wantStarAt].Expr.(*plan.Expr_F).F
				require.Equal(t, "starcount", agg.Func.ObjName)
				require.Equal(t, wantCountStarObj, agg.Func.Obj)
			}
		})
	}
}

func TestForceSingleScan_ZmMinMaxEligibility(t *testing.T) {
	d256 := []*plan.ColDef{zmAggColDef("d256", 0, types.T_decimal256, false)}
	i64 := []*plan.ColDef{zmAggColDef("i", 0, types.T_int64, false)}

	// Decimal256 min/max cannot use zonemap metadata: the whole node must keep
	// a normal scan in every aggregate ordering.
	require.False(t, forceSingleScan(zmAggNode(d256, zmAggFuncExpr("min", zmAggColExpr(0), 0))))
	require.False(t, forceSingleScan(zmAggNode(d256, zmAggFuncExpr("max", zmAggColExpr(0), 0))))
	require.False(t, forceSingleScan(zmAggNode(d256,
		zmAggFuncExpr("count", zmAggLitExpr(1), 0),
		zmAggFuncExpr("min", zmAggColExpr(0), 0))))
	require.False(t, forceSingleScan(zmAggNode(d256,
		zmAggFuncExpr("min", zmAggColExpr(0), 0),
		zmAggFuncExpr("count", zmAggLitExpr(1), 0))))
	// Supported column metadata keeps the fast path and its single scan.
	require.True(t, forceSingleScan(zmAggNode(i64, zmAggFuncExpr("min", zmAggColExpr(0), 0))))
}

// Actual Scope.aggOptimize consumer closure: a whole-node eligibility decline
// must keep the exact relation data the scan would read and must not collect
// ranges, build readers, or publish partials. The capture relation counts
// Ranges and BuildReaders calls; none of them may run on the declined path.
//
// Canonicalization trace for the aggregate shapes: the binder represents
// COUNT(*) as starcount(1) (pkg/sql/plan/opt_misc.go) and the planner rewrites
// count(not_null_col) to starcount (pkg/sql/plan/build_util.go
// RewriteCountNotNullColToStarcount), so the "starcount beside max" row models
// the planner-shaped aggregate that reaches compile; the count(1) rows keep
// the compile-side contract covered in both aggregate positions.
func TestScopeAggOptimizeDeclinesWholeNode(t *testing.T) {
	d256Cols := []*plan.ColDef{zmAggColDef("d256", 0, types.T_decimal256, false)}
	intAndDec64 := []*plan.ColDef{
		zmAggColDef("i", 0, types.T_int64, false),
		zmAggColDef("d64", 1, types.T_decimal64, false),
	}

	distinctObj := int64(uint64(function.EncodeOverloadID(function.COUNT, 0)) | uint64(function.Distinct))

	tests := []struct {
		name            string
		cols            []*plan.ColDef
		aggs            []*plan.Expr
		wantStarcountAt int // index rewritten to starcount by the real call; -1 = none
	}{
		{
			name:            "min over decimal256 keeps persisted relation data",
			cols:            d256Cols,
			aggs:            []*plan.Expr{zmAggFuncExpr("min", zmAggColExpr(0), 0)},
			wantStarcountAt: -1,
		},
		{
			name:            "max over decimal256 keeps persisted relation data",
			cols:            d256Cols,
			aggs:            []*plan.Expr{zmAggFuncExpr("max", zmAggColExpr(0), 0)},
			wantStarcountAt: -1,
		},
		{
			name: "starcount beside max over decimal256 keeps persisted relation data",
			cols: d256Cols,
			aggs: []*plan.Expr{
				zmAggFuncExpr("starcount", zmAggConstExpr(), 0),
				zmAggFuncExpr("max", zmAggColExpr(0), 0),
			},
			wantStarcountAt: -1,
		},
		{
			name: "count literal before min over decimal256 keeps persisted relation data",
			cols: d256Cols,
			aggs: []*plan.Expr{
				zmAggFuncExpr("count", zmAggLitExpr(1), 0),
				zmAggFuncExpr("min", zmAggColExpr(0), 0),
			},
			wantStarcountAt: 0,
		},
		{
			name: "min over decimal256 before count literal keeps persisted relation data",
			cols: d256Cols,
			aggs: []*plan.Expr{
				zmAggFuncExpr("min", zmAggColExpr(0), 0),
				zmAggFuncExpr("count", zmAggLitExpr(1), 0),
			},
			wantStarcountAt: -1,
		},
		{
			// Nearest control: the retention behavior is not decimal256
			// specific. COUNT(DISTINCT) is refused by the pre-existing
			// eligibility rule at the same scope gate.
			name: "count distinct before min over decimal64 keeps persisted relation data",
			cols: intAndDec64,
			aggs: []*plan.Expr{
				zmAggFuncExpr("count", zmAggColExpr(0), distinctObj),
				zmAggFuncExpr("min", zmAggColExpr(1), 0),
			},
			wantStarcountAt: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Two persisted blocks: a wrongly admitted node would enter the
			// DataCnt > 1 metadata fast path and replace NodeInfo.Data.
			data := readutil.NewBlockListRelationData(0)
			data.AppendBlockInfo(&objectio.BlockInfo{})
			data.AppendBlockInfo(&objectio.BlockInfo{})
			require.Equal(t, 2, data.DataCnt())

			node := zmAggNode(tt.cols, tt.aggs...)
			s := &Scope{
				DataSource: &Source{node: node},
				NodeInfo:   engine.Node{Mcpu: 1, Data: data},
			}
			rel := &readerPathCaptureRelation{rangesData: data}

			require.NoError(t, s.aggOptimize(&Compile{}, rel, context.Background()))

			require.Same(t, data, s.NodeInfo.Data, "declined node must keep the exact relation data")
			require.Equal(t, 2, s.NodeInfo.Data.DataCnt())
			require.False(t, s.StarCountOnly)
			require.Nil(t, s.StarCountMergeGroup)
			require.Zero(t, rel.rangesCalls, "no range collection on a declined node")
			require.Zero(t, rel.buildReadersCalls, "no reader construction on a declined node")

			if tt.wantStarcountAt >= 0 {
				fn := tt.aggs[tt.wantStarcountAt].Expr.(*plan.Expr_F).F
				require.Equal(t, "starcount", fn.Func.ObjName)
				require.Equal(t, wantCountStarObj, fn.Func.Obj)
			}
		})
	}
}
