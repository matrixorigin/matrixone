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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCheckAggOptimize_CountNotNull(t *testing.T) {
	tests := []struct {
		name              string
		node              *plan.Node
		wantObjName       string
		wantColumnMapSize int
	}{
		{
			name: "COUNT(not_null_col) should rewrite to starcount",
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
			wantColumnMapSize: 1,
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
				require.Equal(t, tt.wantColumnMapSize, len(columnMap))
			}
		})
	}
}
