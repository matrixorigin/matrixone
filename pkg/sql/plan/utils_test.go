// Copyright 2024 Matrix Origin
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

package plan

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_removeIf(t *testing.T) {
	strs := []string{"abc", "bc", "def"}

	del1 := make(map[string]struct{})
	del1["abc"] = struct{}{}
	res1 := RemoveIf[string](strs, func(t string) bool {
		return Find[string](del1, t)
	})
	assert.Equal(t, []string{"bc", "def"}, res1)

	del2 := make(map[string]struct{})
	for _, str := range strs {
		del2[str] = struct{}{}
	}
	res2 := RemoveIf[string](strs, func(t string) bool {
		return Find[string](del2, t)
	})
	assert.Equal(t, []string{}, res2)

	assert.Equal(t, []string(nil), RemoveIf[string](nil, nil))
}

func TestOffsetToString(t *testing.T) {
	tests := []struct {
		offset int
		want   string
	}{
		{3600, "+01:00"},
		{7200, "+02:00"},
		{-3600, "-01:00"},
		{-7200, "-02:00"},
		{0, "+00:00"},
		{3660, "+01:01"},
		{-3660, "-01:01"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("offset %d", tt.offset), func(t *testing.T) {
			if got := offsetToString(tt.offset); got != tt.want {
				t.Errorf("offsetToString(%d) = %v, want %v", tt.offset, got, tt.want)
			}
		})
	}
}

func TestInitStageS3Param(t *testing.T) {
	param := &tree.ExternParam{}
	u, err := url.Parse("s3://bucket/path?offset=0")
	require.Nil(t, err)
	s := stage.StageDef{Url: u}
	err = InitStageS3Param(param, s)
	require.NotNil(t, err)

	param = &tree.ExternParam{}
	u, err = url.Parse("https://bucket/path?offset=0")
	require.Nil(t, err)
	s = stage.StageDef{Url: u}
	err = InitStageS3Param(param, s)
	require.NotNil(t, err)

	param = &tree.ExternParam{}
	u, err = url.Parse("s3://bucket/path")
	require.Nil(t, err)
	s = stage.StageDef{Url: u,
		Credentials: map[string]string{"aws_key_id": "abc", "aws_secret_key": "secret", "aws_region": "region", "endpoint": "endpoint", "provider": "amazon"},
		Name:        "mystage",
		Id:          1000}
	err = InitStageS3Param(param, s)
	require.Nil(t, err)
}

func TestHandleOptimizerHints(t *testing.T) {
	builder := &QueryBuilder{}
	handleOptimizerHints("skipDedup=1", builder)
	require.Equal(t, 1, builder.optimizerHints.skipDedup)
}

func TestMakeCPKEYRuntimeFilter(t *testing.T) {
	name2colidx := make(map[string]int32, 0)
	name2colidx[catalog.CPrimaryKeyColName] = 0
	typ := plan.Type{
		Id: int32(types.T_varchar),
	}
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{
				Name: "catalog.CPrimaryKeyColName",
				Typ:  typ,
			},
		},
		Name2ColIndex: name2colidx,
	}
	expr := GetColExpr(typ, 0, 0)
	MakeCPKEYRuntimeFilter(0, 0, expr, tableDef, false)
}

func TestDbNameOfObjRef(t *testing.T) {
	type args struct {
		objRef *ObjectRef
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case 1",
			args: args{
				objRef: &ObjectRef{
					SchemaName: "db",
				},
			},
			want: "db",
		},
		{
			name: "case 2",
			args: args{
				objRef: &ObjectRef{
					SchemaName:       "whatever",
					SubscriptionName: "sub",
				},
			},
			want: "sub",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, DbNameOfObjRef(tt.args.objRef), "DbNameOfObjRef(%v)", tt.args.objRef)
		})
	}
}

func TestDoResolveTimeStamp(t *testing.T) {
	tests := []struct {
		timeStamp string
		expected  int64
		expectErr bool
	}{
		//{"2023-10-01 12:00:00", 1696132800000000000, false},
		{"", 0, true},
		{"2023-10-01", 0, true},
		{"invalid-timestamp", 0, true},
		{"2023-10-01 25:00:00", 0, true}, // Invalid hour
	}

	for _, test := range tests {
		result, err := doResolveTimeStamp(test.timeStamp)
		if test.expectErr {
			if err == nil {
				t.Errorf("expected an error for timestamp %s, got none", test.timeStamp)
			}
		} else {
			if err != nil {
				t.Errorf("did not expect an error for timestamp %s, got %v", test.timeStamp, err)
			}
			if result != test.expected {
				t.Errorf("for timestamp %s, expected %d, got %d", test.timeStamp, test.expected, result)
			}
		}
	}
}

func TestReplaceParamVals(t *testing.T) {
	// Setup test cases
	tests := []struct {
		name      string
		plan      *Plan
		paramVals []any
		wantErr   bool
	}{
		{
			name: "empty param values",
			plan: &Plan{
				Plan: &plan.Plan_Tcl{},
			},
			paramVals: []any{},
			wantErr:   false,
		},
		{
			name: "multiple param values",
			plan: &Plan{
				Plan: &plan.Plan_Tcl{},
			},
			paramVals: []any{42, "string", 3.14, true, time.Now(), nil},
			wantErr:   false,
		},
		{
			name: "complex plan with params",
			plan: &Plan{
				Plan: &plan.Plan_Query{
					Query: &plan.Query{
						Nodes: []*plan.Node{
							{
								ProjectList: []*plan.Expr{
									{
										Expr: &plan.Expr_P{
											P: &plan.ParamRef{
												Pos: 0,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			paramVals: []any{"value1", 123},
			wantErr:   false,
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := replaceParamVals(ctx, tt.plan, tt.paramVals)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Test_extractColRefInFilter(t *testing.T) {
	tests := []struct {
		name    string
		expr    *plan.Expr
		wantCol *ColRef
		wantNil bool
		desc    string
	}{
		{
			name: "simple column reference",
			expr: &plan.Expr{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 2,
						Name:   "col1",
					},
				},
			},
			wantCol: &ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "col1",
			},
			wantNil: false,
			desc:    "Direct column reference should return the column",
		},
		{
			name: "comparison with literal - col = 1",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 2,
										Name:   "col1",
									},
								},
							},
							{
								Expr: &plan.Expr_Lit{
									Lit: &plan.Literal{
										Value: &plan.Literal_I64Val{I64Val: 1},
									},
								},
							},
						},
					},
				},
			},
			wantCol: &ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "col1",
			},
			wantNil: false,
			desc:    "Comparison with literal should return the column",
		},
		{
			name: "comparison with same column - col = trim(col)",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 2,
										Name:   "col1",
									},
								},
							},
							{
								Expr: &plan.Expr_F{
									F: &plan.Function{
										Func: &plan.ObjectRef{ObjName: "trim"},
										Args: []*plan.Expr{
											{
												Expr: &plan.Expr_Col{
													Col: &plan.ColRef{
														RelPos: 1,
														ColPos: 2,
														Name:   "col1",
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
			wantCol: &ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "col1",
			},
			wantNil: false,
			desc:    "Comparison with function of same column should return the column",
		},
		{
			name: "comparison with different column - col1 = col2",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 2,
										Name:   "col1",
									},
								},
							},
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 3,
										Name:   "col2",
									},
								},
							},
						},
					},
				},
			},
			wantNil: true,
			desc:    "Comparison with different column should return nil",
		},
		{
			name: "nested function - func(col) > 2",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: ">"},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_F{
									F: &plan.Function{
										Func: &plan.ObjectRef{ObjName: "upper"},
										Args: []*plan.Expr{
											{
												Expr: &plan.Expr_Col{
													Col: &plan.ColRef{
														RelPos: 1,
														ColPos: 2,
														Name:   "col1",
													},
												},
											},
										},
									},
								},
							},
							{
								Expr: &plan.Expr_Lit{
									Lit: &plan.Literal{
										Value: &plan.Literal_I64Val{I64Val: 2},
									},
								},
							},
						},
					},
				},
			},
			wantCol: &ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "col1",
			},
			wantNil: false,
			desc:    "Nested function call should return the column",
		},
		{
			name: "logical operator - and(col, col)",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "and"},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 2,
										Name:   "col1",
									},
								},
							},
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 2,
										Name:   "col1",
									},
								},
							},
						},
					},
				},
			},
			wantCol: &ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "col1",
			},
			wantNil: false,
			desc:    "Logical operator with same column should return the column",
		},
		{
			name: "logical operator - and(col1, col2)",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "and"},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 2,
										Name:   "col1",
									},
								},
							},
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 3,
										Name:   "col2",
									},
								},
							},
						},
					},
				},
			},
			wantNil: true,
			desc:    "Logical operator with different columns should return nil",
		},
		{
			name: "logical operator - and(col, 1)",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "and"},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 2,
										Name:   "col1",
									},
								},
							},
							{
								Expr: &plan.Expr_Lit{
									Lit: &plan.Literal{
										Value: &plan.Literal_I64Val{I64Val: 1},
									},
								},
							},
						},
					},
				},
			},
			wantCol: &ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "col1",
			},
			wantNil: false,
			desc:    "Logical operator with literal should return the column",
		},
		{
			name: "function with no args",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "now"},
						Args: []*plan.Expr{},
					},
				},
			},
			wantNil: true,
			desc:    "Function with no arguments should return nil",
		},
		{
			name: "function with first arg without column",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Lit{
									Lit: &plan.Literal{
										Value: &plan.Literal_I64Val{I64Val: 1},
									},
								},
							},
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 2,
										Name:   "col1",
									},
								},
							},
						},
					},
				},
			},
			wantNil: true,
			desc:    "Function with first arg as literal should return nil",
		},
		{
			name: "cast function",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "cast"},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 2,
										Name:   "col1",
									},
								},
							},
							{
								Expr: &plan.Expr_T{
									T: &plan.TargetType{},
								},
							},
						},
					},
				},
			},
			wantCol: &ColRef{
				RelPos: 1,
				ColPos: 2,
				Name:   "col1",
			},
			wantNil: false,
			desc:    "Cast function should return the column",
		},
		{
			name: "comparison with cast of different column - m.id = cast(o.id as int)",
			expr: &plan.Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 1,
										ColPos: 0,
										Name:   "m.id",
									},
								},
							},
							{
								Expr: &plan.Expr_F{
									F: &plan.Function{
										Func: &plan.ObjectRef{ObjName: "cast"},
										Args: []*plan.Expr{
											{
												Expr: &plan.Expr_Col{
													Col: &plan.ColRef{
														RelPos: 2,
														ColPos: 0,
														Name:   "o.id",
													},
												},
											},
											{
												Expr: &plan.Expr_T{
													T: &plan.TargetType{},
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
			wantNil: true,
			desc:    "Comparison with cast of different column should return nil",
		},
		{
			name: "non-expression type",
			expr: &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_I64Val{I64Val: 1},
					},
				},
			},
			wantNil: true,
			desc:    "Literal expression should return nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractColRefInFilter(tt.expr)
			if tt.wantNil {
				assert.Nil(t, got, tt.desc)
			} else {
				require.NotNil(t, got, tt.desc)
				assert.Equal(t, tt.wantCol.RelPos, got.RelPos, "RelPos should match")
				assert.Equal(t, tt.wantCol.ColPos, got.ColPos, "ColPos should match")
				assert.Equal(t, tt.wantCol.Name, got.Name, "Name should match")
			}
		})
	}
}
