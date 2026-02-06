// Copyright 2024 Matrix Origin
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

package explain

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestGetDecimalCastOptimizationHint(t *testing.T) {
	tests := []struct {
		name       string
		filterList []*plan.Expr
		wantHint   bool
	}{
		{
			name: "Cast with precision mismatch",
			filterList: []*plan.Expr{
				{
					Typ: plan.Type{Id: 19, Width: 10, Scale: 9}, // decimal64(10,9)
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: &plan.ObjectRef{ObjName: "="},
							Args: []*plan.Expr{
								{
									Typ: plan.Type{Id: 19, Width: 10, Scale: 9},
									Expr: &plan.Expr_F{
										F: &plan.Function{
											Func: &plan.ObjectRef{ObjName: "cast"},
											Args: []*plan.Expr{
												{
													Typ: plan.Type{Id: 19, Width: 10, Scale: 2}, // column decimal64(10,2)
													Expr: &plan.Expr_Col{
														Col: &plan.ColRef{Name: "price"},
													},
												},
											},
										},
									},
								},
								{
									Typ: plan.Type{Id: 19, Width: 10, Scale: 9}, // constant decimal64(10,9)
									Expr: &plan.Expr_Lit{
										Lit: &plan.Literal{
											Isnull: false,
											Value: &plan.Literal_Decimal64Val{
												Decimal64Val: &plan.Decimal64{A: 99991234567},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantHint: true,
		},
		{
			name: "No cast - no hint",
			filterList: []*plan.Expr{
				{
					Typ: plan.Type{Id: 19, Width: 10, Scale: 2},
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: &plan.ObjectRef{ObjName: "="},
							Args: []*plan.Expr{
								{
									Typ: plan.Type{Id: 19, Width: 10, Scale: 2},
									Expr: &plan.Expr_Col{
										Col: &plan.ColRef{Name: "price"},
									},
								},
								{
									Typ: plan.Type{Id: 19, Width: 10, Scale: 2},
									Expr: &plan.Expr_Lit{
										Lit: &plan.Literal{
											Isnull: false,
											Value: &plan.Literal_Decimal64Val{
												Decimal64Val: &plan.Decimal64{A: 9999},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantHint: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hint := getDecimalCastOptimizationHint(tt.filterList)
			if tt.wantHint {
				require.NotEmpty(t, hint, "Expected optimization hint but got none")
				require.Contains(t, hint, "HINT", "Hint should contain 'HINT' keyword")
			} else {
				require.Empty(t, hint, "Expected no hint but got: %s", hint)
			}
		})
	}
}

func TestIsDecimalType(t *testing.T) {
	tests := []struct {
		name   string
		typeId int32
		want   bool
	}{
		{"decimal64", 19, true},
		{"decimal128", 20, true},
		{"int64", 6, false},
		{"float64", 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDecimalType(tt.typeId)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsLiteral(t *testing.T) {
	tests := []struct {
		name string
		expr *plan.Expr
		want bool
	}{
		{
			name: "Literal constant",
			expr: &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value:  &plan.Literal_I64Val{I64Val: 100},
					},
				},
			},
			want: true,
		},
		{
			name: "Column reference",
			expr: &plan.Expr{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{Name: "price"},
				},
			},
			want: false,
		},
		{
			name: "Nil expression",
			expr: nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isLiteral(tt.expr)
			require.Equal(t, tt.want, got)
		})
	}
}
