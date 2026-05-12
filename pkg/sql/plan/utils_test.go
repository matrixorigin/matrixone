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

// TestParseHiveOptionKV verifies hive key parsing via Init*Param helper.
// Covers legacy-JSON fallback where Option[] still carries hive_partitioning /
// hive_partition_columns (stripHiveOptionKeys did not run). The key behavior:
// each key's skip-if-set guard must only inspect its own field; otherwise a
// reversed option order silently drops hive_partitioning=true.
func TestParseHiveOptionKV(t *testing.T) {
	t.Run("canonical order applies both", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Option = []string{
			"hive_partitioning", "true",
			"hive_partition_columns", "year,month",
		}
		for i := 0; i < len(param.Option); i += 2 {
			handled, err := parseHiveOptionKV(param, param.Option[i], param.Option[i+1])
			require.True(t, handled)
			require.NoError(t, err)
		}
		assert.True(t, param.HivePartitioning)
		assert.Equal(t, []string{"year", "month"}, param.HivePartitionCols)
	})

	// Each key's skip-if-set guard must inspect only its own field. A coupled
	// guard that treats non-empty HivePartitionCols as "already handled" would
	// silently drop hive_partitioning=true when cols appeared first in Option[],
	// leaving the table mis-classified as non-hive. Keep this case as a
	// regression for that contract.
	t.Run("reversed order still applies both", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Option = []string{
			"hive_partition_columns", "year,month",
			"hive_partitioning", "true",
		}
		for i := 0; i < len(param.Option); i += 2 {
			handled, err := parseHiveOptionKV(param, param.Option[i], param.Option[i+1])
			require.True(t, handled, "key=%s", param.Option[i])
			require.NoError(t, err)
		}
		assert.True(t, param.HivePartitioning,
			"hive_partitioning must not be dropped when cols appeared first in Option[]")
		assert.Equal(t, []string{"year", "month"}, param.HivePartitionCols)
	})

	t.Run("pre-populated HivePartitioning is not overwritten", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.HivePartitioning = true
		handled, err := parseHiveOptionKV(param, "hive_partitioning", "false")
		require.True(t, handled)
		require.NoError(t, err)
		assert.True(t, param.HivePartitioning, "skip-if-set must not flip true→false")
	})

	t.Run("pre-populated HivePartitionCols is not overwritten", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.HivePartitionCols = []string{"year"}
		handled, err := parseHiveOptionKV(param, "hive_partition_columns", "month,day")
		require.True(t, handled)
		require.NoError(t, err)
		assert.Equal(t, []string{"year"}, param.HivePartitionCols)
	})

	t.Run("invalid bool value reports error", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Ctx = context.Background()
		handled, err := parseHiveOptionKV(param, "hive_partitioning", "yes")
		require.True(t, handled)
		require.Error(t, err)
	})

	t.Run("non-hive key returns not-handled", func(t *testing.T) {
		param := &tree.ExternParam{}
		handled, err := parseHiveOptionKV(param, "filepath", "/data/")
		assert.False(t, handled)
		assert.NoError(t, err)
	})

	t.Run("false value", func(t *testing.T) {
		param := &tree.ExternParam{}
		handled, err := parseHiveOptionKV(param, "hive_partitioning", "false")
		require.True(t, handled)
		require.NoError(t, err)
		assert.False(t, param.HivePartitioning)
	})

	t.Run("cols lowercased and trimmed", func(t *testing.T) {
		param := &tree.ExternParam{}
		handled, err := parseHiveOptionKV(param, "hive_partition_columns", "  Year ,  MONTH  , , Day ")
		require.True(t, handled)
		require.NoError(t, err)
		assert.Equal(t, []string{"year", "month", "day"}, param.HivePartitionCols)
	})
}

// -------------------------------------------------------------------------
// Init*Param legacy-JSON hive branches and plain happy paths.
// -------------------------------------------------------------------------

// TestInitInfileParam_Plain exercises the normal option pass-through with
// filepath/format/compression/jsondata so the non-hive arms are covered too.
func TestInitInfileParam_Plain(t *testing.T) {
	param := &tree.ExternParam{}
	param.Option = []string{
		"filepath", "/data/x",
		"compression", "gzip",
		"format", "parquet",
	}
	require.NoError(t, InitInfileParam(param))
	assert.Equal(t, "/data/x", param.Filepath)
	assert.Equal(t, "gzip", param.CompressType)
	assert.Equal(t, "parquet", param.Format)

	// jsonline/jsondata branch
	param = &tree.ExternParam{}
	param.Option = []string{"filepath", "/f", "jsondata", "object"}
	require.NoError(t, InitInfileParam(param))
	assert.Equal(t, "object", param.JsonData)
	assert.Equal(t, "jsonline", param.Format)

	// csv default
	param = &tree.ExternParam{}
	param.Option = []string{"filepath", "/csv"}
	require.NoError(t, InitInfileParam(param))
	assert.Equal(t, "csv", param.Format)
}

// TestInitInfileParam_HiveLegacyOption exercises parseHiveOptionKV via
// InitInfileParam when Option[] still contains hive keys (simulating JSON
// that predates stripHiveOptionKeys).
func TestInitInfileParam_HiveLegacyOption(t *testing.T) {
	param := &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{
		"filepath", "/data/",
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "year,month",
	}
	require.NoError(t, InitInfileParam(param))
	assert.True(t, param.HivePartitioning)
	assert.Equal(t, []string{"year", "month"}, param.HivePartitionCols)
}

func TestInitInfileParam_Errors(t *testing.T) {
	param := &tree.ExternParam{}
	param.Ctx = context.Background()
	// Unknown format
	param.Option = []string{"filepath", "/x", "format", "orc"}
	require.Error(t, InitInfileParam(param))

	// Unknown jsondata
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"filepath", "/x", "jsondata", "ndjson"}
	require.Error(t, InitInfileParam(param))

	// Missing filepath
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"format", "parquet"}
	require.Error(t, InitInfileParam(param))

	// jsonline without jsondata
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"filepath", "/x", "format", "jsonline"}
	require.Error(t, InitInfileParam(param))

	// Unknown keyword
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"unknown", "val", "filepath", "/x"}
	require.Error(t, InitInfileParam(param))

	// Invalid hive_partitioning value
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"filepath", "/x", "format", "parquet", "hive_partitioning", "yes"}
	require.Error(t, InitInfileParam(param))

	// Columns with hive_partitioning disabled are rejected after legacy parsing.
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{
		"filepath", "/x",
		"format", "parquet",
		"hive_partitioning", "false",
		"hive_partition_columns", "year",
	}
	err := InitInfileParam(param)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires hive_partitioning='true'")
}

// TestInitS3Param_Plain exercises the S3 arm with normal options.
func TestInitS3Param_Plain(t *testing.T) {
	param := &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{
		"endpoint", "https://s3.example.com",
		"region", "us-west-2",
		"access_key_id", "AK",
		"secret_access_key", "SK",
		"bucket", "my-bucket",
		"filepath", "sales/",
		"compression", "none",
		"provider", "minio",
		"role_arn", "arn:aws:iam::111:role/R",
		"external_id", "ext",
		"format", "csv",
	}
	require.NoError(t, InitS3Param(param))
	assert.Equal(t, "https://s3.example.com", param.S3Param.Endpoint)
	assert.Equal(t, "my-bucket", param.S3Param.Bucket)
	assert.Equal(t, "sales/", param.Filepath)
	assert.Equal(t, "csv", param.Format)

	// jsondata jsonline path
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"bucket", "b", "jsondata", "array"}
	require.NoError(t, InitS3Param(param))
	assert.Equal(t, "jsonline", param.Format)
}

func TestInitS3Param_HiveLegacyOption(t *testing.T) {
	param := &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{
		"bucket", "b",
		"format", "csv",
		"hive_partitioning", "true",
		"hive_partition_columns", "year",
	}
	require.NoError(t, InitS3Param(param))
	assert.True(t, param.HivePartitioning)
	assert.Equal(t, []string{"year"}, param.HivePartitionCols)
}

func TestInitS3Param_Errors(t *testing.T) {
	param := &tree.ExternParam{}
	param.Ctx = context.Background()
	// Bad format
	param.Option = []string{"bucket", "b", "format", "orc"}
	require.Error(t, InitS3Param(param))

	// Bad jsondata
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"bucket", "b", "jsondata", "bad"}
	require.Error(t, InitS3Param(param))

	// jsonline without jsondata
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"bucket", "b", "format", "jsonline"}
	require.Error(t, InitS3Param(param))

	// Unknown key
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"bogus", "x"}
	require.Error(t, InitS3Param(param))

	// Invalid hive_partitioning boolean
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"bucket", "b", "format", "csv", "hive_partitioning", "maybe"}
	require.Error(t, InitS3Param(param))

	// Columns with hive_partitioning disabled are rejected after legacy parsing.
	param = &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{
		"bucket", "b",
		"format", "parquet",
		"hive_partitioning", "false",
		"hive_partition_columns", "year",
	}
	err := InitS3Param(param)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires hive_partitioning='true'")
}

// -------------------------------------------------------------------------
// build_ddl.go hive DDL helpers.
// -------------------------------------------------------------------------

func TestParseHiveOptionsFromRawOptions_AllPaths(t *testing.T) {
	ctx := context.Background()

	// Absent → (false, nil, nil)
	en, cols, err := parseHiveOptionsFromRawOptions(ctx, []string{"filepath", "/x"})
	require.NoError(t, err)
	assert.False(t, en)
	assert.Nil(t, cols)

	// Explicit false → (false, nil, nil)
	en, cols, err = parseHiveOptionsFromRawOptions(ctx, []string{"hive_partitioning", "false"})
	require.NoError(t, err)
	assert.False(t, en)
	assert.Nil(t, cols)

	// Columns without an enabled hive_partitioning flag are inconsistent.
	_, _, err = parseHiveOptionsFromRawOptions(ctx, []string{"hive_partition_columns", "year"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires hive_partitioning='true'")

	_, _, err = parseHiveOptionsFromRawOptions(ctx,
		[]string{"hive_partitioning", "false", "hive_partition_columns", "year"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires hive_partitioning='true'")

	// Invalid value → error
	_, _, err = parseHiveOptionsFromRawOptions(ctx, []string{"hive_partitioning", "yes"})
	require.Error(t, err)

	// true + empty cols → (true, nil, nil)  (caller enforces non-empty)
	en, cols, err = parseHiveOptionsFromRawOptions(ctx, []string{"hive_partitioning", "true"})
	require.NoError(t, err)
	assert.True(t, en)
	assert.Nil(t, cols)

	// true + cols — trimmed split
	en, cols, err = parseHiveOptionsFromRawOptions(ctx,
		[]string{"hive_partitioning", "TRUE", "hive_partition_columns", " year ,, month "})
	require.NoError(t, err)
	assert.True(t, en)
	assert.Equal(t, []string{"year", "month"}, cols)
}

func TestRejectDuplicateKeys(t *testing.T) {
	ctx := context.Background()
	// No duplicates → nil.
	err := rejectDuplicateKeys(ctx,
		[]string{"format", "parquet", "filepath", "/x"},
		[]string{"format", "filepath"})
	assert.NoError(t, err)

	// Key not in list is tolerated.
	err = rejectDuplicateKeys(ctx,
		[]string{"compression", "gzip", "compression", "none"},
		[]string{"format"})
	assert.NoError(t, err)

	// Duplicate of a watched key → error.
	err = rejectDuplicateKeys(ctx,
		[]string{"format", "parquet", "format", "csv"},
		[]string{"format"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate option key 'format'")
}

func TestGetRawOption(t *testing.T) {
	opts := []string{"Filepath", "/x", "format", "parquet"}
	assert.Equal(t, "/x", getRawOption(opts, "filepath"))
	assert.Equal(t, "parquet", getRawOption(opts, "format"))
	assert.Equal(t, "", getRawOption(opts, "bucket"))
}

func TestStripHiveOptionKeys(t *testing.T) {
	in := []string{
		"filepath", "/x",
		"hive_partitioning", "true",
		"format", "parquet",
		"hive_partition_columns", "year,month",
		"compression", "gzip",
	}
	out := stripHiveOptionKeys(in)
	assert.Equal(t, []string{
		"filepath", "/x",
		"format", "parquet",
		"compression", "gzip",
	}, out)

	// Idempotent / no hive keys
	in2 := []string{"filepath", "/x", "format", "parquet"}
	assert.Equal(t, in2, stripHiveOptionKeys(in2))

	// All hive keys
	in3 := []string{"hive_partitioning", "true", "hive_partition_columns", "y"}
	assert.Equal(t, []string{}, stripHiveOptionKeys(in3))
}

func TestFindColInTableDefCaseInsensitive(t *testing.T) {
	td := []*plan.ColDef{
		{Name: "year"},
		{Name: "Month"},
		{Name: "Day"},
	}
	got := findColInTableDefCaseInsensitive(td, "YEAR")
	require.NotNil(t, got)
	assert.Equal(t, "year", got.Name)

	got = findColInTableDefCaseInsensitive(td, "month")
	require.NotNil(t, got)
	assert.Equal(t, "Month", got.Name)

	assert.Nil(t, findColInTableDefCaseInsensitive(td, "nonexistent"))
}

// -------------------------------------------------------------------------
// validateAndSetHivePartitionOptions — every branch (happy + negative).
// -------------------------------------------------------------------------

// makeHivePlan builds a minimal plan.CreateTable with the given columns for
// validateAndSetHivePartitionOptions testing.
func makeHivePlan(cols ...*plan.ColDef) *plan.CreateTable {
	return &plan.CreateTable{
		TableDef: &plan.TableDef{Cols: cols},
	}
}

func TestValidateAndSetHivePartitionOptions_Disabled(t *testing.T) {
	// hive_partitioning absent → returns nil, does not touch stmt.Param.
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{"filepath", "/x", "format", "parquet"}
	ct := makeHivePlan(&plan.ColDef{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}})
	require.NoError(t, validateAndSetHivePartitionOptions(context.Background(), stmt, ct))
	assert.False(t, stmt.Param.HivePartitioning)
}

func TestValidateAndSetHivePartitionOptions_DisabledWithColumnsRejected(t *testing.T) {
	cases := []struct {
		name string
		opts []string
	}{
		{
			name: "columns without hive_partitioning",
			opts: []string{"filepath", "/x", "format", "parquet", "hive_partition_columns", "year"},
		},
		{
			name: "columns with hive_partitioning false",
			opts: []string{
				"filepath", "/x",
				"format", "parquet",
				"hive_partitioning", "false",
				"hive_partition_columns", "year",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
			stmt.Param.Option = tc.opts
			ct := makeHivePlan(&plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}})
			err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "requires hive_partitioning='true'")
		})
	}
}

func TestValidateAndSetHivePartitionOptions_HappyPath(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"filepath", "/data/",
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "Year",
	}
	ct := makeHivePlan(
		&plan.ColDef{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
		&plan.ColDef{
			Name:    "year",
			Typ:     plan.Type{Id: int32(types.T_int32)},
			Default: &plan.Default{NullAbility: true},
		},
	)
	require.NoError(t, validateAndSetHivePartitionOptions(context.Background(), stmt, ct))
	assert.True(t, stmt.Param.HivePartitioning)
	assert.Equal(t, []string{"year"}, stmt.Param.HivePartitionCols)
	require.Equal(t, 1, len(stmt.Param.HivePartitionColTypes))
	assert.Equal(t, int32(types.T_int32), stmt.Param.HivePartitionColTypes[0].Id)
	assert.True(t, stmt.Param.HivePartitionColTypes[0].NullAbility)
	// Option[] should be stripped of hive keys.
	for i := 0; i < len(stmt.Param.Option); i += 2 {
		assert.NotEqual(t, "hive_partitioning", stmt.Param.Option[i])
		assert.NotEqual(t, "hive_partition_columns", stmt.Param.Option[i])
	}
}

func TestValidateAndSetHivePartitionOptions_MissingCols(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{"format", "parquet", "hive_partitioning", "true"}
	ct := makeHivePlan()
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hive_partition_columns is required")
}

func TestValidateAndSetHivePartitionOptions_DuplicateHiveKey(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"hive_partitioning", "true",
		"hive_partitioning", "false",
		"hive_partition_columns", "year",
	}
	ct := makeHivePlan()
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate option key")
}

func TestValidateAndSetHivePartitionOptions_DuplicateFormat(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"format", "parquet",
		"format", "csv",
		"hive_partitioning", "true",
		"hive_partition_columns", "year",
	}
	ct := makeHivePlan(&plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate option key 'format'")
}

func TestValidateAndSetHivePartitionOptions_NonParquetFormat(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"format", "csv",
		"hive_partitioning", "true",
		"hive_partition_columns", "year",
	}
	ct := makeHivePlan(&plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only supports format='parquet'")
}

func TestValidateAndSetHivePartitionOptions_StageFilepath(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"filepath", "stage://mystage/data/",
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "year",
	}
	ct := makeHivePlan(&plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support stage external tables")
}

func TestValidateAndSetHivePartitionOptions_StageNameSet(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.StageName = "mystage"
	stmt.Param.Option = []string{
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "year",
	}
	ct := makeHivePlan(&plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support stage external tables")
}

func TestValidateAndSetHivePartitionOptions_ColumnNotFound(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "year",
	}
	ct := makeHivePlan(&plan.ColDef{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in table columns")
}

func TestValidateAndSetHivePartitionOptions_HiddenColumn(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "year",
	}
	ct := makeHivePlan(&plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}, Hidden: true})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be a hidden column")
}

func TestValidateAndSetHivePartitionOptions_VectorType(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "emb",
	}
	ct := makeHivePlan(&plan.ColDef{Name: "emb", Typ: plan.Type{Id: int32(types.T_array_float32)}})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be a VECTOR type")

	ct = makeHivePlan(&plan.ColDef{Name: "emb", Typ: plan.Type{Id: int32(types.T_array_float64)}})
	err = validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be a VECTOR type")
}

func TestValidateAndSetHivePartitionOptions_DuplicatePartitionColumn(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "year,year",
	}
	ct := makeHivePlan(&plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate partition column")
}

func TestValidateAndSetHivePartitionOptions_MultiLevelAndNullability(t *testing.T) {
	// Multi-level partition columns; mixing with/without Default to exercise
	// NullAbility default.
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"filepath", "/data/",
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "year,month",
	}
	ct := makeHivePlan(
		// year: NOT NULL (Default.NullAbility=false)
		&plan.ColDef{
			Name:    "year",
			Typ:     plan.Type{Id: int32(types.T_int32)},
			Default: &plan.Default{NullAbility: false},
		},
		// month: no Default → treated as nullable (default true)
		&plan.ColDef{Name: "month", Typ: plan.Type{Id: int32(types.T_varchar), Width: 2}},
	)
	require.NoError(t, validateAndSetHivePartitionOptions(context.Background(), stmt, ct))
	require.Len(t, stmt.Param.HivePartitionColTypes, 2)
	assert.False(t, stmt.Param.HivePartitionColTypes[0].NullAbility, "year declared NOT NULL")
	assert.True(t, stmt.Param.HivePartitionColTypes[1].NullAbility, "month default nullable when Default is nil")
	assert.Equal(t, int32(2), stmt.Param.HivePartitionColTypes[1].Width)
}

func TestValidateAndSetHivePartitionOptions_InvalidHiveValue(t *testing.T) {
	// parseHiveOptionsFromRawOptions returns an error path.
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"format", "parquet",
		"hive_partitioning", "maybe",
		"hive_partition_columns", "year",
	}
	ct := makeHivePlan(&plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be 'true' or 'false'")
}

// -------------------------------------------------------------------------
// InitStageS3Param — happy path + credential-missing error paths.
// -------------------------------------------------------------------------

func TestInitStageS3Param_HappyAndErrors(t *testing.T) {
	parse := func(raw string) *url.URL {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		return u
	}

	baseCreds := map[string]string{
		stage.PARAMKEY_AWS_KEY_ID:     "AK",
		stage.PARAMKEY_AWS_SECRET_KEY: "SK",
		stage.PARAMKEY_AWS_REGION:     "us-west-2",
		stage.PARAMKEY_ENDPOINT:       "https://s3.example.com",
	}

	t.Run("happy_path", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Ctx = context.Background()
		sd := stage.StageDef{
			Url:         parse("s3://my-bucket/prefix/"),
			Credentials: baseCreds,
		}
		require.NoError(t, InitStageS3Param(param, sd))
		assert.Equal(t, tree.S3, param.ScanType)
		assert.Equal(t, "my-bucket", param.S3Param.Bucket)
		assert.Equal(t, "AK", param.S3Param.APIKey)
		assert.Equal(t, "SK", param.S3Param.APISecret)
		assert.Equal(t, "us-west-2", param.S3Param.Region)
	})

	t.Run("bad_protocol", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Ctx = context.Background()
		sd := stage.StageDef{Url: parse("http://x/")}
		require.Error(t, InitStageS3Param(param, sd))
	})

	t.Run("raw_query_rejected", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Ctx = context.Background()
		sd := stage.StageDef{Url: parse("s3://b/p/?q=1")}
		require.Error(t, InitStageS3Param(param, sd))
	})

	// Each missing-cred path.
	for _, k := range []string{
		stage.PARAMKEY_AWS_KEY_ID, stage.PARAMKEY_AWS_SECRET_KEY,
		stage.PARAMKEY_AWS_REGION, stage.PARAMKEY_ENDPOINT,
	} {
		t.Run("missing_"+k, func(t *testing.T) {
			creds := map[string]string{}
			for kk, vv := range baseCreds {
				if kk != k {
					creds[kk] = vv
				}
			}
			param := &tree.ExternParam{}
			param.Ctx = context.Background()
			sd := stage.StageDef{
				Url:         parse("s3://b/p/"),
				Credentials: creds,
			}
			err := InitStageS3Param(param, sd)
			require.Error(t, err)
			assert.Contains(t, err.Error(), k)
		})
	}

	t.Run("option_format_csv_invalid", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Ctx = context.Background()
		param.Option = []string{"format", "orc"}
		sd := stage.StageDef{
			Url:         parse("s3://b/p/"),
			Credentials: baseCreds,
		}
		require.Error(t, InitStageS3Param(param, sd))
	})

	t.Run("option_unknown_key", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Ctx = context.Background()
		param.Option = []string{"unknown", "x"}
		sd := stage.StageDef{
			Url:         parse("s3://b/p/"),
			Credentials: baseCreds,
		}
		require.Error(t, InitStageS3Param(param, sd))
	})

	t.Run("jsonline_without_jsondata", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Ctx = context.Background()
		param.Option = []string{"format", "jsonline"}
		sd := stage.StageDef{
			Url:         parse("s3://b/p/"),
			Credentials: baseCreds,
		}
		require.Error(t, InitStageS3Param(param, sd))
	})

	t.Run("hive_legacy_option_under_stage", func(t *testing.T) {
		// The defense-in-depth hive branch under InitStageS3Param.
		param := &tree.ExternParam{}
		param.Ctx = context.Background()
		param.Option = []string{"hive_partitioning", "true", "hive_partition_columns", "year"}
		sd := stage.StageDef{
			Url:         parse("s3://b/p/"),
			Credentials: baseCreds,
		}
		require.NoError(t, InitStageS3Param(param, sd))
		assert.True(t, param.HivePartitioning)
	})

	t.Run("hive_legacy_columns_disabled_under_stage", func(t *testing.T) {
		param := &tree.ExternParam{}
		param.Ctx = context.Background()
		param.Option = []string{"hive_partitioning", "false", "hive_partition_columns", "year"}
		sd := stage.StageDef{
			Url:         parse("s3://b/p/"),
			Credentials: baseCreds,
		}
		err := InitStageS3Param(param, sd)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires hive_partitioning='true'")
	})
}

// -------------------------------------------------------------------------
// InitInfileOrStageParam — non-stage pass-through.
// -------------------------------------------------------------------------

func TestInitInfileOrStageParam_NonStageFallsThrough(t *testing.T) {
	param := &tree.ExternParam{}
	param.Ctx = context.Background()
	param.Option = []string{"filepath", "/data/x", "format", "parquet"}
	// proc is unused for the non-stage branch.
	require.NoError(t, InitInfileOrStageParam(param, nil))
	assert.Equal(t, "/data/x", param.Filepath)
	assert.Equal(t, "parquet", param.Format)
}

// Avoid unused import warning when some branches of types are not directly referenced.
var _ = types.T_int32
