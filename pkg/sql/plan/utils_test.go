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

package plan

import (
	"context"
	"net/url"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasTrailingZeros(t *testing.T) {
	tests := []struct {
		name         string
		value        string
		constScale   int32
		columnScale  int32
		expectResult bool
	}{
		{
			name:         "99.990000000 scale=9 to scale=2 - has trailing zeros",
			value:        "99.990000000",
			constScale:   9,
			columnScale:  2,
			expectResult: true,
		},
		{
			name:         "99.991234567 scale=9 to scale=2 - no trailing zeros",
			value:        "99.991234567",
			constScale:   9,
			columnScale:  2,
			expectResult: false,
		},
		{
			name:         "100.000000000 scale=9 to scale=0 - has trailing zeros",
			value:        "100.000000000",
			constScale:   9,
			columnScale:  0,
			expectResult: true,
		},
		{
			name:         "12.340000000 scale=9 to scale=5 - has trailing zeros",
			value:        "12.340000000",
			constScale:   9,
			columnScale:  5,
			expectResult: true,
		},
		{
			name:         "12.340000001 scale=9 to scale=5 - no trailing zeros",
			value:        "12.340000001",
			constScale:   9,
			columnScale:  5,
			expectResult: false,
		},
		{
			name:         "12.34567 scale=5 to scale=5 - exact match",
			value:        "12.34567",
			constScale:   5,
			columnScale:  5,
			expectResult: false, // constScale <= columnScale, returns false
		},
		{
			name:         "12.34567 scale=5 to scale=10 - column has higher scale",
			value:        "12.34567",
			constScale:   5,
			columnScale:  10,
			expectResult: false, // constScale <= columnScale, returns false
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the decimal value
			dec, _, err := types.Parse128(tt.value)
			require.NoError(t, err)

			// Create constant expression
			constExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.constScale,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_Decimal128Val{
							Decimal128Val: &plan.Decimal128{
								A: int64(dec.B0_63),
								B: int64(dec.B64_127),
							},
						},
					},
				},
			}

			constType := types.Type{
				Oid:   types.T_decimal128,
				Width: 38,
				Scale: tt.constScale,
			}

			// Test hasTrailingZeros
			result := hasTrailingZeros(constExpr, constType, tt.columnScale)

			t.Logf("Value: %s, ConstScale: %d, ColumnScale: %d", tt.value, tt.constScale, tt.columnScale)
			t.Logf("Decimal128: A=%d, B=%d", dec.B0_63, dec.B64_127)
			t.Logf("Expected: %v, Got: %v", tt.expectResult, result)

			require.Equal(t, tt.expectResult, result, "hasTrailingZeros result mismatch")
		})
	}
}

func TestCheckNoNeedCastWithTrailingZeros(t *testing.T) {
	tests := []struct {
		name         string
		constValue   string
		constScale   int32
		columnScale  int32
		expectResult bool
	}{
		{
			name:         "trailing zeros - should not need cast",
			constValue:   "99.990000000",
			constScale:   9,
			columnScale:  2,
			expectResult: true,
		},
		{
			name:         "no trailing zeros - should need cast",
			constValue:   "99.991234567",
			constScale:   9,
			columnScale:  2,
			expectResult: false,
		},
		{
			name:         "exact scale match - should not need cast",
			constValue:   "99.99",
			constScale:   2,
			columnScale:  2,
			expectResult: true,
		},
		{
			name:         "column scale higher - should not need cast",
			constValue:   "99.99",
			constScale:   2,
			columnScale:  5,
			expectResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the decimal value
			dec, _, err := types.Parse128(tt.constValue)
			require.NoError(t, err)

			// Create constant expression
			constExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.constScale,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_Decimal128Val{
							Decimal128Val: &plan.Decimal128{
								A: int64(dec.B0_63),
								B: int64(dec.B64_127),
							},
						},
					},
				},
			}

			constType := types.Type{
				Oid:   types.T_decimal128,
				Width: 38,
				Scale: tt.constScale,
			}

			columnType := types.Type{
				Oid:   types.T_decimal128,
				Width: 38,
				Scale: tt.columnScale,
			}

			// Test checkNoNeedCast
			result := checkNoNeedCast(constType, columnType, constExpr)

			t.Logf("ConstValue: %s, ConstScale: %d, ColumnScale: %d", tt.constValue, tt.constScale, tt.columnScale)
			t.Logf("Expected: %v, Got: %v", tt.expectResult, result)

			require.Equal(t, tt.expectResult, result, "checkNoNeedCast result mismatch")
		})
	}
}

func TestBindFuncExprWithTrailingZeros(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name             string
		colScale         int32
		constValue       string
		constScale       int32
		shouldUseColType bool
		description      string
	}{
		{
			name:             "trailing zeros - should use col type",
			colScale:         2,
			constValue:       "99.990000000",
			constScale:       9,
			shouldUseColType: true,
			description:      "99.990000000 has trailing zeros, should use column scale 2",
		},
		// Note: Removed "no trailing zeros - should NOT use col type" test case
		// because it triggers early false detection optimization which returns FALSE directly.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Test: %s", tt.description)

			// Create column expression
			colExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.colScale,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			}

			// Parse the constant value
			dec, _, err := types.Parse128(tt.constValue)
			require.NoError(t, err)

			// Create constant expression
			constExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.constScale,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_Decimal128Val{
							Decimal128Val: &plan.Decimal128{
								A: int64(dec.B0_63),
								B: int64(dec.B64_127),
							},
						},
					},
				},
			}

			// Call BindFuncExprImplByPlanExpr
			result, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{colExpr, constExpr})
			require.NoError(t, err)
			require.NotNil(t, result)

			funcExpr := result.GetF()
			require.NotNil(t, funcExpr)

			// Check the scale of the arguments
			leftScale := funcExpr.Args[0].Typ.Scale
			rightScale := funcExpr.Args[1].Typ.Scale

			t.Logf("Input: colScale=%d, constValue=%s, constScale=%d", tt.colScale, tt.constValue, tt.constScale)
			t.Logf("Result: leftScale=%d, rightScale=%d", leftScale, rightScale)

			// Check if cast is present
			leftHasCast := funcExpr.Args[0].GetF() != nil && funcExpr.Args[0].GetF().Func.GetObjName() == "cast"
			rightHasCast := funcExpr.Args[1].GetF() != nil && funcExpr.Args[1].GetF().Func.GetObjName() == "cast"
			t.Logf("Left has cast: %v, Right has cast: %v", leftHasCast, rightHasCast)

			if tt.shouldUseColType {
				// Both should have column's scale (optimization applied)
				require.Equal(t, tt.colScale, leftScale, "Left arg should have col scale")
				require.Equal(t, tt.colScale, rightScale, "Right arg should have col scale")
				t.Logf("✓ Optimization applied: using column scale %d", tt.colScale)
			} else {
				// Should use higher scale (no optimization)
				expectedScale := tt.constScale
				if tt.colScale > tt.constScale {
					expectedScale = tt.colScale
				}
				require.Equal(t, expectedScale, leftScale, "Left arg should have higher scale")
				require.Equal(t, expectedScale, rightScale, "Right arg should have higher scale")
				t.Logf("✓ No optimization: using higher scale %d", expectedScale)
			}
		})
	}
}

// TestDecimal128HasTrailingZeros tests the decimal128HasTrailingZeros function
// specifically for large values that use the high 64 bits
func TestDecimal128HasTrailingZeros(t *testing.T) {
	tests := []struct {
		name           string
		value          string
		constScale     int32
		columnScale    int32
		expectTrailing bool
		description    string
	}{
		{
			name:           "Large value with trailing zeros",
			value:          "12345678901234567890.000000",
			constScale:     6,
			columnScale:    0,
			expectTrailing: true,
			description:    "20-digit integer with 6 trailing zeros",
		},
		{
			name:           "Large value without trailing zeros",
			value:          "12345678901234567890.123456",
			constScale:     6,
			columnScale:    0,
			expectTrailing: false,
			description:    "20-digit value with non-zero fractional part",
		},
		{
			name:           "Large value partial trailing zeros",
			value:          "12345678901234567890.123000",
			constScale:     6,
			columnScale:    3,
			expectTrailing: true,
			description:    "20-digit value with 3 trailing zeros (scale 6 to 3)",
		},
		{
			name:           "Large value partial no trailing zeros",
			value:          "12345678901234567890.123456",
			constScale:     6,
			columnScale:    3,
			expectTrailing: false,
			description:    "20-digit value without trailing zeros (scale 6 to 3)",
		},
		{
			name:           "Max Decimal128 range with trailing zeros",
			value:          "99999999999999999999999999999999.00000",
			constScale:     5,
			columnScale:    0,
			expectTrailing: true,
			description:    "Near max value with trailing zeros",
		},
		{
			name:           "Small value high bits zero",
			value:          "123.000000",
			constScale:     6,
			columnScale:    0,
			expectTrailing: true,
			description:    "Small value (high bits = 0) with trailing zeros",
		},
		{
			name:           "Small value high bits zero no trailing",
			value:          "123.456789",
			constScale:     6,
			columnScale:    0,
			expectTrailing: false,
			description:    "Small value (high bits = 0) without trailing zeros",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec, _, err := types.Parse128(tt.value)
			require.NoError(t, err)

			t.Logf("Value: %s", tt.value)
			t.Logf("Decimal128: low=%d, high=%d", dec.B0_63, dec.B64_127)
			t.Logf("ConstScale: %d, ColumnScale: %d", tt.constScale, tt.columnScale)

			// Test decimal128HasTrailingZeros directly
			trailingDigits := tt.constScale - tt.columnScale
			result := decimal128HasTrailingZeros(int64(dec.B0_63), int64(dec.B64_127), trailingDigits)

			t.Logf("TrailingDigits: %d, Expected: %v, Got: %v", trailingDigits, tt.expectTrailing, result)
			require.Equal(t, tt.expectTrailing, result, tt.description)

			// Also test through hasTrailingZeros wrapper
			constExpr := &plan.Expr{
				Typ: plan.Type{
					Id:    int32(types.T_decimal128),
					Width: 38,
					Scale: tt.constScale,
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_Decimal128Val{
							Decimal128Val: &plan.Decimal128{
								A: int64(dec.B0_63),
								B: int64(dec.B64_127),
							},
						},
					},
				},
			}

			constType := types.Type{
				Oid:   types.T_decimal128,
				Width: 38,
				Scale: tt.constScale,
			}

			wrapperResult := hasTrailingZeros(constExpr, constType, tt.columnScale)
			require.Equal(t, tt.expectTrailing, wrapperResult, "hasTrailingZeros wrapper result mismatch")
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
		"format", "parquet",
	}
	require.NoError(t, InitS3Param(param))
	assert.Equal(t, "https://s3.example.com", param.S3Param.Endpoint)
	assert.Equal(t, "my-bucket", param.S3Param.Bucket)
	assert.Equal(t, "sales/", param.Filepath)
	assert.Equal(t, "parquet", param.Format)

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
		"format", "parquet",
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
	param.Option = []string{"bucket", "b", "format", "parquet", "hive_partitioning", "maybe"}
	require.Error(t, InitS3Param(param))
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

func TestValidateAndSetHivePartitionOptions_GeneratedColumn(t *testing.T) {
	stmt := &tree.CreateTable{Param: &tree.ExternParam{}}
	stmt.Param.Option = []string{
		"format", "parquet",
		"hive_partitioning", "true",
		"hive_partition_columns", "year",
	}
	ct := makeHivePlan(&plan.ColDef{
		Name:         "year",
		Typ:          plan.Type{Id: int32(types.T_int32)},
		GeneratedCol: &plan.GeneratedCol{},
	})
	err := validateAndSetHivePartitionOptions(context.Background(), stmt, ct)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be a generated column")
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
