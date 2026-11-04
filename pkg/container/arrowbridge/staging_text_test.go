// Copyright 2026 Matrix Origin
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

package arrowbridge

import (
	"context"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestRenderStagingDecimalTextContract(t *testing.T) {
	cases := []struct {
		name      string
		coeff     int64
		precision int32
		scale     int32
		want      string
	}{
		{name: "plain fixed point", coeff: 12345, precision: 10, scale: 2, want: "123.45"},
		{name: "negative keeps trailing zeros", coeff: -150, precision: 10, scale: 2, want: "-1.50"},
		{name: "scale surplus is zero padded", coeff: -4, precision: 10, scale: 3, want: "-0.004"},
		{name: "integer coefficient with scale", coeff: 100, precision: 5, scale: 2, want: "1.00"},
		{name: "zero keeps scale digits", coeff: 0, precision: 10, scale: 2, want: "0.00"},
		{name: "sub-unit coefficient", coeff: 7, precision: 4, scale: 4, want: "0.0007"},
		{name: "zero scale", coeff: -42, precision: 10, scale: 0, want: "-42"},
		{name: "zero scale positive renders plain digits", coeff: 42, precision: 10, scale: 0, want: "42"},
		{name: "scale beyond fixed bound renders exponent", coeff: 123, precision: 10, scale: 77, want: "123e-77"},
		{name: "negative scale renders positive exponent", coeff: 123, precision: 10, scale: -2, want: "123e2"},
		{name: "scale above precision renders exponent", coeff: 1, precision: 2, scale: 3, want: "1e-3"},
		{name: "precision above fixed bound renders exponent", coeff: 5, precision: 77, scale: 2, want: "5e-2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want,
				RenderStagingDecimalText(big.NewInt(tc.coeff), tc.precision, tc.scale))
		})
	}
}

// Scale-boundary scientific rendering must widen the source scale BEFORE
// negating: with scale == math.MinInt32 the exact value coeff x 10^-scale is
// coeff x 10^2147483648 — an out-of-range magnitude the downstream decimal
// conversion must reject (or loudly clamp), never silently store. Negating
// in int32 would wrap the exponent's sign and render coeff=1 as
// "1e-2147483648", whose cast quantizer path rounds to an ACCEPTED zero.
func TestRenderStagingDecimalTextScaleWidening(t *testing.T) {
	require.Equal(t, "1e2147483648",
		RenderStagingDecimalText(big.NewInt(1), 76, math.MinInt32))
	require.Equal(t, "-1e2147483648",
		RenderStagingDecimalText(big.NewInt(-1), 76, math.MinInt32))
	require.Equal(t, "0e2147483648",
		RenderStagingDecimalText(big.NewInt(0), 76, math.MinInt32))
	require.Equal(t, "1e2147483647",
		RenderStagingDecimalText(big.NewInt(1), 76, math.MinInt32+1))
	require.Equal(t, "1e1",
		RenderStagingDecimalText(big.NewInt(1), 76, -1))

	// The rendered staging text is consumed by the engine's decimal parsers:
	// the correctly widened magnitude is out of range for every target and is
	// rejected loudly at this parse level too.
	_, err := types.ParseDecimal128("1e2147483648", 38, 10)
	require.Error(t, err, "an exponent past MaxInt32 is an invalid Decimal128 literal")
	_, err = types.ParseDecimal256("1e2147483648", 76, 10)
	require.Error(t, err, "an exponent past MaxInt32 is an invalid Decimal256 literal")
}

func TestArrowDecimalTypeInfoOf(t *testing.T) {
	info, ok := arrowDecimalTypeInfoOf(&arrow.Decimal128Type{Precision: 18, Scale: 2})
	require.True(t, ok)
	require.Equal(t, arrowDecimalTypeInfo{precision: 18, scale: 2}, info)

	info, ok = arrowDecimalTypeInfoOf(&arrow.Decimal256Type{Precision: 38, Scale: 4})
	require.True(t, ok)
	require.Equal(t, arrowDecimalTypeInfo{precision: 38, scale: 4}, info)

	_, ok = arrowDecimalTypeInfoOf(arrow.PrimitiveTypes.Int64)
	require.False(t, ok)
}

// stagedTextTarget mirrors the scan-owned staging varchar the reader plan
// installs for LOAD attributed-DECIMAL targets.
func stagedTextTarget(name string) TargetColumn {
	return TargetColumn{Name: name, Type: types.New(types.T_varchar, 0, 0), TextStage: true}
}

func TestBindLoadTextStageConversionMatrix(t *testing.T) {
	ctx := context.Background()

	// Eligible scalar sources bind the text materialization only when the
	// staging marker is set.
	eligible := []struct {
		name string
		typ  arrow.DataType
	}{
		{"decimal128", &arrow.Decimal128Type{Precision: 18, Scale: 2}},
		{"decimal256", &arrow.Decimal256Type{Precision: 38, Scale: 4}},
		{"int8", arrow.PrimitiveTypes.Int8},
		{"int16", arrow.PrimitiveTypes.Int16},
		{"int32", arrow.PrimitiveTypes.Int32},
		{"int64", arrow.PrimitiveTypes.Int64},
		{"uint8", arrow.PrimitiveTypes.Uint8},
		{"uint16", arrow.PrimitiveTypes.Uint16},
		{"uint32", arrow.PrimitiveTypes.Uint32},
		{"uint64", arrow.PrimitiveTypes.Uint64},
		{"float16", &arrow.Float16Type{}},
		{"float32", arrow.PrimitiveTypes.Float32},
		{"float64", arrow.PrimitiveTypes.Float64},
		{"bool", arrow.FixedWidthTypes.Boolean},
	}
	for _, tc := range eligible {
		t.Run("eligible/"+tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: tc.typ}}, nil)
			_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
			require.NoError(t, err)

			// The same pair keeps the exact rejection without the marker.
			_, err = BindLoad(ctx, schema, []TargetColumn{{
				Name: "v", Type: types.New(types.T_varchar, 0, 0),
			}}, MatchByName)
			require.ErrorContains(t, err, "no exact long-term conversion")
		})
	}

	// String sources already have the borrowed raw-varlen conversion; the
	// marker must not change their selection.
	t.Run("string keeps borrowed varlen", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}}, nil)
		staged, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
		require.NoError(t, err)
		plain, err := BindLoad(ctx, schema, []TargetColumn{{
			Name: "v", Type: types.New(types.T_varchar, 0, 0),
		}}, MatchByName)
		require.NoError(t, err)
		require.NotEqual(t, staged.Fingerprint(), plain.Fingerprint(),
			"the staging marker joins the conversion fingerprint")
	})

	// Binary, temporal, and nested sources keep the existing LOAD rejection
	// even for staged targets.
	rejected := []struct {
		name string
		typ  arrow.DataType
	}{
		{"date32", arrow.FixedWidthTypes.Date32},
		{"timestamp micros", &arrow.TimestampType{Unit: arrow.Microsecond}},
		{"binary", arrow.BinaryTypes.Binary},
		{"time64", arrow.FixedWidthTypes.Time64us},
	}
	for _, tc := range rejected {
		t.Run("rejected/"+tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: tc.typ}}, nil)
			_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
			require.ErrorContains(t, err, "no exact long-term conversion")
		})
	}

	// A staged marker on an exact-physical decimal target must not disturb
	// the borrowed fixed-layout selection.
	t.Run("marker leaves exact decimal layout borrowed", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{
			Name: "v", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2},
		}}, nil)
		_, err := BindLoad(ctx, schema, []TargetColumn{{
			Name: "v", Type: types.New(types.T_decimal128, 18, 2), TextStage: true,
		}}, MatchByName)
		require.NoError(t, err)
	})

	// Dictionary encodings: staged text supports the borrowed string value
	// path and the rendered decimal-text path; other value types keep the
	// existing rejection.
	t.Run("dictionary string staged binds", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String,
		}}}, nil)
		_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
		require.NoError(t, err)
	})
	t.Run("dictionary decimal staged binds", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int8, ValueType: &arrow.Decimal128Type{Precision: 18, Scale: 2},
		}}}, nil)
		_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
		require.NoError(t, err)
	})
	t.Run("dictionary date staged rejected", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.FixedWidthTypes.Date32,
		}}}, nil)
		_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
		require.ErrorContains(t, err, "no exact long-term conversion")
	})
}

func TestConvertMaterializesStagedDecimalText(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	decimalType := &arrow.Decimal128Type{Precision: 18, Scale: 2}
	builder := array.NewDecimal128Builder(alloc, decimalType)
	builder.AppendValues([]decimal128.Num{
		decimal128.FromBigInt(big.NewInt(12345)),
		decimal128.FromBigInt(big.NewInt(-150)),
		decimal128.FromBigInt(big.NewInt(0)),
	}, []bool{true, true, false})
	values := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: decimalType, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
	plan, err := BindLoad(context.Background(), schema, []TargetColumn{stagedTextTarget("d")}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, len(bat.Vecs))
	require.Equal(t, types.T_varchar, bat.Vecs[0].GetType().Oid)
	require.Equal(t, "123.45", bat.Vecs[0].GetStringAt(0))
	require.Equal(t, "-1.50", bat.Vecs[0].GetStringAt(1))
	require.True(t, bat.Vecs[0].IsNull(2))

	record.Release()
	values.Release()
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

// TestAppendArrowStageTextValueScalarKinds renders one value per staged Arrow
// scalar kind through the staging renderer directly: each integer width
// renders exact integer text, floats render shortest round-trip text, and an
// unsupported source kind keeps the loud rejection.
func TestAppendArrowStageTextValueScalarKinds(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	mp := mpool.MustNewZero()
	ctx := context.Background()

	check := func(t *testing.T, values arrow.Array, want string) {
		t.Helper()
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, appendArrowStageTextValue(ctx, vec, values, 0, false, mp))
		require.Equal(t, want, vec.GetStringAt(0))
		vec.Free(mp)
		values.Release()
	}

	// Decimal256 renders coeff x 10^-scale through the shared renderer.
	decimal256Type := &arrow.Decimal256Type{Precision: 38, Scale: 4}
	decimal256Builder := array.NewDecimal256Builder(alloc, decimal256Type)
	decimal256Builder.AppendValues([]decimal256.Num{
		decimal256.FromBigInt(big.NewInt(123456789)),
	}, nil)
	check(t, decimal256Builder.NewArray(), "12345.6789")
	decimal256Builder.Release()

	int8Builder := array.NewInt8Builder(alloc)
	int8Builder.AppendValues([]int8{-8}, nil)
	check(t, int8Builder.NewArray(), "-8")
	int8Builder.Release()

	int16Builder := array.NewInt16Builder(alloc)
	int16Builder.AppendValues([]int16{-1600}, nil)
	check(t, int16Builder.NewArray(), "-1600")
	int16Builder.Release()

	int32Builder := array.NewInt32Builder(alloc)
	int32Builder.AppendValues([]int32{-320000}, nil)
	check(t, int32Builder.NewArray(), "-320000")
	int32Builder.Release()

	uint8Builder := array.NewUint8Builder(alloc)
	uint8Builder.AppendValues([]uint8{8}, nil)
	check(t, uint8Builder.NewArray(), "8")
	uint8Builder.Release()

	uint16Builder := array.NewUint16Builder(alloc)
	uint16Builder.AppendValues([]uint16{1600}, nil)
	check(t, uint16Builder.NewArray(), "1600")
	uint16Builder.Release()

	uint32Builder := array.NewUint32Builder(alloc)
	uint32Builder.AppendValues([]uint32{320000}, nil)
	check(t, uint32Builder.NewArray(), "320000")
	uint32Builder.Release()

	uint64Builder := array.NewUint64Builder(alloc)
	uint64Builder.AppendValues([]uint64{6400000000}, nil)
	check(t, uint64Builder.NewArray(), "6400000000")
	uint64Builder.Release()

	float16Builder := array.NewFloat16Builder(alloc)
	float16Builder.AppendValues([]float16.Num{float16.New(1.5)}, nil)
	check(t, float16Builder.NewArray(), "1.5")
	float16Builder.Release()

	float32Builder := array.NewFloat32Builder(alloc)
	float32Builder.AppendValues([]float32{2.5}, nil)
	check(t, float32Builder.NewArray(), "2.5")
	float32Builder.Release()

	// A source kind outside the staged-text set is rejected loudly; nothing
	// is appended to the staging vector.
	stringBuilder := array.NewStringBuilder(alloc)
	stringBuilder.AppendValues([]string{"x"}, nil)
	strings := stringBuilder.NewArray()
	stringBuilder.Release()
	vec := vector.NewVec(types.T_varchar.ToType())
	err := appendArrowStageTextValue(ctx, vec, strings, 0, false, mp)
	require.ErrorContains(t, err, "unsupported Arrow array")
	require.Equal(t, 0, vec.Length())
	vec.Free(mp)
	strings.Release()

	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

// TestRenderedStageTextBoundKinds pins the conservative rendered-text byte
// bound for each staged source family and the loud rejection for source
// kinds outside the staged-text set.
func TestRenderedStageTextBoundKinds(t *testing.T) {
	bound, err := renderedStageTextBound(arrow.PrimitiveTypes.Int32)
	require.NoError(t, err)
	require.Equal(t, 21, bound, "integers bound sign plus at most 20 digits")

	bound, err = renderedStageTextBound(arrow.PrimitiveTypes.Float32)
	require.NoError(t, err)
	require.Equal(t, 32, bound, "floats bound shortest round-trip text with a small exponent")

	bound, err = renderedStageTextBound(arrow.FixedWidthTypes.Boolean)
	require.NoError(t, err)
	require.Equal(t, 1, bound, "bools render a single byte")

	// Decimals charge the larger of the two render forms over the physical
	// coefficient span: declaring an extreme scale cannot shrink the bound
	// even though such a schema renders the scientific form.
	bound, err = renderedStageTextBound(&arrow.Decimal128Type{Precision: 18, Scale: 2})
	require.NoError(t, err)
	require.Equal(t, 79, bound,
		"Decimal128 bounds sign plus the fixed-point digit span capped at scale 76 plus the point")

	bound, err = renderedStageTextBound(&arrow.Decimal128Type{Precision: 38, Scale: 1_000_000_000})
	require.NoError(t, err)
	require.Equal(t, 79, bound, "an extreme declared scale keeps the conservative Decimal128 bound")

	bound, err = renderedStageTextBound(&arrow.Decimal256Type{Precision: 76, Scale: 0})
	require.NoError(t, err)
	require.Equal(t, 90, bound,
		"Decimal256 bounds sign plus 77 physical coefficient digits plus the scientific suffix")

	bound, err = renderedStageTextBound(&arrow.Decimal256Type{Precision: 76, Scale: -2})
	require.NoError(t, err)
	require.Equal(t, 90, bound, "a negative declared scale keeps the conservative Decimal256 bound")

	_, err = renderedStageTextBound(arrow.BinaryTypes.Binary)
	require.ErrorContains(t, err, "unsupported Arrow staged text type")
}

// TestRenderedStageDecimalBoundCoversWorstRenders pins the decimal row
// bound against the worst renders the staging gate can physically emit, so
// the estimate can never be loosened below the reachable payload again:
// Decimal256 scientific text reaches the bound exactly (77 physical digits,
// sign, 'e', and an 11-character exponent), and the shared renderer's
// fixed-point ceiling needs the full 77-character padded span.
func TestRenderedStageDecimalBoundCoversWorstRenders(t *testing.T) {
	// |coeff| < 2^255 carries at most 77 digits; scale math.MaxInt32 takes
	// the scientific fallback with the longest reachable exponent text.
	coeff256 := new(big.Int).Neg(new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(1)))
	scientific := RenderStagingDecimalText(coeff256, 76, math.MaxInt32)
	require.Len(t, scientific, 90, "sign + 77 digits + 'e' + 11 exponent characters")
	require.True(t, strings.HasSuffix(scientific, "e-2147483647"))
	bound, err := renderedStageTextBound(&arrow.Decimal256Type{Precision: 76, Scale: math.MaxInt32})
	require.NoError(t, err)
	require.Equal(t, len(scientific), bound, "the bound covers the worst scientific render exactly")

	// The gate admits source scales up to 76, whose zero padding plus sign
	// and point renders 79 bytes even for a minimal coefficient.
	fixedPoint := RenderStagingDecimalText(big.NewInt(-1), 76, 76)
	require.Len(t, fixedPoint, 79, "sign + 77 padded digits + point")
	require.True(t, strings.HasPrefix(fixedPoint, "-0."))
	bound, err = renderedStageTextBound(&arrow.Decimal128Type{Precision: 38, Scale: 10})
	require.NoError(t, err)
	require.Equal(t, len(fixedPoint), bound, "the bound covers the shared renderer's fixed-point ceiling")

	// More digits than the declared precision still stay inside the bound: a
	// forged Decimal128 record can carry the full 39-digit physical span.
	coeff128 := new(big.Int).Neg(new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1)))
	overwide := RenderStagingDecimalText(coeff128, 1, 0)
	require.Len(t, overwide, 40)
	require.LessOrEqual(t, len(overwide), bound)
}

// TestConvertStagedTextCanceledContext checks the staged-text conversion
// loop observes context cancellation between rows instead of rendering the
// whole batch first.
func TestConvertStagedTextCanceledContext(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewInt64Builder(alloc)
	builder.AppendValues([]int64{1, 2}, nil)
	values := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	plan, err := BindLoad(context.Background(), schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 2)
	mp := mpool.MustNewZero()
	bat, _, err := plan.Convert(ctx, record, mp, ConvertOptions{})
	require.Error(t, err, "a canceled context aborts the staged conversion")
	require.Nil(t, bat)

	// The per-column staged-text loop checks the context before rendering a
	// row: a canceled context aborts the materialization at row zero. The
	// whole-batch path above may already reject during row budgeting, so the
	// column loop needs this direct exercise as well.
	_, _, err = materializeConverted(ctx, values, columnPlan{
		target: stagedTextTarget("v"),
		kind:   conversionMaterializeText,
	}, mp, nil, nil)
	require.Error(t, err, "a canceled context aborts the staged per-column loop")

	record.Release()
	values.Release()
	alloc.AssertSize(t, 0)
}

// TestEstimateColumnRowBytesStagedTextUnsupportedSource checks row budgeting
// for a staged-text target propagates the rendered-bound rejection instead
// of silently under-budgeting the batch window.
func TestEstimateColumnRowBytesStagedTextUnsupportedSource(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewStringBuilder(alloc)
	builder.AppendValues([]string{"x"}, nil)
	values := builder.NewArray()
	builder.Release()

	binding := columnPlan{
		target: stagedTextTarget("v"),
		kind:   conversionMaterializeText,
	}
	_, err := estimateColumnRowBytes(context.Background(), values, binding, 0)
	require.ErrorContains(t, err, "unsupported Arrow staged text type")

	values.Release()
	alloc.AssertSize(t, 0)
}

func TestConvertMaterializesStagedDictionaryDecimalText(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	decimalType := &arrow.Decimal128Type{Precision: 18, Scale: 2}
	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8, ValueType: decimalType,
	}

	// Dictionary payload [123.45, -1.50] selected as [0, 1, 0] with the third
	// index NULL.
	builder := array.NewDecimal128Builder(alloc, decimalType)
	builder.AppendValues([]decimal128.Num{
		decimal128.FromBigInt(big.NewInt(12345)),
		decimal128.FromBigInt(big.NewInt(-150)),
	}, nil)
	dictValues := builder.NewArray()
	builder.Release()
	indexBuilder := array.NewInt8Builder(alloc)
	indexBuilder.AppendValues([]int8{0, 1, 0}, []bool{true, true, false})
	indices := indexBuilder.NewArray()
	indexBuilder.Release()
	dictionary := array.NewDictionaryArray(dictionaryType, indices, dictValues)
	indices.Release()
	dictValues.Release()
	dictSchema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: dictionaryType, Nullable: true}}, nil)

	// The plain array carrying the same logical rows.
	plainBuilder := array.NewDecimal128Builder(alloc, decimalType)
	plainBuilder.AppendValues([]decimal128.Num{
		decimal128.FromBigInt(big.NewInt(12345)),
		decimal128.FromBigInt(big.NewInt(-150)),
		{},
	}, []bool{true, true, false})
	plainValues := plainBuilder.NewArray()
	plainBuilder.Release()
	plainSchema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: decimalType, Nullable: true}}, nil)

	mp := mpool.MustNewZero()
	targets := []TargetColumn{stagedTextTarget("d")}

	dictRecord := array.NewRecordBatch(dictSchema, []arrow.Array{dictionary}, 3)
	dictPlan, err := BindLoad(context.Background(), dictSchema, targets, MatchByName)
	require.NoError(t, err)
	dictBat, _, err := dictPlan.Convert(context.Background(), dictRecord, mp, ConvertOptions{})
	require.NoError(t, err)

	plainRecord := array.NewRecordBatch(plainSchema, []arrow.Array{plainValues}, 3)
	plainPlan, err := BindLoad(context.Background(), plainSchema, targets, MatchByName)
	require.NoError(t, err)
	plainBat, _, err := plainPlan.Convert(context.Background(), plainRecord, mp, ConvertOptions{})
	require.NoError(t, err)

	// Encoding equivalence: a dictionary encoding materializes the identical
	// rows the plain array produces, values and NULLs alike.
	for row := uint64(0); row < 3; row++ {
		require.Equal(t, plainBat.Vecs[0].IsNull(row), dictBat.Vecs[0].IsNull(row),
			"row %d null handling", row)
		if !plainBat.Vecs[0].IsNull(row) {
			require.Equal(t, plainBat.Vecs[0].GetStringAt(int(row)), dictBat.Vecs[0].GetStringAt(int(row)),
				"row %d stored value", row)
		}
	}
	require.Equal(t, "123.45", dictBat.Vecs[0].GetStringAt(0))
	require.Equal(t, "-1.50", dictBat.Vecs[0].GetStringAt(1))

	dictRecord.Release()
	plainRecord.Release()
	dictionary.Release()
	plainValues.Release()
	dictBat.Clean(mp)
	plainBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestMaxOutputRowsAccountsRenderedStagedText(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	decimalType := &arrow.Decimal256Type{Precision: 76, Scale: 0}
	// 10^75 renders 76 digits.
	coeff := new(big.Int).Exp(big.NewInt(10), big.NewInt(75), nil)
	builder := array.NewDecimal256Builder(alloc, decimalType)
	builder.AppendValues([]decimal256.Num{
		decimal256.FromBigInt(coeff),
		decimal256.FromBigInt(coeff),
	}, nil)
	values := builder.NewArray()
	builder.Release()

	// The same payload once as a plain array and once under a dictionary
	// encoding: both drive the rendered-text budget.
	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8, ValueType: decimalType,
	}
	indexBuilder := array.NewInt8Builder(alloc)
	indexBuilder.AppendValues([]int8{0, 0}, nil)
	indices := indexBuilder.NewArray()
	indexBuilder.Release()
	dictionary := array.NewDictionaryArray(dictionaryType, indices, values)
	indices.Release()

	// A staged DECIMAL row costs its varchar descriptor plus the conservative
	// rendered-text bound for the source storage (90 bytes for Decimal256:
	// sign, up to 77 physical coefficient digits, and the scientific suffix);
	// a budget fitting one such row must admit only that one.
	rowBytes := uint64(types.VarlenaSize + 90)
	for _, tc := range []struct {
		name   string
		schema *arrow.Schema
		values arrow.Array
	}{
		{name: "plain", schema: arrow.NewSchema([]arrow.Field{{Name: "v", Type: decimalType}}, nil), values: values},
		{name: "dictionary", schema: arrow.NewSchema([]arrow.Field{{Name: "v", Type: dictionaryType, Nullable: true}}, nil), values: dictionary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := array.NewRecordBatch(tc.schema, []arrow.Array{tc.values}, 2)
			plan, err := BindLoad(context.Background(), tc.schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
			require.NoError(t, err)

			rows, err := plan.MaxOutputRows(context.Background(), record, 0, 2, rowBytes)
			require.NoError(t, err)
			require.Equal(t, 1, rows,
				"the rendered payload counts toward the byte budget")
			rows, err = plan.MaxOutputRows(context.Background(), record, 0, 2, 2*rowBytes)
			require.NoError(t, err)
			require.Equal(t, 2, rows,
				"a budget for both rendered rows admits both")
			rows, err = plan.MaxOutputRows(context.Background(), record, 0, 2, 1)
			require.NoError(t, err)
			require.Equal(t, 1, rows, "one oversized row still makes progress")

			record.Release()
		})
	}

	dictionary.Release()
	values.Release()
	alloc.AssertSize(t, 0)
}

// TestMaxOutputRowsAccountsScientificStagedText is the regression for a
// declared scale outside the fixed-point domain: the renderer falls back to
// the bounded scientific form, whose exponent bytes the estimate must charge
// as well. Under the previous precision+3 estimate the byte budget admitted
// two individually valid rows whose rendered staging payload overflowed the
// budget; the conservative bound admits only rows the converted staging
// vector actually fits.
func TestMaxOutputRowsAccountsScientificStagedText(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	mp := mpool.MustNewZero()

	// 10^37 renders 38 coefficient digits; scale +/-1e9 falls outside the
	// fixed-point gate, so each row renders "<digits>e-1000000000" (50 bytes)
	// or "<digits>e1000000000" (49 bytes).
	coeff := new(big.Int).Exp(big.NewInt(10), big.NewInt(37), nil)

	for _, tc := range []struct {
		name          string
		scale         int32
		wantRendered  string
		wantFootprint int
	}{
		{name: "positive scale", scale: 1_000_000_000,
			wantRendered:  "1" + strings.Repeat("0", 37) + "e-1000000000",
			wantFootprint: 2*types.VarlenaSize + 100},
		{name: "negative scale", scale: -1_000_000_000,
			wantRendered:  "1" + strings.Repeat("0", 37) + "e1000000000",
			wantFootprint: 2*types.VarlenaSize + 98},
	} {
		decimalType := &arrow.Decimal128Type{Precision: 38, Scale: tc.scale}
		builder := array.NewDecimal128Builder(alloc, decimalType)
		builder.AppendValues([]decimal128.Num{
			decimal128.FromBigInt(coeff),
			decimal128.FromBigInt(coeff),
		}, nil)
		values := builder.NewArray()
		builder.Release()

		dictionaryType := &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int8, ValueType: decimalType,
		}
		indexBuilder := array.NewInt8Builder(alloc)
		indexBuilder.AppendValues([]int8{0, 0}, nil)
		indices := indexBuilder.NewArray()
		indexBuilder.Release()
		dictionary := array.NewDictionaryArray(dictionaryType, indices, values)
		indices.Release()

		for _, encoding := range []struct {
			name   string
			schema *arrow.Schema
			values arrow.Array
		}{
			{name: "plain",
				schema: arrow.NewSchema([]arrow.Field{{Name: "v", Type: decimalType}}, nil),
				values: values},
			{name: "dictionary",
				schema: arrow.NewSchema([]arrow.Field{{Name: "v", Type: dictionaryType, Nullable: true}}, nil),
				values: dictionary},
		} {
			t.Run(tc.name+"/"+encoding.name, func(t *testing.T) {
				record := array.NewRecordBatch(encoding.schema, []arrow.Array{encoding.values}, 2)
				plan, err := BindLoad(context.Background(), encoding.schema,
					[]TargetColumn{stagedTextTarget("v")}, MatchByName)
				require.NoError(t, err)

				// The conservative Decimal128 row bound charges descriptor +
				// sign + padded/physical digits + point or exponent suffix.
				rowBytes := uint64(types.VarlenaSize + 79)
				const budget = uint64(130)
				require.GreaterOrEqual(t, budget, rowBytes)
				require.Less(t, budget, 2*rowBytes,
					"the probe budget fits one bounded row but not two")

				rows, err := plan.MaxOutputRows(context.Background(), record, 0, 2, budget)
				require.NoError(t, err)
				require.Equal(t, 1, rows,
					"the scientific render charges exponent bytes toward the byte budget")

				window := record.NewSlice(0, int64(rows))
				bat, _, err := plan.Convert(context.Background(), window, mp, ConvertOptions{})
				require.NoError(t, err)
				require.Equal(t, rows, bat.Vecs[0].Length())
				require.Equal(t, tc.wantRendered, bat.Vecs[0].GetStringAt(0))
				require.LessOrEqual(t, stagedFootprint(bat.Vecs[0]), int(budget),
					"the admitted window materializes within the budget")
				window.Release()
				bat.Clean(mp)

				// A budget for both bounded rows admits both; the rendered
				// footprint is identical under plain and dictionary encodings.
				rows, err = plan.MaxOutputRows(context.Background(), record, 0, 2, 2*rowBytes)
				require.NoError(t, err)
				require.Equal(t, 2, rows)
				bat, _, err = plan.Convert(context.Background(), record, mp, ConvertOptions{})
				require.NoError(t, err)
				require.Equal(t, tc.wantFootprint, stagedFootprint(bat.Vecs[0]))
				require.LessOrEqual(t, stagedFootprint(bat.Vecs[0]), int(2*rowBytes))
				bat.Clean(mp)

				record.Release()
			})
		}

		dictionary.Release()
		values.Release()
	}
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

// stagedFootprint returns the bytes a materialized staging vector occupies:
// one varlena descriptor per row plus every non-null rendered payload byte.
func stagedFootprint(vec *vector.Vector) int {
	footprint := vec.Length() * types.VarlenaSize
	for row := 0; row < vec.Length(); row++ {
		if !vec.IsNull(uint64(row)) {
			footprint += len(vec.GetBytesAt(row))
		}
	}
	return footprint
}

func TestConvertMaterializesStagedScalarText(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(alloc, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{-42, 0}, nil)
	builder.Field(1).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1.5, -0.25}, nil)
	record := builder.NewRecordBatch()
	builder.Release()
	targets := []TargetColumn{
		stagedTextTarget("i"), stagedTextTarget("b"), stagedTextTarget("f"),
	}
	plan, err := BindLoad(context.Background(), schema, targets, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.Equal(t, "-42", bat.Vecs[0].GetStringAt(0))
	require.Equal(t, "0", bat.Vecs[0].GetStringAt(1))
	require.Equal(t, "1", bat.Vecs[1].GetStringAt(0))
	require.Equal(t, "0", bat.Vecs[1].GetStringAt(1))
	require.Equal(t, "1.5", bat.Vecs[2].GetStringAt(0))
	require.Equal(t, "-0.25", bat.Vecs[2].GetStringAt(1))

	record.Release()
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}
