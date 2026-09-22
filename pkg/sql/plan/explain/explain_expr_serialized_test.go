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

package explain

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	planpkg "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func TestCompositeSecondaryIndexRangeBoundsArePrintable(t *testing.T) {
	qualifiedColumn := catalog.SecondaryIndexTableNamePrefix + "range_bounds." + catalog.IndexTableIndexColName

	for _, test := range []struct {
		name       string
		columnName string
		op         string
		boundHex   string
		serialized bool
	}{
		{
			name:       "invalid UTF-8 decimal lower bound",
			columnName: catalog.IndexTableIndexColName,
			op:         ">=",
			boundHex:   "458000000000000000000000000002673c",
			serialized: true,
		},
		{
			name:       "non-printable varchar upper bound",
			columnName: qualifiedColumn,
			op:         "<",
			boundHex:   "46016100",
			serialized: true,
		},
		{
			name:       "printable boolean bound",
			columnName: catalog.IndexTableIndexColName,
			op:         ">=",
			boundHex:   "27",
			serialized: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			serializedBound := mustDecodeHex(t, test.boundHex)
			got := describeComparisonForLiteralTest(
				t, test.columnName, test.op, string(serializedBound), types.T_varchar, false, test.serialized,
			)
			if !utf8.ValidString(got) {
				t.Fatalf("EXPLAIN expression is not valid UTF-8: %x", []byte(got))
			}
			for _, r := range got {
				if !unicode.IsPrint(r) {
					t.Fatalf("EXPLAIN expression contains non-printable rune %U: %q", r, got)
				}
			}
			want := "(" + catalog.IndexTableIndexColName + " " + test.op + " '<opaque>')"
			if got != want {
				t.Fatalf("serialized bound was not redacted: got %q, want %q", got, want)
			}
		})
	}
}

func TestOrdinaryStringLiteralExplainRemainsMeaningful(t *testing.T) {
	const literal = "R\u00e9sum\u00e9 \u6771\u4eac"

	for _, columnName := range []string{
		"customer_name",
		catalog.IndexTableIndexColName,
		"user___mo_index_idx_col_note",
	} {
		got := describeComparisonForTest(t, columnName, "<", literal)
		if !strings.Contains(got, "'"+literal+"'") {
			t.Fatalf("ordinary string literal was not rendered meaningfully: %q", got)
		}
	}
}

func TestGeometryLiteralExplainRemainsWKT(t *testing.T) {
	wkb := mustDecodeHex(t, "0101000000000000000000f03f0000000000000040")
	got := describeComparisonForTypeTest(t, catalog.IndexTableIndexColName, "st_contains", string(wkb), types.T_geometry)
	if want := "st_contains(__mo_index_idx_col, 'POINT(1 2)')"; got != want {
		t.Fatalf("geometry literal rendering changed: got %q, want %q", got, want)
	}
}

func TestSerializedLiteralRedactionAppliesAcrossExpressionLayouts(t *testing.T) {
	registered, err := function.GetFunctionByName(
		context.Background(), "between",
		[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
	)
	if err != nil {
		t.Fatal(err)
	}

	stringExpr := func(value string, serialized bool) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_varchar)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value:        &planpb.Literal_Sval{Sval: value},
				IsSerialized: serialized,
			}},
		}
	}
	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: registered.GetEncodedOverloadID(), ObjName: "between"},
			Args: []*planpb.Expr{
				{
					Typ: planpb.Type{Id: int32(types.T_varchar)},
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
						Name: catalog.CPrimaryKeyColName,
					}},
				},
				stringExpr(string([]byte{0x27}), true),
				stringExpr("ordinary text", false),
			},
		}},
	}

	var buf bytes.Buffer
	if err := describeExpr(context.Background(), expr, NewExplainDefaultOptions(), &buf); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), catalog.CPrimaryKeyColName+" BETWEEN '<opaque>' AND 'ordinary text'"; got != want {
		t.Fatalf("serialized redaction depends on expression layout: got %q, want %q", got, want)
	}
}

func TestBinaryLiteralExplainUsesHex(t *testing.T) {
	got := describeComparisonForLiteralTest(t, "payload", "=", "AB", types.T_varchar, true, false)
	if want := "(payload = 0x4142)"; got != want {
		t.Fatalf("binary literal rendering changed: got %q, want %q", got, want)
	}
}

func TestNonPrintableStringLiteralExplainUsesHex(t *testing.T) {
	got := describeComparisonForLiteralTest(t, "payload", "=", string([]byte{0xff, 0x00}), types.T_varchar, false, false)
	if want := "(payload = 0xFF00)"; got != want {
		t.Fatalf("non-text literal rendering changed: got %q, want %q", got, want)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("EXPLAIN expression is not valid UTF-8: %x", []byte(got))
	}
}

func TestLiteralVecExplainNeverWritesRawNonTextBytes(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	vec := vector.NewVec(types.T_varchar.ToType())
	if err := vector.AppendBytes(vec, []byte{0xff, 0x00}, false, mp); err != nil {
		t.Fatal(err)
	}
	data, err := vec.MarshalBinary()
	if err != nil {
		vec.Free(mp)
		t.Fatal(err)
	}
	vec.Free(mp)

	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
			Len:  1,
			Data: data,
		}},
	}
	var buf bytes.Buffer
	if err := describeExpr(t.Context(), expr, NewExplainDefaultOptions(), &buf); err != nil {
		t.Fatal(err)
	}
	assertPrintableExplainText(t, buf.String())
	if got, want := buf.String(), "0xFF00"; got != want {
		t.Fatalf("non-text vector was not rendered canonically: got %q, want %q", got, want)
	}
}

func TestJSONExplainPreservesTimestampPrecisionAndCompleteLiteralVector(t *testing.T) {
	ts, err := types.ParseTimestamp(time.UTC, "2024-01-02 03:04:05.123456", 6)
	if err != nil {
		t.Fatal(err)
	}
	timestamp := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_timestamp), Scale: 6},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Timestampval{Timestampval: int64(ts)},
		}},
	}
	var timestampBuf bytes.Buffer
	if err := describeExpr(t.Context(), timestamp, &ExplainOptions{Format: EXPLAIN_FORMAT_JSON}, &timestampBuf); err != nil {
		t.Fatal(err)
	}
	if got, want := timestampBuf.String(), "2024-01-02 03:04:05.123456"; got != want {
		t.Fatalf("timestamp precision was not preserved: got %q, want %q", got, want)
	}

	mp := mpool.MustNew(t.Name())
	vec := vector.NewVec(types.T_int32.ToType())
	for i := 1; i <= 17; i++ {
		if err := vector.AppendFixed[int32](vec, int32(i), false, mp); err != nil {
			t.Fatal(err)
		}
	}
	data, err := vec.MarshalBinary()
	vec.Free(mp)
	if err != nil {
		t.Fatal(err)
	}
	vectorExpr := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int32)},
		Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: 17, Data: data}},
	}
	var vectorBuf bytes.Buffer
	if err := describeExpr(t.Context(), vectorExpr, &ExplainOptions{
		Format:                 EXPLAIN_FORMAT_TEXT,
		CompleteLiteralVectors: true,
	}, &vectorBuf); err != nil {
		t.Fatal(err)
	}
	if got := vectorBuf.String(); !strings.Contains(got, "16") || !strings.Contains(got, "17") {
		t.Fatalf("complete JSON vector lost its 17th value: %q", got)
	}
	var textBuf bytes.Buffer
	if err := describeExpr(t.Context(), vectorExpr, NewExplainDefaultOptions(), &textBuf); err != nil {
		t.Fatal(err)
	}
	if got := textBuf.String(); !strings.Contains(got, "... 17 values") {
		t.Fatalf("text EXPLAIN vector truncation changed: %q", got)
	}
}

func TestLiteralVecExplainPreservesTypedScaleBoundariesAndNulls(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	datetimeType := types.T_datetime.ToTypeWithScale(6)
	datetimeVec := vector.NewVec(datetimeType)
	dt, err := types.ParseDatetime("2024-01-02 03:04:05.123456", 6)
	if err != nil {
		t.Fatal(err)
	}
	if err = vector.AppendFixed[types.Datetime](datetimeVec, dt, false, mp); err != nil {
		t.Fatal(err)
	}
	if err = vector.AppendFixed[types.Datetime](datetimeVec, 0, true, mp); err != nil {
		t.Fatal(err)
	}
	datetimeData, err := datetimeVec.MarshalBinary()
	datetimeVec.Free(mp)
	if err != nil {
		t.Fatal(err)
	}

	decimalType := types.T_decimal64.ToTypeWithScale(2)
	decimalVec := vector.NewVec(decimalType)
	first, err := types.ParseDecimal64("1.20", decimalType.Width, decimalType.Scale)
	if err != nil {
		t.Fatal(err)
	}
	second, err := types.ParseDecimal64("12.00", decimalType.Width, decimalType.Scale)
	if err != nil {
		t.Fatal(err)
	}
	if err = vector.AppendFixed[types.Decimal64](decimalVec, first, false, mp); err != nil {
		t.Fatal(err)
	}
	if err = vector.AppendFixed[types.Decimal64](decimalVec, second, false, mp); err != nil {
		t.Fatal(err)
	}
	decimalData, err := decimalVec.MarshalBinary()
	decimalVec.Free(mp)
	if err != nil {
		t.Fatal(err)
	}

	for name, test := range map[string]struct {
		typ  planpb.Type
		data []byte
		want string
	}{
		"datetime": {
			typ:  planpb.Type{Id: int32(types.T_datetime), Scale: 6},
			data: datetimeData,
			want: "[2024-01-02 03:04:05.123456, NULL]",
		},
		"decimal": {
			typ:  planpb.Type{Id: int32(types.T_decimal64), Scale: 2},
			data: decimalData,
			want: "[1.20, 12.00]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			expr := &planpb.Expr{
				Typ: test.typ,
				Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
					Len:  int32(strings.Count(test.want, ",") + 1),
					Data: test.data,
				}},
			}
			var buf bytes.Buffer
			if err := describeExpr(t.Context(), expr, &ExplainOptions{
				Format:                 EXPLAIN_FORMAT_JSON,
				CompleteLiteralVectors: true,
			}, &buf); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != test.want {
				t.Fatalf("typed literal vector rendering changed: got %q, want %q", got, test.want)
			}
		})
	}
}

func TestPublicSerializedINListExplainIsOpaqueAndPrintable(t *testing.T) {
	planText := explainSQLForSerializedTest(
		t,
		"select n_name from nation where n_name in ("+
			"serial(cast(99999 as decimal(38,0))), "+
			"serial(cast(100000 as decimal(38,0))))",
	)
	assertPrintableExplainText(t, planText)
	if !strings.Contains(planText, "<opaque>") {
		t.Fatalf("serialized IN-list was exposed in EXPLAIN: %q", planText)
	}
}

func TestOrdinaryINListExplainRemainsMeaningful(t *testing.T) {
	planText := explainSQLForSerializedTest(
		t,
		"select n_name from nation where n_name in ('Résumé', '東京')",
	)
	assertPrintableExplainText(t, planText)
	for _, value := range []string{"Résumé", "東京"} {
		if !strings.Contains(planText, value) {
			t.Fatalf("ordinary IN-list value %q was hidden in EXPLAIN: %q", value, planText)
		}
	}
	if strings.Contains(planText, "<opaque>") {
		t.Fatalf("ordinary IN-list acquired serialized provenance: %q", planText)
	}
}

func TestMalformedLiteralVecExplainIsTotal(t *testing.T) {
	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
			Len:  1,
			Data: []byte{0xff},
		}},
	}
	var buf bytes.Buffer
	if err := describeExpr(t.Context(), expr, NewExplainDefaultOptions(), &buf); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), "<invalid-vector>"; got != want {
		t.Fatalf("malformed vector diagnostic changed: got %q, want %q", got, want)
	}
}

func explainSQLForSerializedTest(t *testing.T, sql string) string {
	t.Helper()
	stmt, err := mysql.ParseOne(t.Context(), sql, 1)
	if err != nil {
		t.Fatal(err)
	}
	query, err := planpkg.NewBaseOptimizer(planpkg.NewMockCompilerContext(true)).Optimize(stmt, false)
	if err != nil {
		t.Fatal(err)
	}
	buffer := NewExplainDataBuffer()
	if err := NewExplainQueryImpl(query).ExplainPlan(
		t.Context(), buffer, NewExplainDefaultOptions(),
	); err != nil {
		t.Fatal(err)
	}
	return buffer.ToString()
}

func assertPrintableExplainText(t *testing.T, text string) {
	t.Helper()
	if !utf8.ValidString(text) {
		t.Fatalf("EXPLAIN text is not valid UTF-8: %x", []byte(text))
	}
	for _, r := range text {
		if r != '\n' && !unicode.IsPrint(r) {
			t.Fatalf("EXPLAIN text contains non-printable rune %U: %q", r, text)
		}
	}
}

func describeComparisonForTest(t *testing.T, columnName, op, literal string) string {
	t.Helper()
	return describeComparisonForLiteralTest(t, columnName, op, literal, types.T_varchar, false, false)
}

func describeComparisonForTypeTest(t *testing.T, columnName, op, literal string, typ types.T) string {
	t.Helper()
	return describeComparisonForLiteralTest(t, columnName, op, literal, typ, false, false)
}

func describeComparisonForLiteralTest(
	t *testing.T,
	columnName, op, literal string,
	typ types.T,
	isBinary bool,
	isSerialized bool,
) string {
	t.Helper()

	registered, err := function.GetFunctionByName(context.Background(), op, []types.Type{typ.ToType(), typ.ToType()})
	if err != nil {
		t.Fatalf("resolve comparison %q: %v", op, err)
	}
	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: registered.GetEncodedOverloadID(), ObjName: op},
			Args: []*planpb.Expr{
				{
					Typ: planpb.Type{Id: int32(typ)},
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
						Name: columnName,
					}},
				},
				{
					Typ: planpb.Type{Id: int32(typ)},
					Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
						Value:        &planpb.Literal_Sval{Sval: literal},
						IsBin:        isBinary,
						IsSerialized: isSerialized,
					}},
				},
			},
		}},
	}

	var buf bytes.Buffer
	if err := describeExpr(context.Background(), expr, NewExplainDefaultOptions(), &buf); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func mustDecodeHex(t *testing.T, encoded string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
