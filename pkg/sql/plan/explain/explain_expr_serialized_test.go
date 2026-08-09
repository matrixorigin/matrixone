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
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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
