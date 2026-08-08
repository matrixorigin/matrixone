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
	}{
		{
			name:       "invalid UTF-8 decimal lower bound",
			columnName: catalog.IndexTableIndexColName,
			op:         ">=",
			boundHex:   "458000000000000000000000000002673c",
		},
		{
			name:       "non-printable varchar upper bound",
			columnName: qualifiedColumn,
			op:         "<",
			boundHex:   "46016100",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			serializedBound := mustDecodeHex(t, test.boundHex)
			got := describeComparisonForTest(t, test.columnName, test.op, string(serializedBound))
			if !utf8.ValidString(got) {
				t.Fatalf("EXPLAIN expression is not valid UTF-8: %x", []byte(got))
			}
			for _, r := range got {
				if !unicode.IsPrint(r) {
					t.Fatalf("EXPLAIN expression contains non-printable rune %U: %q", r, got)
				}
			}
			if strings.Contains(got, string(serializedBound)) {
				t.Fatalf("EXPLAIN expression exposes serialized secondary-index bytes: %x", []byte(got))
			}
			if !strings.Contains(got, catalog.IndexTableIndexColName+" "+test.op) {
				t.Fatalf("EXPLAIN expression lost the range predicate: %q", got)
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

func describeComparisonForTest(t *testing.T, columnName, op, literal string) string {
	t.Helper()
	return describeComparisonForTypeTest(t, columnName, op, literal, types.T_varchar)
}

func describeComparisonForTypeTest(t *testing.T, columnName, op, literal string, typ types.T) string {
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
						Value: &planpb.Literal_Sval{Sval: literal},
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
