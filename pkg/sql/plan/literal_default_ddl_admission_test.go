// Copyright 2026 Matrix Origin
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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// parseDDLDefaultColumn parses a single-column CREATE TABLE declaration and
// returns its column definition. Callers own the parsed statement.
func parseDDLDefaultColumn(t testing.TB, decl string) *tree.ColumnTableDef {
	t.Helper()
	stmt, err := mysql.ParseOne(context.Background(), "create table t ("+decl+")", 1)
	require.NoError(t, err)
	createTable, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)
	return colDef
}

func buildDDLColumnDefault(t testing.TB, proc *process.Process, decl string) (*plan.Default, error) {
	t.Helper()
	colDef := parseDDLDefaultColumn(t, decl)
	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)
	return buildDefaultExpr(proc.Ctx, colDef, typ, proc)
}

// #31860: a bare numeric literal DEFAULT whose converted value does not fit the
// destination CHAR/VARCHAR width must be rejected with ER_INVALID_DEFAULT, like
// an over-long string literal, instead of silently storing truncated text. The
// stored length is decided by the converted value, never by the source SQL text
// (signs, exponents and decimal scale make text length unrelated to the value).
func TestDDLNumericLiteralDefaultRejectsCharWidthOverflow(t *testing.T) {
	for _, mode := range []string{"STRICT_TRANS_TABLES", ""} {
		for _, tc := range []struct {
			name string
			decl string
			want string // folded stored default for admitted cases
			fail bool
		}{
			{name: "int wider than varchar", decl: "v varchar(3) default 12345", fail: true},
			{name: "int wider than char", decl: "v char(3) default 12345", fail: true},
			{name: "decimal wider than varchar", decl: "v varchar(3) default 1.25", fail: true},
			{name: "signed decimal wider than varchar", decl: "v varchar(3) default -1.25", fail: true},
			{name: "int one over boundary", decl: "v varchar(3) default 1234", fail: true},
			{name: "decimal scale keeps digits", decl: "v varchar(4) default 1.250", fail: true},
			{name: "int fits exactly", decl: "v varchar(3) default 123", want: "123"},
			{name: "decimal fits", decl: "v varchar(4) default 1.25", want: "1.25"},
			{name: "int fits char", decl: "v char(5) default 12345", want: "12345"},
			{name: "zero fits one char", decl: "v varchar(1) default 0", want: "0"},
			// #31860 P1: uint64 and scientific float64 literals bind raw
			// (baseBinder.bindNumVal returns uncast numeric), so the width probe
			// must measure the real destination conversion, not GetSval of a
			// numeric literal. Pairs cover overflow/exact-fit on both families.
			{name: "uint64 wider than varchar", decl: "v varchar(19) default 18446744073709551615", fail: true},
			{name: "uint64 fits varchar exactly", decl: "v varchar(20) default 18446744073709551615", want: "18446744073709551615"},
			{name: "uint64 wider than char", decl: "v char(19) default 18446744073709551615", fail: true},
			{name: "uint64 fits char exactly", decl: "v char(20) default 18446744073709551615", want: "18446744073709551615"},
			{name: "scientific wider than varchar", decl: "v varchar(5) default 1e5", fail: true},
			{name: "scientific fits varchar exactly", decl: "v varchar(6) default 1e5", want: "100000"},
			{name: "scientific wider than char", decl: "v char(5) default 1e5", fail: true},
			{name: "scientific fits char exactly", decl: "v char(6) default 1e5", want: "100000"},
			{name: "negative scientific wider than varchar", decl: "v varchar(6) default -1e5", fail: true},
			{name: "negative scientific fits varchar exactly", decl: "v varchar(7) default -1e5", want: "-100000"},
			{name: "signed decimal one over boundary", decl: "v varchar(4) default -1.25", fail: true},
			{name: "signed decimal fits varchar exactly", decl: "v varchar(5) default -1.25", want: "-1.25"},
			{name: "signed decimal wider than char", decl: "v char(4) default -1.25", fail: true},
			{name: "signed decimal fits char exactly", decl: "v char(5) default -1.25", want: "-1.25"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
					if name == "sql_mode" {
						return mode, nil
					}
					return "", nil
				})
				value, err := buildDDLColumnDefault(t, proc, tc.decl)
				if tc.fail {
					require.Error(t, err, "admission must reject the wide default under sql_mode=%q", mode)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT, got %v", err)
					require.Contains(t, err.Error(), "'v'")
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, value.Expr.GetLit().GetSval())
			})
		}
	}
}

// #31894: a bare literal DEFAULT that cannot be converted to the destination
// type must report ER_INVALID_DEFAULT with the actual authored column name,
// not the inner cast error (20203 invalid argument / 1690 out of range). All
// five authoring forms share this owner, so the ADD COLUMN shape is probed
// with the new column name c2. The mapping is sql_mode independent, matching
// MySQL 8.4 prepare_default_value: disallowed conversion statuses are rejected
// in strict and empty sql_mode alike.
func TestDDLLiteralDefaultConversionFailureReportsInvalidDefault(t *testing.T) {
	for _, mode := range []string{"STRICT_TRANS_TABLES", ""} {
		for _, tc := range []struct {
			name, decl, column string
		}{
			{name: "int from bad string", decl: "c int default 'abc'", column: "'c'"},
			{name: "int from prefixed digits", decl: "c int default '12x'", column: "'c'"},
			{name: "int out of range", decl: "c int default 99999999999", column: "'c'"},
			{name: "int unary plus bare literal out of range", decl: "c int default +99999999999", column: "'c'"},
			{name: "tinyint out of range", decl: "c tinyint default 300", column: "'c'"},
			{name: "tinyint unary plus bare literal out of range", decl: "c tinyint default +300", column: "'c'"},
			{name: "date from bad string", decl: "c date default 'abc'", column: "'c'"},
			{name: "add column names the new column", decl: "c2 int default 'abc'", column: "'c2'"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
					if name == "sql_mode" {
						return mode, nil
					}
					return "", nil
				})
				_, err := buildDDLColumnDefault(t, proc, tc.decl)
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT under sql_mode=%q, got %v", mode, err)
				require.Contains(t, err.Error(), tc.column)
			})
		}
	}
}

// E1: a parenthesized signed decimal DEFAULT (-1.25) is an expression default,
// not a bare literal. The bare-literal source-domain rebinding must not reach
// it: binding keeps the destination-driven owner, and the raw cast error
// classification (20203 invalid argument) must not be rewrapped as
// ER_INVALID_DEFAULT. Both sql_modes and both CHAR/VARCHAR families probe the
// same boundary on fitting and overflowing widths.
func TestDDLParenthesizedSignedDecimalDefaultKeepsExpressionOwner(t *testing.T) {
	for _, mode := range []string{"STRICT_TRANS_TABLES", ""} {
		for _, tc := range []struct {
			name, decl string
		}{
			{name: "varchar fitting width", decl: "v varchar(5) default (-1.25)"},
			{name: "varchar narrow width", decl: "v varchar(3) default (-1.25)"},
			{name: "char fitting width", decl: "v char(5) default (-1.25)"},
			{name: "char narrow width", decl: "v char(3) default (-1.25)"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
					if name == "sql_mode" {
						return mode, nil
					}
					return "", nil
				})
				_, err := buildDDLColumnDefault(t, proc, tc.decl)
				require.Error(t, err, "the expression default keeps its existing rejection")
				require.False(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "expression defaults must not be rewrapped as ER_INVALID_DEFAULT, got %v", err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidArg), "the expression default keeps the raw cast error owner, got %v", err)
			})
		}
	}
}

// Historical catalogs keep defaults admitted before the width admission, for
// example varchar(3) DEFAULT 12345 stored as '123'. Reading those persisted
// defaults through the verification zone must keep the legacy binding; only the
// defaultExprForDDL authoring zone rejects them. This is the regression guard
// for the table-dump breakage recorded in the previous closed fix attempt.
func TestPersistedVerificationStillAdmitsLegacyNumericWidthDefault(t *testing.T) {
	for decl, stored := range map[string]string{
		"v varchar(3) default 12345": "123",
		"v varchar(3) default 1.25":  "1.2",
	} {
		t.Run(decl, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			colDef := parseDDLDefaultColumn(t, decl)
			typ, err := getTypeFromAst(context.Background(), colDef.Type)
			require.NoError(t, err)

			reading, err := buildDefaultExprWithColumnsObserved(proc.Ctx, colDef, typ, proc, nil, defaultExprForPersistedVerification, nil)
			require.NoError(t, err, "the persisted reading zone must keep the old admission")
			require.Equal(t, stored, reading.Expr.GetLit().GetSval())

			// The same declaration authored as new DDL must now be rejected.
			_, err = buildDefaultExprWithColumnsObserved(proc.Ctx, colDef, typ, proc, nil, defaultExprForDDL, nil)
			require.Error(t, err, "new DDL authoring must reject the wide numeric default")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT, got %v", err)
		})
	}
}

// Neighbouring default contracts keep their owners: expression defaults keep
// the inner error, existing string-width and typed-domain admissions keep
// ER_INVALID_DEFAULT, volatile defaults stay unevaluated, non-CHAR/VARCHAR
// destinations stay outside the new width admission, and ON UPDATE keeps the
// raw cast error.
func TestDDLLiteralDefaultAdmissionKeepsAdjacentContracts(t *testing.T) {
	t.Run("expression default keeps truncating owner", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		value, err := buildDDLColumnDefault(t, proc, "v varchar(3) default (12345)")
		require.NoError(t, err)
		require.Equal(t, "123", value.Expr.GetLit().GetSval())
	})
	t.Run("expression default keeps inner conversion error", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		_, err := buildDDLColumnDefault(t, proc, "v tinyint default (300)")
		require.Error(t, err)
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "expression defaults must not be rewrapped, got %v", err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
	})
	t.Run("overlong string literal still rejected", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		_, err := buildDDLColumnDefault(t, proc, "v varchar(3) default 'abcd'")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault))
	})
	t.Run("mediumint out of range keeps dedicated report", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		_, err := buildDDLColumnDefault(t, proc, "v mediumint(9) default 8388608")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault))
	})
	t.Run("unsigned decimal negative keeps dedicated report", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		_, err := buildDDLColumnDefault(t, proc, "v decimal(5,2) unsigned default -1.25")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault))
	})
	t.Run("volatile temporal default stays admitted", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		_, err := buildDDLColumnDefault(t, proc, "d datetime default now()")
		require.NoError(t, err)
	})
	t.Run("text destination stays outside width admission", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		value, err := buildDDLColumnDefault(t, proc, "v text default 12345")
		require.NoError(t, err)
		require.Equal(t, "12345", value.Expr.GetLit().GetSval())
	})
	t.Run("json literal default rejection is unchanged", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		_, err := buildDDLColumnDefault(t, proc, "v json default 'x'")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	})
	t.Run("on update keeps raw conversion error", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		onUpdateCol := tree.NewColumnTableDef(
			tree.NewUnresolvedColName("v"),
			nil,
			[]tree.ColumnAttribute{
				&tree.AttributeOnUpdate{Expr: tree.NewNumVal("abc", "abc", false, tree.P_char)},
			},
		)
		_, err := buildOnUpdate(proc.Ctx, onUpdateCol, plan.Type{Id: int32(types.T_int64)}, proc)
		require.Error(t, err)
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "ON UPDATE must keep the raw cast error, got %v", err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidArg))
	})
}

// legacyNumericWidthFixtureFile pins the provenance of the committed
// old-catalog fixture: exact base commit, generator identity and the
// serialized historical TableDef payloads produced by that pristine tree.
type legacyNumericWidthFixtureFile struct {
	BaseCommit   string            `json:"base_commit"`
	Generator    string            `json:"generator"`
	GoVersion    string            `json:"go_version"`
	GeneratedAt  string            `json:"generated_at"`
	Cases        map[string]string `json:"cases"`
	StoredValues map[string]string `json:"stored_values"`
	Fixtures     map[string][]byte `json:"fixtures"`
}

// The committed fixture records real catalog states authored by the pristine
// tree at the base commit: `v varchar(3) DEFAULT 12345` folded to the stored
// default '123', and `DEFAULT 1.25` folded to '1.2'. Those historical payload
// bytes must keep verifying through the table-dump binding while the same SQL
// authored today is rejected by the new DDL admission, and a forged stored
// payload must not verify. This is the compatibility boundary the maintainer
// required after the table-dump breakage in the previous closed fix attempt.
func TestTableDumpVerifiesLegacyNumericWidthLiteralDefault(t *testing.T) {
	const fixturePath = "testdata/legacy_numeric_width_literal_default_ea184ad906.json"
	data, err := os.ReadFile(fixturePath)
	require.NoError(t, err)
	require.Equal(t, "e9643286b47e38c001feb8f4a186adcd56f248ad296cc384b15a45577757b643", fmt.Sprintf("%x", sha256.Sum256(data)))

	var file legacyNumericWidthFixtureFile
	require.NoError(t, json.Unmarshal(data, &file))
	require.Equal(t, "ea184ad90624ae3dba8ced628ac019271a6c309c", file.BaseCommit)
	require.Equal(t, "TestGenerateLegacyNumericWidthLiteralDefaultFixture", file.Generator)
	require.Len(t, file.Fixtures, 2)

	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables["t"] = &plan.TableDef{Name: "t"}

	for name, sql := range file.Cases {
		t.Run(name, func(t *testing.T) {
			var source plan.TableDef
			require.NoError(t, proto.Unmarshal(file.Fixtures[name], &source))

			var legacyCol *plan.ColDef
			for _, col := range source.Cols {
				if col.Name == "v" {
					legacyCol = col
				}
			}
			require.NotNil(t, legacyCol, "fixture must carry the v column")
			require.Equal(t, file.StoredValues[name], legacyCol.Default.Expr.GetLit().GetSval(), "fixture must encode the pristine stored default")
			require.Equal(t, int32(3), legacyCol.Typ.Width)

			// Historical payload bytes: unchanged self-verification.
			snapshot := proto.Clone(&source)
			_, err := AnalyzeTableDumpBindings(compiler, &source, &source)
			require.NoError(t, err, "legacy dump verification must keep passing: %v", err)
			require.True(t, proto.Equal(snapshot, &source))

			// The same SQL authored today is rejected by the new DDL admission.
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(compiler, stmt, false, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT for %q, got %v", sql, err)

			// A forged stored default payload must not verify.
			forged := proto.Clone(&source).(*plan.TableDef)
			for _, col := range forged.Cols {
				if col.Name == "v" {
					col.Default.Expr = &plan.Expr{
						Typ: plan.Type{Id: int32(types.T_varchar), Width: 3},
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Value: &plan.Literal_Sval{Sval: "77"},
						}},
					}
				}
			}
			_, err = AnalyzeTableDumpBindings(compiler, forged, forged)
			require.ErrorContains(t, err, "cannot verify bound default expression")
		})
	}
}

// A newly authored fitting signed-decimal default must survive the real
// table-dump consumer: the CREATE plan's TableDef self-verifies through
// AnalyzeTableDumpBindings with the exact stored value, origin text and type,
// the verification mutates nothing, and a forged stored payload is still
// rejected. This closes the create-then-dump loop for the signed-literal
// correction; the historical positive-width fixtures above keep their own
// immutable payload bytes.
func TestTableDumpVerifiesNewSignedDecimalLiteralDefault(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))

	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t (v varchar(5) default -1.25)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false, nil)
	require.NoError(t, err)
	source := built.GetDdl().GetCreateTable().GetTableDef()
	require.NotNil(t, source)

	var col *plan.ColDef
	for _, c := range source.Cols {
		if c.Name == "v" {
			col = c
		}
	}
	require.NotNil(t, col, "the CREATE plan must carry the v column")
	require.Equal(t, "-1.25", col.Default.Expr.GetLit().GetSval(), "the stored default is the converted value")
	require.Equal(t, int32(types.T_varchar), col.Typ.Id)
	require.Equal(t, int32(5), col.Typ.Width)
	// An authored bare signed plain-decimal keeps its bare origin: a
	// formatter-inserted pair of parentheses would reclassify the literal as
	// an older era's expression default on every origin reparse (catalog
	// rebinding, ALTER round-trip, dump analysis). Assert the exact text so
	// a dropped sign cannot hide behind a substring check.
	require.Equal(t, "-1.25", col.Default.OriginString, "origin text keeps the exact signed literal, sign included")

	snapshot := proto.Clone(source)
	_, err = AnalyzeTableDumpBindings(compiler, source, source)
	require.NoError(t, err, "the persisted reading of a new signed default must keep verifying: %v", err)
	require.True(t, proto.Equal(snapshot, source), "dump verification must not mutate the persisted payload")

	// The persisted reading zone re-binds the same authored shape without the
	// width admission and must reconstruct the identical stored value.
	proc := testutil.NewProcess(t)
	colDef := parseDDLDefaultColumn(t, "v varchar(5) default -1.25")
	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)
	reading, err := buildDefaultExprWithColumnsObserved(proc.Ctx, colDef, typ, proc, nil, defaultExprForPersistedVerification, nil)
	require.NoError(t, err, "persisted verification must rebuild the new signed default")
	require.Equal(t, "-1.25", reading.Expr.GetLit().GetSval())

	// A forged stored payload must not verify.
	forged := proto.Clone(source).(*plan.TableDef)
	for _, c := range forged.Cols {
		if c.Name == "v" {
			c.Default.Expr = &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: 5},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "-9.9"}}},
			}
		}
	}
	_, err = AnalyzeTableDumpBindings(compiler, forged, forged)
	require.ErrorContains(t, err, "cannot verify bound default expression")
}

// Reviewer regression (#5764078751): an admitted parenthesized expression
// default must keep its historical producer end to end. DDL folds (-0.25)
// into VARCHAR(1) through the destination-cast owner and stores '0'; the
// persisted dump verification must reconstruct that exact stored value
// instead of rebinding the parenthesized origin in the numeric literal
// domain. The stored value distinguishes the two producers: a bare authored
// signed decimal keeps its own literal reconstruction in the tests above,
// and a mismatched stored payload keeps failing verification.
func TestTableDumpVerifiesAdmittedParenthesizedSignedDecimalExpressionDefault(t *testing.T) {
	// The authoring zone keeps the historical expression owner and its
	// folded value: the destination cast truncates the magnitude to '0'
	// before the unary minus, so the parenthesized form stays admitted.
	proc := testutil.NewProcess(t)
	value, err := buildDDLColumnDefault(t, proc, "v varchar(1) default (-0.25)")
	require.NoError(t, err, "the parenthesized expression default keeps its historical admission")
	require.Equal(t, "0", value.Expr.GetLit().GetSval())

	// The complete CREATE plan stores the same catalog state and must keep
	// verifying through the real dump consumer unchanged.
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t (v varchar(1) default (-0.25))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false, nil)
	require.NoError(t, err)
	source := built.GetDdl().GetCreateTable().GetTableDef()
	require.NotNil(t, source)

	var col *plan.ColDef
	for _, c := range source.Cols {
		if c.Name == "v" {
			col = c
		}
	}
	require.NotNil(t, col, "the CREATE plan must carry the v column")
	require.Equal(t, "0", col.Default.Expr.GetLit().GetSval(), "the stored default is the historical folded value")
	require.Equal(t, "(-0.25)", col.Default.OriginString, "origin text keeps the parenthesized expression form")

	snapshot := proto.Clone(source)
	_, err = AnalyzeTableDumpBindings(compiler, source, source)
	require.NoError(t, err, "the admitted expression default must keep verifying through the dump consumer: %v", err)
	require.True(t, proto.Equal(snapshot, source), "dump verification must not mutate the persisted payload")

	// The persisted reading zone re-binds the same origin without the new
	// width admission and must reconstruct the identical stored value.
	proc = testutil.NewProcess(t)
	colDef := parseDDLDefaultColumn(t, "v varchar(1) default (-0.25)")
	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)
	reading, err := buildDefaultExprWithColumnsObserved(proc.Ctx, colDef, typ, proc, nil, defaultExprForPersistedVerification, nil)
	require.NoError(t, err, "persisted verification must rebuild the admitted expression default")
	require.Equal(t, "0", reading.Expr.GetLit().GetSval())

	// A forged stored payload must not verify.
	forged := proto.Clone(source).(*plan.TableDef)
	for _, c := range forged.Cols {
		if c.Name == "v" {
			c.Default.Expr = &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: 1},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "7"}}},
			}
		}
	}
	_, err = AnalyzeTableDumpBindings(compiler, forged, forged)
	require.ErrorContains(t, err, "cannot verify bound default expression")
}

// Review 5766780637: a leading-zero bare signed decimal authored by new DDL
// stores the normalized numeric-domain value while the origin keeps the raw
// literal, and the historical destination-directed chain folds that raw text
// into a different value without failing. Both producers therefore bind
// legally, so a first-success pick never lets the matching witness reach the
// stored-expression comparison. The persisted reading owner must compare both
// legal witnesses against the real stored payload, the older tree's fold of
// the same origin keeps verifying, and a tampered payload stays rejected.
func TestTableDumpVerifiesLeadingZeroBareSignedDecimalDefault(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t (v varchar(5) default -000001.25)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false, nil)
	require.NoError(t, err)
	source := built.GetDdl().GetCreateTable().GetTableDef()
	require.NotNil(t, source)
	var col *plan.ColDef
	for _, c := range source.Cols {
		if c.Name == "v" {
			col = c
		}
	}
	require.NotNil(t, col, "the CREATE plan must carry the v column")
	require.Equal(t, "-1.25", col.Default.Expr.GetLit().GetSval(), "the numeric-domain admission normalizes the value")
	require.Equal(t, "-000001.25", col.Default.OriginString, "origin text keeps the raw leading-zero literal, bare as authored")

	// This tree's stored payload must verify end to end.
	snapshot := proto.Clone(source)
	_, err = AnalyzeTableDumpBindings(compiler, source, source)
	require.NoError(t, err, "dump verification must compare both producer witnesses against the stored payload: %v", err)
	require.True(t, proto.Equal(snapshot, source), "dump verification must not mutate the persisted payload")

	// An older tree stored the destination-cast fold of the same literal: the
	// historical raw-text chain truncated '000001.25' to the column width and
	// folded '0', and that era's origin formatter parenthesized the signed
	// literal (the v8.2-era decimal marker did not exist there). Model that
	// payload by keeping the clause's real plan type and literal metadata,
	// restoring the legacy parenthesized origin, and swapping only the folded
	// value and the era marker.
	historical := proto.Clone(source).(*plan.TableDef)
	for _, c := range historical.Cols {
		if c.Name == "v" {
			historicalFold := proto.Clone(c.Default.Expr).(*plan.Expr)
			lit := historicalFold.GetLit()
			lit.Value = &plan.Literal_Sval{Sval: "0"}
			lit.DecimalLiteralRequiresV82 = false
			c.Default.Expr = historicalFold
			c.Default.OriginString = "(-000001.25)"
		}
	}
	_, err = AnalyzeTableDumpBindings(compiler, historical, historical)
	require.NoError(t, err, "the older tree's stored fold of the same origin must keep verifying: %v", err)

	// A tampered payload matching neither witness must not verify.
	forgedZero := proto.Clone(source).(*plan.TableDef)
	for _, c := range forgedZero.Cols {
		if c.Name == "v" {
			c.Default.Expr = &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: 5},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "999"}}},
			}
		}
	}
	_, err = AnalyzeTableDumpBindings(compiler, forgedZero, forgedZero)
	require.ErrorContains(t, err, "cannot verify bound default expression")
}

// Review 5766780637: the width probe must measure the converted text before
// any destination cap can truncate it. Probing through the CHAR family
// maximum makes the probe's own cast clip a 256-character value to 255, so
// the widest value a CHAR(255) destination must reject is silently admitted.
// The exact fit at the maximum, the one-character overflow on both sides of
// the boundary and the nearest VARCHAR controls keep the #31860 admission
// semantics; the value is judged as converted text, never as source length.
func TestDDLCharMaximumWidthBareNumericLiteralAdmission(t *testing.T) {
	fit := "1." + strings.Repeat("2", 253) // exactly 255 characters
	over := fit + "2"                      // 256 characters
	proc := testutil.NewProcess(t)
	value, err := buildDDLColumnDefault(t, proc, "v char(255) default "+fit)
	require.NoError(t, err, "an exact maximum-width converted value stays admitted")
	require.Equal(t, fit, value.Expr.GetLit().GetSval())

	_, err = buildDDLColumnDefault(t, proc, "v char(255) default "+over)
	require.Error(t, err, "a converted value wider than the CHAR maximum must not be silently truncated")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT, got %v", err)

	char254 := "1." + strings.Repeat("2", 252) // exactly 254 characters
	_, err = buildDDLColumnDefault(t, proc, "v char(254) default "+char254)
	require.NoError(t, err, "the nearest narrower CHAR boundary keeps admitting its exact fit")
	_, err = buildDDLColumnDefault(t, proc, "v char(254) default "+fit)
	require.Error(t, err, "a 255-character converted value overflows CHAR(254)")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT, got %v", err)

	_, err = buildDDLColumnDefault(t, proc, "v varchar(255) default "+fit)
	require.NoError(t, err, "the VARCHAR boundary keeps admitting its exact fit")
	_, err = buildDDLColumnDefault(t, proc, "v varchar(255) default "+over)
	require.Error(t, err, "a converted value wider than VARCHAR(255) is rejected")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT, got %v", err)
}

// A bare unary plus literal authored as new DDL is a bare literal like the
// existing unary minus form (review 5771414743): a fitting value stays
// admitted, the plus signed plain decimal keeps its true value text in a
// CHAR/VARCHAR destination, the #31860 width admission judges it through the
// converted text, and a parenthesized (+300) expression default keeps its
// raw owner instead of the conversion remap.
func TestDDLUnaryPlusBareLiteralAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)

	_, err := buildDDLColumnDefault(t, proc, "c tinyint default +100")
	require.NoError(t, err, "a fitting bare unary plus value stays admitted")

	value, err := buildDDLColumnDefault(t, proc, "v varchar(5) default +1.25")
	require.NoError(t, err)
	require.Equal(t, "1.25", value.Expr.GetLit().GetSval())
	// The origin keeps the actual authored unary operator: every persisted
	// consumer (catalog rebind, ALTER round-trip, table-dump analysis)
	// reparses this text, so a dropped or flipped sign silently rebinds the
	// negative of the stored value.
	require.Equal(t, "+1.25", value.OriginString, "origin text keeps the actual unary plus operator, sign included")

	leadingZero, err := buildDDLColumnDefault(t, proc, "v varchar(10) default +001.25")
	require.NoError(t, err)
	require.Equal(t, "1.25", leadingZero.Expr.GetLit().GetSval(), "the numeric-domain admission normalizes the value")
	require.Equal(t, "+001.25", leadingZero.OriginString, "origin text keeps the raw leading-zero literal with its authored plus sign")

	_, err = buildDDLColumnDefault(t, proc, "v varchar(5) default +123456")
	require.Error(t, err, "the width admission judges the bare unary plus literal")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT, got %v", err)

	_, err = buildDDLColumnDefault(t, proc, "c tinyint default (+300)")
	require.Error(t, err)
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault),
		"the parenthesized expression default keeps its raw owner, got %v", err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))

	// The persisted consumer reparses the origin: the plus form must
	// self-verify through the real table-dump analysis exactly like the
	// minus form in TestTableDumpVerifiesNewSignedDecimalLiteralDefault.
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t (v varchar(5) default +1.25)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false, nil)
	require.NoError(t, err)
	source := built.GetDdl().GetCreateTable().GetTableDef()
	require.NotNil(t, source)
	snapshot := proto.Clone(source)
	_, err = AnalyzeTableDumpBindings(compiler, source, source)
	require.NoError(t, err, "the persisted reading of a unary plus default must keep verifying: %v", err)
	require.True(t, proto.Equal(snapshot, source), "dump verification must not mutate the persisted payload")
}

// #31860 at the VARCHAR maximum (review 5771414743): the width probe measures
// the converted text before any capacity truncation, so an exact 65535-
// character value is admitted unchanged while the one-character overflow
// token from the review is rejected instead of being stored as a 65535
// prefix. The control mirrors the collation-only binary family the review
// names, which keeps T_varchar; CHARACTER SET binary conversion and the
// unqualified UTF8 budget are not the witness.
func TestDDLMaximumVarcharBareNumericLiteralAdmission(t *testing.T) {
	fit := "1." + strings.Repeat("2", 65533)  // exactly 65535 characters
	over := "1." + strings.Repeat("2", 65534) // exactly 65536 characters
	proc := testutil.NewProcess(t)

	value, err := buildDDLColumnDefault(t, proc, "v varchar(65535) collate binary default "+fit)
	require.NoError(t, err, "an exact maximum-width converted value stays admitted")
	require.Equal(t, fit, value.Expr.GetLit().GetSval())

	_, err = buildDDLColumnDefault(t, proc, "v varchar(65535) collate binary default "+over)
	require.Error(t, err, "a converted value wider than the VARCHAR maximum must not be stored as a prefix")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT, got %v", err)

	_, err = buildDDLColumnDefault(t, proc, "v varchar(65534) default "+fit)
	require.Error(t, err, "the nearest narrower VARCHAR boundary keeps rejecting the wider value")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "want ER_INVALID_DEFAULT, got %v", err)
}
