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
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// This complete tree was authored by the merge-base DDL binder, before the
// new BIT_COUNT overloads. It combines float32, float64 and integer operands.
func TestTableDumpBitCountMergeBaseBinding(t *testing.T) {
	data, err := os.ReadFile("testdata/bit_count_binding_dea4f40.json")
	require.NoError(t, err)
	var snapshot struct {
		Source     string `json:"source"`
		SQL        string `json:"sql"`
		Definition []byte `json:"definition"`
	}
	require.NoError(t, json.Unmarshal(data, &snapshot))
	require.Equal(t, "dea4f40b17d0893da6823fec1fdd27cf6493dea3", snapshot.Source)
	var source pb.TableDef
	require.NoError(t, source.Unmarshal(snapshot.Definition))
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	target := tableDumpDDLCatalog(tableDumpDDLCreate(t, compiler, snapshot.SQL))
	sensitive, err := AnalyzeTableDumpBindings(compiler, target, &source)
	require.NoError(t, err)
	require.True(t, sensitive)

	forged := DeepCopyTableDef(&source, true)
	var changed bool
	require.NoError(t, pb.VisitExprTree(tableDumpDDLColumn(t, forged, "g").GeneratedCol.Expr, func(expr *pb.Expr) error {
		fn := expr.GetF()
		if changed || fn == nil || fn.Func == nil || fn.Func.ObjName != "bit_count" || len(fn.Args) != 1 ||
			types.T(fn.Args[0].Typ.Id) != types.T_float64 {
			return nil
		}
		_, old := function.DecodeOverloadID(fn.Func.Obj)
		require.Equal(t, int32(function.BitCountRoundedFloat64Overload), old)
		fn.Func.Obj = fn.Func.Obj&^0xffffffff | 16
		changed = true
		return nil
	}))
	require.True(t, changed)
	_, err = AnalyzeTableDumpBindings(compiler, target, forged)
	require.ErrorContains(t, err, "cannot verify bound generated expression")
}

func TestTableDumpIntegerBitCountRejectsForgedLegacyCast(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	target := tableDumpDDLCatalog(tableDumpDDLCreate(t, compiler,
		"create table t(i int, g double generated always as (cast(bit_count(i) as double)) stored)"))
	forged := DeepCopyTableDef(target, true)
	var changed bool
	require.NoError(t, pb.VisitExprTree(tableDumpDDLColumn(t, forged, "g").GeneratedCol.Expr, func(expr *pb.Expr) error {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" ||
			types.T(expr.Typ.Id) != types.T_float64 {
			return nil
		}
		_, old := function.DecodeOverloadID(fn.Func.Obj)
		require.Equal(t, int32(1), old)
		fn.Func.Obj &^= 0xffffffff
		fn.SyntaxExplicitCast = true
		changed = true
		return nil
	}))
	require.True(t, changed)
	_, err := AnalyzeTableDumpBindings(compiler, target, forged)
	require.ErrorContains(t, err, "cannot verify bound generated expression")
}

// The fixture contains attr_generated directly from the official v4.2.8
// catalog, rather than a tree constructed with the compatibility binder.
func TestTableDumpV428IntegerCatalogBindings(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	const ddl = "create table t(id int primary key, i int, s double generated always as (CAST(BIT_COUNT(i) AS DOUBLE)) stored, v double generated always as (CAST(BIT_COUNT(i) AS DOUBLE)) virtual)"
	current := tableDumpDDLCatalog(tableDumpDDLCreate(t, compiler, ddl))
	legacy := DeepCopyTableDef(current, true)
	data, err := os.ReadFile("testdata/bit_count_v428_integer_catalog.tsv")
	require.NoError(t, err)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		parts := strings.Split(line, "\t")
		require.Len(t, parts, 3)
		wire, err := hex.DecodeString(parts[1])
		require.NoError(t, err)
		generated := tableDumpDDLColumn(t, legacy, parts[0]).GeneratedCol
		generated.Reset()
		require.NoError(t, generated.Unmarshal(wire))
	}
	for _, tc := range []struct {
		name             string
		target, supplied *pb.TableDef
		sensitive        bool
	}{
		{"fresh v1", current, nil, false},
		{"historical DUMP", legacy, legacy, true},
		{"historical LOAD", current, legacy, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sensitive, err := AnalyzeTableDumpBindings(compiler, tc.target, tc.supplied)
			require.NoError(t, err)
			require.Equal(t, tc.sensitive, sensitive)
		})
	}
}

func TestTableDumpBitCountBindingAcrossOwners(t *testing.T) {
	for _, tc := range []struct {
		name, ddl string
		sensitive bool
	}{
		{"integer check", "create table t(i int, constraint ck check(cast(bit_count(i) as double)>=0))", false},
		{"floating check", "create table t(d double, constraint ck check(bit_count(cast(d as double))>=0))", true},
		{"binary", "create table t(b varbinary(8), d double generated always as (cast(bit_count(b) as double)) stored)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			table := tableDumpDDLCatalog(tableDumpDDLCreate(t, compiler, tc.ddl))
			for _, supplied := range []*pb.TableDef{nil, table} {
				sensitive, err := AnalyzeTableDumpBindings(compiler, table, supplied)
				require.NoError(t, err)
				require.Equal(t, tc.sensitive, sensitive)
			}
		})
	}
}
