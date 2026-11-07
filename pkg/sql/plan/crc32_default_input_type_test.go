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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// A retained legacy CRC32 DEFAULT whose referenced input no longer has the
// bound representation must not be copied. Otherwise COPY would publish
// overload 0 against e.g. a BIGINT column and INSERT would evaluate the
// default through a JSON varlena reader.
func TestCRC32DefaultPreserveRejectsConvertedInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := planpb.Type{Id: int32(types.T_uint64)}
	source := &planpb.ColDef{
		Name: "c",
		Typ:  typ,
		Default: &planpb.Default{
			NullAbility:  true,
			OriginString: "(crc32(j))",
			Expr:         legacyCRC32Expr(),
		},
	}
	stmt, err := mysql.ParseOne(proc.Ctx, "create table t(c bigint unsigned default (crc32(j)))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)

	for _, inputType := range []types.T{types.T_int64, types.T_varchar, types.T_uint64} {
		t.Run(inputType.String(), func(t *testing.T) {
			copied := proto.Clone(source).(*planpb.ColDef)
			changed := []*planpb.ColDef{
				{Name: "j", Typ: planpb.Type{Id: int32(inputType)}},
				copied,
			}
			got, err := buildDefaultExprWithColumns(proc.Ctx, col, typ, proc, changed, copied)
			require.ErrorContains(t, err, "explicit table rebuild")
			require.Nil(t, got)
			require.Equal(t, int32(types.T_json), copied.Default.Expr.GetF().Args[0].Typ.Id,
				"rejected preserve must not mutate the source default")
		})
	}
}

// Unchanged JSON inputs keep their bound identity across COPY.
func TestCRC32DefaultPreserveKeepsMatchingInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := planpb.Type{Id: int32(types.T_uint64)}
	source := &planpb.ColDef{
		Name: "c",
		Typ:  typ,
		Default: &planpb.Default{
			NullAbility:  true,
			OriginString: "(crc32(j))",
			Expr:         legacyCRC32Expr(),
		},
	}
	stmt, err := mysql.ParseOne(proc.Ctx, "create table t(c bigint unsigned default (crc32(j)))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	matching := []*planpb.ColDef{
		{Name: "j", Typ: planpb.Type{Id: int32(types.T_json)}},
		source,
	}
	got, err := buildDefaultExprWithColumns(proc.Ctx, col, typ, proc, matching, source)
	require.NoError(t, err)
	require.True(t, containsLegacyCRC32(got.Expr))
	require.NotSame(t, source.Default.Expr, got.Expr)
}

// MODIFY/CHANGE of a legacy CRC32 default input is rejected even when the
// default column itself is not part of the ALTER list.
func TestCRC32DefaultAlterRejectsConvertedInput(t *testing.T) {
	for _, sql := range []string{
		"alter table constraint_test.t_on_update_gen modify column val bigint, algorithm=copy",
		"alter table constraint_test.t_on_update_gen change column val val varchar(32), algorithm=copy",
	} {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			base := mock.ctxt.tables["t_on_update_gen"]
			base.Indexes = nil
			base.Name2ColIndex = make(map[string]int32, len(base.Cols))
			for i, col := range base.Cols {
				base.Name2ColIndex[col.Name] = int32(i)
				col.GeneratedCol = nil
				col.OnUpdate = nil
				col.Default = &planpb.Default{NullAbility: true}
			}
			pos := mockTableColPos(t, base, "val")
			base.Cols[pos].Typ = planpb.Type{Id: int32(types.T_json)}
			base.Cols[pos].Default = &planpb.Default{NullAbility: true}

			// c holds the legacy DEFAULT (crc32(val)) with a JSON-typed ColRef.
			cPos := mockTableColPos(t, base, "id")
			legacy := legacyCRC32Expr()
			legacy.GetF().Args[0].GetCol().ColPos = pos
			legacy.GetF().Args[0].GetCol().Name = "val"
			base.Cols[cPos].Name = "c"
			base.Cols[cPos].Typ = planpb.Type{Id: int32(types.T_uint64)}
			base.Cols[cPos].Default = &planpb.Default{
				NullAbility:  true,
				OriginString: "(crc32(val))",
				Expr:         legacy,
			}
			original := proto.Clone(base).(*planpb.TableDef)
			_, err := runOneStmt(mock, t, sql)
			require.ErrorContains(t, err, "explicit table rebuild")
			require.True(t, proto.Equal(original, base), "rejected ALTER must leave the catalog unchanged")
		})
	}
}

// Same-name CHANGE that repeats the default clause must still reject when
// a sibling input column's representation was converted.
func TestCRC32UnchangedDefaultClauseRejectsConvertedSiblingInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := planpb.Type{Id: int32(types.T_uint64)}
	source := &planpb.ColDef{
		Typ: typ,
		Default: &planpb.Default{
			NullAbility:  true,
			OriginString: "(crc32(j))",
			Expr:         legacyCRC32Expr(),
		},
	}
	stmt, err := mysql.ParseOne(proc.Ctx, "create table t(c bigint unsigned not null default (crc32(j)))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	preserved := unchangedCRC32ColumnClauses(source, col, typ)
	require.NotNil(t, preserved.Default)

	converted := []*planpb.ColDef{
		{Name: "j", Typ: planpb.Type{Id: int32(types.T_int64)}},
		source,
	}
	_, err = buildDefaultExprWithColumns(proc.Ctx, col, typ, proc, converted, preserved)
	require.ErrorContains(t, err, "explicit table rebuild")
}

// COPY rebuild of the owning table goes through build_ddl's per-column
// default binding. A JSON-to-BIGINT conversion on the hashed input must
// fail that rebuild instead of publishing stale overload-0 metadata.
func TestCRC32DefaultCopyRebuildRejectsConvertedInput(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	built, err := runOneStmt(mock, t,
		"create table t_on_update_gen(j json, c bigint unsigned default (crc32(j)))")
	require.NoError(t, err)
	table := built.GetDdl().GetCreateTable().TableDef
	cCol := FindColumn(table.Cols, "c")
	require.NotNil(t, cCol)
	require.NotNil(t, cCol.Default)
	converted := 0
	err = planpb.VisitExprTree(cCol.Default.Expr, func(expr *planpb.Expr) error {
		f := expr.GetF()
		if f != nil && f.Func.Obj == function.EncodeOverloadID(function.CRC32, function.CRC32JSONTextOverload) {
			f.Func.Obj = function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload)
			converted++
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, converted)
	require.True(t, containsLegacyCRC32(cCol.Default.Expr))
	table.Name2ColIndex = make(map[string]int32, len(table.Cols))
	for i, col := range table.Cols {
		table.Name2ColIndex[col.Name] = int32(i)
	}
	mock.ctxt.tables["t_on_update_gen"] = table

	original := proto.Clone(table).(*planpb.TableDef)
	_, err = runOneStmt(mock, t,
		"alter table constraint_test.t_on_update_gen modify column j bigint, algorithm=copy")
	require.ErrorContains(t, err, "explicit table rebuild")
	require.True(t, proto.Equal(original, mock.ctxt.tables["t_on_update_gen"]))
}

// A retained legacy CRC32 ON UPDATE whose referenced input no longer has the
// bound representation must not be copied. Same contract as DEFAULT.
// Public ON UPDATE grammar accepts only datetime functions, so the AST is
// constructed the way catalog rebuild / table dump callers do.
func TestCRC32OnUpdatePreserveRejectsConvertedInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := planpb.Type{Id: int32(types.T_uint64)}
	source := &planpb.ColDef{
		Name: "c",
		Typ:  typ,
		OnUpdate: &planpb.OnUpdate{
			OriginString: "(crc32(j))",
			Expr:         legacyCRC32Expr(),
		},
	}
	col := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("c"),
		nil,
		[]tree.ColumnAttribute{&tree.AttributeOnUpdate{Expr: tree.NewNumVal("0", "0", false, tree.P_char)}},
	)

	for _, inputType := range []types.T{types.T_int64, types.T_varchar, types.T_uint64} {
		t.Run(inputType.String(), func(t *testing.T) {
			copied := proto.Clone(source).(*planpb.ColDef)
			changed := []*planpb.ColDef{
				{Name: "j", Typ: planpb.Type{Id: int32(inputType)}},
				copied,
			}
			got, err := buildOnUpdate(proc.Ctx, col, typ, proc, changed, copied)
			require.ErrorContains(t, err, "explicit table rebuild")
			require.Nil(t, got)
			require.Equal(t, int32(types.T_json), copied.OnUpdate.Expr.GetF().Args[0].Typ.Id,
				"rejected preserve must not mutate the source on-update")
		})
	}
}

// Unchanged JSON inputs keep their bound ON UPDATE identity across COPY.
func TestCRC32OnUpdatePreserveKeepsMatchingInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := planpb.Type{Id: int32(types.T_uint64)}
	source := &planpb.ColDef{
		Name: "c",
		Typ:  typ,
		OnUpdate: &planpb.OnUpdate{
			OriginString: "(crc32(j))",
			Expr:         legacyCRC32Expr(),
		},
	}
	col := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("c"),
		nil,
		[]tree.ColumnAttribute{&tree.AttributeOnUpdate{Expr: tree.NewNumVal("0", "0", false, tree.P_char)}},
	)
	matching := []*planpb.ColDef{
		{Name: "j", Typ: planpb.Type{Id: int32(types.T_json)}},
		source,
	}
	got, err := buildOnUpdate(proc.Ctx, col, typ, proc, matching, source)
	require.NoError(t, err)
	require.True(t, containsLegacyCRC32(got.Expr))
	require.NotSame(t, source.OnUpdate.Expr, got.Expr)
}

// Sibling input conversion is rejected even when the ON UPDATE column itself
// is not the column being rewritten (COPY / table rebuild).
func TestCRC32OnUpdateUnchangedClauseRejectsConvertedSiblingInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := planpb.Type{Id: int32(types.T_uint64)}
	source := &planpb.ColDef{
		Name: "c",
		Typ:  typ,
		OnUpdate: &planpb.OnUpdate{
			OriginString: "(crc32(j))",
			Expr:         legacyCRC32Expr(),
		},
	}
	col := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("c"),
		nil,
		[]tree.ColumnAttribute{&tree.AttributeOnUpdate{Expr: tree.NewNumVal("0", "0", false, tree.P_char)}},
	)
	copied := proto.Clone(source).(*planpb.ColDef)
	converted := []*planpb.ColDef{
		{Name: "j", Typ: planpb.Type{Id: int32(types.T_int64)}},
		copied,
	}
	_, err := buildOnUpdate(proc.Ctx, col, typ, proc, converted, copied)
	require.ErrorContains(t, err, "explicit table rebuild")
}
