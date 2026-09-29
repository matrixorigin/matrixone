// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollationMetadataShowCharsetUsesCatalog(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.dbs["information_schema"] = true
	ctx.objects["character_sets"] = &pb.ObjectRef{SchemaName: "information_schema", ObjName: "character_sets", Obj: 1001}
	ctx.tables["character_sets"] = &pb.TableDef{TblId: 1001, Name: "character_sets", Cols: []*pb.ColDef{
		{Name: "character_set_name", Typ: pb.Type{Id: int32(types.T_varchar), Width: 64}},
		{Name: "default_collate_name", Typ: pb.Type{Id: int32(types.T_varchar), Width: 64}},
		{Name: "description", Typ: pb.Type{Id: int32(types.T_varchar), Width: 64}},
		{Name: "maxlen", Typ: pb.Type{Id: int32(types.T_int32)}},
	}}
	for _, sql := range []string{"show charset", "show character set", "show charset like 'utf8%'", "show character set where Charset = 'utf8mb4'"} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := BuildPlan(ctx, stmt, false)
		stmt.Free()
		require.NoError(t, err)
		require.Equal(t, []string{"Charset", "Description", "Default collation", "Maxlen"}, p.GetQuery().Headings)
		found := false
		for _, node := range p.GetQuery().Nodes {
			if node.TableDef != nil && node.TableDef.Name == "character_sets" {
				found = true
			}
		}
		require.True(t, found, "SHOW must query the actual supported charset catalog, not an empty stub")
	}
}

func TestCollationMetadataPlannerAdmission(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	col := FindColumn(ctx.tables["nation"].Cols, "n_name")
	require.NotNil(t, col)
	col.Typ.Charset, col.Typ.CollationVersion = 4, 1
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select n_name from nation", 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, "disabled")
}

func TestCollationMetadataCloneAndCacheIdentity(t *testing.T) {
	typ := pb.Type{Id: int32(types.T_varchar), Charset: 4, CollationVersion: 1,
		CollationCoercibilitySet: true, CollationCoercibility: 0, CollationMergeConflict: true,
		XXX_unrecognized: []byte{0x98, 0x06, 0x07}}
	require.Equal(t, &typ, DeepCopyType(&typ))
	table := &pb.TableDef{DefaultCharset: 4, CollationVersion: 1, KeyFormat: 1, XXX_unrecognized: []byte{0x98, 0x06, 0x07},
		Cols: []*pb.ColDef{{Name: "v", Typ: typ}}, Indexes: []*pb.IndexDef{{IndexName: "idx", KeyFormat: 1, XXX_unrecognized: []byte{0x98, 0x06, 0x07}}}}
	_, _, err := ConstructCreateTableSQL(nil, table, nil, false, nil)
	require.ErrorContains(t, err, "disabled")
	cloned := DeepCopyTableDef(table, true)
	require.True(t, proto.Equal(table, cloned))
	require.NotSame(t, table, cloned)
	require.Equal(t, table, CloneTableDefForPlan(table, true))
	require.Equal(t, table.Indexes[0], DeepCopyIndexDef(table.Indexes[0]))
	cloned.Cols[0].Typ.XXX_unrecognized[2] = 8
	require.Equal(t, byte(7), table.Cols[0].Typ.XXX_unrecognized[2])
	expr := &pb.Expr{Typ: typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 1, ColPos: 0, Name: "v"}}}
	require.True(t, exprStructuralEqual(expr, DeepCopyExpr(expr)))
	require.Equal(t, exprStructuralHash(expr), exprStructuralHash(DeepCopyExpr(expr)))
	for _, mutate := range []func(*pb.Type){
		func(t *pb.Type) { t.Charset = 3 }, func(t *pb.Type) { t.CollationVersion = 0 },
		func(t *pb.Type) { t.CollationCoercibilitySet = false },
		func(t *pb.Type) { t.CollationCoercibility = 2 }, func(t *pb.Type) { t.CollationMergeConflict = false },
	} {
		other := DeepCopyExpr(expr)
		mutate(&other.Typ)
		require.False(t, exprStructuralEqual(expr, other))
		require.NotEqual(t, exprStructuralHash(expr), exprStructuralHash(other))
	}
	// Ordinary Type conversion carries runtime semantics but not expression provenance.
	runtime := makeTypeByPlan2Type(typ)
	require.Equal(t, uint8(4), runtime.Charset)
	require.Equal(t, uint8(1), runtime.CollationVersion)
	require.Equal(t, typ.CollationVersion, makePlan2Type(&runtime).CollationVersion)
}
