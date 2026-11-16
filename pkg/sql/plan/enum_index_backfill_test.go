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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func enumIndexTestSpec() *executor.EnumIndexBackfillSpec {
	return &executor.EnumIndexBackfillSpec{
		AccountID: 7, Database: "db", SourceID: 10, TargetID: 20,
		Source: &planpb.TableDef{Name: "src", Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}, Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "e", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "A,a", EnumCollation: 2}},
			{Name: "other", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "x,y"}},
		}},
		Index: &planpb.IndexDef{IndexTableName: "idx", Parts: []string{"e"}, Unique: true},
	}
}

func TestEnumIndexBackfillScopedReferences(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 7)
	spec := enumIndexTestSpec()
	sql := "insert into db.idx select serial_full(e,id), id from db.src where e is not null"
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	marked, err := WithEnumIndexBackfill(ctx, stmt, spec)
	require.NoError(t, err)
	state := marked.Value(enumIndexBackfillKey{}).(*enumIndexBackfillBinding)
	require.Len(t, state.refs, 2)
	var ref *tree.UnresolvedName
	for ref = range state.refs {
		break
	}
	binding := &Binding{db: "db", table: "src", tableID: 10}
	typ := spec.Source.Cols[1].Typ
	ok, err := enumIndexPhysicalRef(marked, ref, binding, &typ, 0)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = enumIndexPhysicalRef(marked, ref, &Binding{db: "db", table: "src", tableID: 11}, &typ, 0)
	require.Error(t, err)
	_, err = enumIndexPhysicalRef(marked, ref, binding, &typ, 1)
	require.Error(t, err)
	wrongType := typ
	wrongType.EnumCollation++
	_, err = enumIndexPhysicalRef(marked, ref, binding, &wrongType, 0)
	require.Error(t, err)
	clean := WithoutEnumIndexBackfill(marked)
	ok, err = enumIndexPhysicalRef(clean, ref, binding, &typ, 0)
	require.NoError(t, err)
	require.False(t, ok)
	var sourceNode *tree.TableName
	for sourceNode = range state.sources {
		break
	}
	source := DeepCopyTableDef(spec.Source, true)
	source.TblId = 10
	sourceObj := &planpb.ObjectRef{Obj: 10}
	require.NoError(t, validateEnumIndexBackfillSource(marked, sourceNode, source, sourceObj))
	source.Cols[1].Typ.EnumCollation++
	require.Error(t, validateEnumIndexBackfillSource(marked, sourceNode, source, sourceObj))
	source.Cols[1].Typ.EnumCollation--
	sourceObj.Obj = 11
	require.Error(t, validateEnumIndexBackfillSource(marked, sourceNode, source, sourceObj))
	require.NoError(t, validateEnumIndexBackfillSource(clean, sourceNode, source, sourceObj))
	def := &planpb.TableDef{TblId: 20, Name: "idx"}
	obj := &planpb.ObjectRef{Obj: 20}
	require.NoError(t, validateEnumIndexBackfillTarget(marked, stmt.(*tree.Insert), def, obj))
	def.TblId = 21
	require.Error(t, validateEnumIndexBackfillTarget(marked, stmt.(*tree.Insert), def, obj))
	require.NoError(t, validateEnumIndexBackfillTarget(clean, stmt.(*tree.Insert), def, obj))
}

func TestEnumIndexBackfillRejectsChangedScope(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 7)
	for _, sql := range []string{
		"insert into db.other select e from db.src",
		"insert into db.idx select e from other.src",
		"insert into db.idx select e from db.src join db.other on 1=1",
		"insert into db.idx select e from db.src where exists (select e from db.src)",
		"insert into db.idx select abs(e) from db.src",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = WithEnumIndexBackfill(ctx, stmt, enumIndexTestSpec())
			require.Error(t, err)
		})
	}
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select e from db.src group by e having count(*) > 1 limit 1", 1)
	require.NoError(t, err)
	defer stmt.Free()
	spec := enumIndexTestSpec()
	spec.DuplicateCheck = true
	_, err = WithEnumIndexBackfill(defines.AttachAccountId(ctx, 8), stmt, spec)
	require.Error(t, err)
	_, err = WithEnumIndexBackfill(ctx, stmt, spec)
	require.NoError(t, err)
}
