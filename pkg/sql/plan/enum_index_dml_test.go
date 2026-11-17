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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestEnumIndexRawProjectionKeepsOrderSlotsSeparate(t *testing.T) {
	spec := enumIndexTestSpec()
	table := spec.Source
	table.TblId, table.DbName = 10, "db"
	table.Indexes = []*planpb.IndexDef{spec.Index}
	obj := &planpb.ObjectRef{Obj: 10, SchemaName: "db"}
	dml := &DMLContext{tableDefs: []*planpb.TableDef{table}, objRefs: []*planpb.ObjectRef{obj}, aliases: []string{"t"}}
	ctx := NewBindContext(nil, nil)
	raw, err := prepareEnumIndexRawProjections(ctx, dml, false)
	require.NoError(t, err)

	cols := []string{"id", "e", "other"}
	types := []*planpb.Type{&table.Cols[0].Typ, &table.Cols[1].Typ, &table.Cols[2].Typ}
	ctx.bindingByTable["t"] = NewBinding(37, 0, "db", "t", 10, cols, nil, types, false, nil)
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{{
		NodeType: planpb.Node_TABLE_SCAN, TableDef: table, ObjRef: obj,
	}}}}
	// One visible projection and a separate ORDER BY projection already exist.
	ctx.projects = []*planpb.Expr{GetColExpr(table.Cols[0].Typ, 37, 0), GetColExpr(table.Cols[0].Typ, 37, 0)}
	positions, err := bindEnumIndexRawProjections(builder, ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []int32{2, 3}, positions)
	require.Equal(t, int32(1), raw["t.e"].resultPos)
	require.Equal(t, int32(2), raw["t.other"].resultPos)
	require.Equal(t, int32(1), ctx.projects[2].GetCol().ColPos)
	pos, err := enumIndexOldValuePos(table, true, raw, "t", "e", 7)
	require.NoError(t, err)
	require.Equal(t, int32(1), pos)
	pos, err = enumIndexOldValuePos(table, false, raw, "t", "e", 7)
	require.NoError(t, err)
	require.Equal(t, int32(7), pos)
	pos, err = enumIndexOldValuePos(table, true, raw, "t", "id", 7)
	require.NoError(t, err)
	require.Equal(t, int32(7), pos)

	// A stale relation generation or a changed column domain must not be
	// accepted as the physical source, even if the alias still matches.
	ctx.bindingByTable["t"].tableID = 11
	_, err = bindEnumIndexRawProjections(builder, ctx, 1)
	require.Error(t, err)
	ctx.bindingByTable["t"].tableID = 10
	table.Cols[1].Typ.EnumCollation = 0
	_, err = bindEnumIndexRawProjections(builder, ctx, 1)
	require.Error(t, err)
}
