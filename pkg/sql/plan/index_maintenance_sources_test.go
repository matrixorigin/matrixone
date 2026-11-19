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
	"errors"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type maintenanceSourceTestContext struct {
	CompilerContext
	ctx context.Context
	id  uint64
}

func (c *maintenanceSourceTestContext) GetContext() context.Context { return c.ctx }
func (c *maintenanceSourceTestContext) Resolve(_ string, _ string, _ *Snapshot, borrowOutput *TableResolution) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	return nil, nil, errors.New("ordinary resolution denied")
}
func (c *maintenanceSourceTestContext) ResolveIndexTableByRef(_ *ObjectRef, name string, _ *Snapshot, borrowOutput *TableResolution) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	return &ObjectRef{Obj: int64(c.id), ObjName: name}, &TableDef{TblId: c.id, Name: name}, nil
}

func TestIndexMaintenanceSourceMarksAreExactASTNodes(t *testing.T) {
	spec := &executor.IndexMaintenanceSpec{Function: "build", Database: "db", DatabaseID: 1, BaseName: "base", BaseID: 2,
		Targets: []executor.IndexMaintenanceTarget{{Role: "store", Name: "hidden", ID: 3}, {Role: "meta", Name: "meta", ID: 4}}}
	for _, tc := range []struct {
		name, sql string
		count     int
	}{
		{"direct", "select * from db.hidden", 1},
		{"where subquery is not marked", "select * from db.hidden where exists (select 1 from db.hidden)", 1},
		{"insert source is not marked", "insert into db.hidden select * from db.hidden", 1},
		{"replace source is not marked", "replace into db.hidden select * from db.hidden", 1},
		{"different database", "select * from other.hidden", 0},
		{"scalar generated maxima", "select greatest(coalesce((select max(chunk_id) from db.hidden where tag=1),0),coalesce((select max(recency) from db.meta),0))+1", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			marked, err := WithIndexMaintenanceSources(context.Background(), stmt, spec)
			require.NoError(t, err)
			sources, ok := marked.Value(indexMaintenanceSourcesKey{}).(map[*tree.TableName]indexMaintenanceSource)
			require.True(t, ok)
			require.Len(t, sources, tc.count)
		})
	}
}

func TestIndexMaintenanceSourceIdentityAndReplacement(t *testing.T) {
	spec := &executor.IndexMaintenanceSpec{Function: "build", Database: "db", DatabaseID: 1, BaseName: "base", BaseID: 2,
		Targets: []executor.IndexMaintenanceTarget{{Role: "store", Name: "hidden", ID: 3}}}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from db.hidden", 1)
	require.NoError(t, err)
	defer stmt.Free()
	marked, err := WithIndexMaintenanceSources(context.Background(), stmt, spec)
	require.NoError(t, err)
	sources := marked.Value(indexMaintenanceSourcesKey{}).(map[*tree.TableName]indexMaintenanceSource)
	var generated *tree.TableName
	for generated = range sources {
	}
	require.NotNil(t, generated)
	compiler := &maintenanceSourceTestContext{ctx: marked, id: 3}
	_, _, err = resolveIndexMaintenanceSource(compiler, generated, "db", "hidden", nil, nil)
	require.NoError(t, err)
	other, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from db.hidden", 1)
	require.NoError(t, err)
	defer other.Free()
	otherMarked, err := WithIndexMaintenanceSources(context.Background(), other, spec)
	require.NoError(t, err)
	var userNode *tree.TableName
	for userNode = range otherMarked.Value(indexMaintenanceSourcesKey{}).(map[*tree.TableName]indexMaintenanceSource) {
	}
	_, _, err = resolveIndexMaintenanceSource(compiler, userNode, "db", "hidden", nil, nil)
	require.ErrorContains(t, err, "ordinary resolution denied")
	compiler.id = 4
	_, _, err = resolveIndexMaintenanceSource(compiler, generated, "db", "hidden", nil, nil)
	require.ErrorContains(t, err, "identity changed")
}

type maintenanceDeleteTestContext struct {
	*MockCompilerContext
	table     *TableDef
	id        uint64
	indexGets int
}

func (c *maintenanceDeleteTestContext) Resolve(_ string, _ string, _ *Snapshot, borrowOutput *TableResolution) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	return nil, nil, errors.New("ordinary resolution denied")
}

func (c *maintenanceDeleteTestContext) ResolveIndexTableByRef(_ *ObjectRef, name string, _ *Snapshot, borrowOutput *TableResolution) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	c.indexGets++
	def := DeepCopyTableDef(c.table, true)
	def.Name, def.DbName, def.TblId = name, "db", c.id
	return &ObjectRef{SchemaName: "db", ObjName: name, Obj: int64(c.id)}, def, nil
}

// Exercise the modern DELETE binder, including the SELECT it constructs over
// the target. Only the original, marked target node receives the capability.
func TestIndexMaintenanceModernDeleteSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mock := NewMockCompilerContext(true, proc)
	mock.dbs["db"] = true
	_, table, err := mock.Resolve("tpch", "nation", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, table)
	compiler := &maintenanceDeleteTestContext{MockCompilerContext: mock, table: table, id: 3}
	spec := &executor.IndexMaintenanceSpec{Function: "build", Database: "db", DatabaseID: 1, BaseName: "base", BaseID: 2,
		Targets: []executor.IndexMaintenanceTarget{{Role: "store", Name: "hidden", ID: 3}}}
	bind := func(sql string, mark bool) error {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		ctx := context.Background()
		if mark {
			ctx, err = WithIndexMaintenanceSources(ctx, stmt, spec)
			require.NoError(t, err)
		}
		compiler.SetContext(ctx)
		builder := NewQueryBuilder(planpb.Query_DELETE, compiler, false, true)
		_, err = builder.bindDelete(compiler, stmt.(*tree.Delete), NewBindContext(builder, nil))
		return err
	}
	const ordinaryDelete = "delete from db.hidden where n_regionkey = 0"
	require.NoError(t, bind(ordinaryDelete, true))
	require.GreaterOrEqual(t, compiler.indexGets, 2, "target and generated SELECT must both use the exact marked node")
	require.ErrorContains(t, bind(ordinaryDelete, false), "ordinary resolution denied")
	require.ErrorContains(t, bind("delete from db.hidden where n_regionkey in (select n_regionkey from db.hidden)", true), "ordinary resolution denied")
	compiler.id = 4
	require.ErrorContains(t, bind(ordinaryDelete, true), "identity changed")
}

// REPLACE is used to persist HNSW rows that could not be indexed. Its target
// alone is generated authority; a SELECT inside the same statement is not.
func TestIndexMaintenanceModernReplaceSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mock := NewMockCompilerContext(true, proc)
	mock.dbs["db"] = true
	_, table, err := mock.Resolve("tpch", "nation", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, table)
	compiler := &maintenanceDeleteTestContext{MockCompilerContext: mock, table: table, id: 3}
	spec := &executor.IndexMaintenanceSpec{Function: "build", Database: "db", DatabaseID: 1, BaseName: "base", BaseID: 2,
		Targets: []executor.IndexMaintenanceTarget{{Role: "ignored", Name: "hidden", ID: 3}}}
	bind := func(sql string, mark bool) error {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		ctx := context.Background()
		if mark {
			ctx, err = WithIndexMaintenanceSources(ctx, stmt, spec)
			require.NoError(t, err)
		}
		compiler.SetContext(ctx)
		builder := NewQueryBuilder(planpb.Query_INSERT, compiler, false, true)
		_, err = builder.bindReplace(stmt.(*tree.Replace), NewBindContext(builder, nil))
		return err
	}
	const replaceValues = "replace into db.hidden values (1, 'n', 2, 'c')"
	require.NoError(t, bind(replaceValues, true))
	require.Positive(t, compiler.indexGets, "the generated target must use its exact marked node")
	require.ErrorContains(t, bind(replaceValues, false), "ordinary resolution denied")
	require.ErrorContains(t, bind("replace into db.hidden select n_nationkey,n_name,n_regionkey,n_comment from db.hidden", true), "ordinary resolution denied")
	compiler.id = 4
	require.ErrorContains(t, bind(replaceValues, true), "identity changed")
}
