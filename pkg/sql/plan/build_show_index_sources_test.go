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

package plan

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var errUserIndexSource = errors.New("user index source denied")

type showIndexGuardContext struct {
	*iscpLogCompilerContext
	generated []string
}

func (c *showIndexGuardContext) Resolve(db, name string, snapshot *Snapshot, borrowOutput *TableResolution) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	if name == "renamed_child" && db == "otherdb" {
		def := DeepCopyTableDef(c.tables["nation"], true)
		def.Name, def.DbName = name, db
		return &ObjectRef{SchemaName: db, ObjName: name, Obj: 9900}, def, nil
	}
	if name == "renamed_child" || strings.HasPrefix(name, "__mo_index_") {
		return nil, nil, errUserIndexSource
	}
	return c.MockCompilerContext.Resolve(db, name, snapshot, nil)
}

func (c *showIndexGuardContext) ResolveIndexTableByRef(parent *ObjectRef, name string, snapshot *Snapshot, borrowOutput *TableResolution) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	c.generated = append(c.generated, name)
	return c.MockCompilerContext.ResolveIndexTableByRef(parent, name, snapshot, nil)
}

func TestShowIndexGeneratedSourceIdentity(t *testing.T) {
	base := newShowIndexStatusTestContext(t)
	base.dbs["otherdb"] = true
	oldName := "__mo_index_i_" + catalog.IndexIgnored_TblType
	base.tables["renamed_child"] = base.tables[oldName]
	base.tables["renamed_child"].Name = "renamed_child"
	base.objects["renamed_child"] = &ObjectRef{SchemaName: "tpch", ObjName: "renamed_child", Obj: 9010}
	for _, idx := range base.tables["vt"].Indexes {
		if idx.IndexTableName == oldName {
			idx.IndexTableName = "renamed_child"
		}
	}
	ctx := &showIndexGuardContext{iscpLogCompilerContext: base}
	previous := ctx.GetContext()
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		return BuildPlan(ctx, stmt, false, nil)
	}
	for _, sql := range []string{
		"show index ignored from tpch.vt",
		"show index status from tpch.vt",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx.generated = nil
			p, err := build(sql)
			require.NoError(t, err)
			require.Same(t, previous, ctx.GetContext())
			require.Contains(t, ctx.generated, "renamed_child")
			require.Contains(t, ctx.generated, "__mo_index_j_ignored")
			for _, node := range p.GetQuery().GetNodes() {
				if node.NodeType == planpb.Node_TABLE_SCAN && node.ObjRef != nil {
					require.Equal(t, "vt", node.GetParentObjRef().GetObjName())
				}
			}
		})
	}

	// A user subquery spelling exactly the generated physical relation is
	// still routed to Resolve, and the scoped map is removed after the error.
	for _, verb := range []string{"ignored", "status"} {
		_, err := build("show index " + verb + " from tpch.vt where exists (select 1 from tpch.renamed_child)")
		require.ErrorIs(t, err, errUserIndexSource)
		require.Same(t, previous, ctx.GetContext())
	}
	_, err := build("show index ignored from tpch.renamed_child")
	require.ErrorIs(t, err, errUserIndexSource)

	// The same unprefixed name in another database is an ordinary user scan.
	// Name-only post-processing would incorrectly assign it vt as a parent.
	p, err := build("show index ignored from tpch.vt where exists (select 1 from otherdb.renamed_child where n_nationkey = 1)")
	require.NoError(t, err)
	found := false
	for _, node := range p.GetQuery().GetNodes() {
		if node.NodeType == planpb.Node_TABLE_SCAN && node.GetObjRef().GetSchemaName() == "otherdb" {
			found = true
			require.Nil(t, node.ParentObjRef)
		}
	}
	require.True(t, found, "the user WHERE scan must remain in the plan")

	// A rebuilt SHOW uses fresh parsed identities rather than a stale map.
	_, err = build("show index ignored from tpch.vt")
	require.NoError(t, err)
	require.Same(t, previous, ctx.GetContext())
}

func TestShowIndexGeneratedSourcesFailClosed(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select * from (select 1 from tpch.actual) t", 1)
	require.NoError(t, err)
	defer stmt.Free()
	parent := &ObjectRef{SchemaName: "tpch", ObjName: "base", Obj: 42}
	_, err = collectShowIndexGeneratedSources(t.Context(), stmt.(*tree.Select), "tpch", map[string]struct{}{"different": {}}, parent)
	require.ErrorContains(t, err, "unexpected SHOW INDEX metadata relation")
	_, err = collectShowIndexGeneratedSources(t.Context(), stmt.(*tree.Select), "tpch", map[string]struct{}{"actual": {}, "missing": {}}, parent)
	require.ErrorContains(t, err, "missing SHOW INDEX metadata relation")
	_, err = collectShowIndexGeneratedSources(t.Context(), stmt.(*tree.Select), "tpch", map[string]struct{}{"actual": {}}, nil)
	require.ErrorContains(t, err, "missing base table")
	join, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select * from tpch.actual join tpch.other on true", 1)
	require.NoError(t, err)
	defer join.Free()
	_, err = collectShowIndexGeneratedSources(t.Context(), join.(*tree.Select), "tpch", map[string]struct{}{"actual": {}, "other": {}}, parent)
	require.ErrorContains(t, err, "unexpected SHOW INDEX metadata join")
}
