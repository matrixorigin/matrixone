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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type preparedAnalyzeCompilerContext struct {
	CompilerContext
	defaultDatabase string
	snapshot        *Snapshot
	resolve         func(string, string, *Snapshot) (*ObjectRef, *TableDef, error)
}

func (c *preparedAnalyzeCompilerContext) DefaultDatabase() string {
	return c.defaultDatabase
}

func (c *preparedAnalyzeCompilerContext) Resolve(
	databaseName, tableName string,
	snapshot *Snapshot,
) (*ObjectRef, *TableDef, error) {
	if c.resolve != nil {
		return c.resolve(databaseName, tableName, snapshot)
	}
	return c.CompilerContext.Resolve(databaseName, tableName, snapshot)
}

func (c *preparedAnalyzeCompilerContext) ResolveSnapshotWithSnapshotName(string) (*Snapshot, error) {
	return c.snapshot, nil
}

func TestBuildPreparedAnalyze(t *testing.T) {
	t.Run("frontend carrier and deduplicated dependencies", func(t *testing.T) {
		prepared, err := buildPrepare(tree.NewPrepareString("analyze_stmt",
			"analyze table nation(n_nationkey), tpch.nation(n_name), nation"),
			NewMockCompilerContext(true))
		require.NoError(t, err)
		prepare := prepared.GetDcl().GetPrepare()
		require.NotNil(t, prepare)
		require.NotNil(t, prepare.Plan)
		require.True(t, prepare.Plan.IsPrepare)
		require.Nil(t, prepare.Plan.Plan)
		require.Empty(t, prepare.ParamTypes)
		require.Len(t, prepare.Schemas, 1)
		require.Equal(t, "tpch", prepare.Schemas[0].SchemaName)
		require.Equal(t, "nation", prepare.Schemas[0].ObjName)
	})

	t.Run("prepare time default database", func(t *testing.T) {
		base := NewMockCompilerContext(true)
		var resolvedDatabase string
		ctx := &preparedAnalyzeCompilerContext{
			CompilerContext: base,
			defaultDatabase: "bvt_test1",
			resolve: func(databaseName, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
				resolvedDatabase = databaseName
				return base.Resolve(databaseName, tableName, snapshot)
			},
		}
		prepared, err := buildPrepare(
			tree.NewPrepareString("analyze_stmt", "analyze table t1(a)"), ctx)
		require.NoError(t, err)
		require.Equal(t, "bvt_test1", resolvedDatabase)
		require.Equal(t, "bvt_test1", prepared.GetDcl().GetPrepare().Schemas[0].SchemaName)
	})

	t.Run("snapshot dependency", func(t *testing.T) {
		base := NewMockCompilerContext(true)
		snapshot := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}}
		ctx := &preparedAnalyzeCompilerContext{
			CompilerContext: base,
			defaultDatabase: "tpch",
			snapshot:        snapshot,
			resolve: func(databaseName, tableName string, actual *Snapshot) (*ObjectRef, *TableDef, error) {
				require.Equal(t, snapshot, actual)
				return base.Resolve(databaseName, tableName, actual)
			},
		}
		prepared, err := buildPrepare(tree.NewPrepareString("analyze_stmt",
			"analyze table nation {snapshot = 'prepared_analyze_snapshot'}"), ctx)
		require.NoError(t, err)
		require.Equal(t, snapshot, prepared.GetDcl().GetPrepare().Schemas[0].Snapshot)
	})
}

func TestBuildPreparedAnalyzeRejectsInvalidDependencies(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		sql   string
		ctx   CompilerContext
		match string
	}{
		{
			name: "no database", sql: "analyze table nation",
			ctx: &preparedAnalyzeCompilerContext{
				CompilerContext: NewMockCompilerContext(true), defaultDatabase: "",
			},
			match: "No database selected",
		},
		{
			name: "missing table", sql: "analyze table missing_table",
			ctx: NewMockCompilerContext(true), match: "no such table tpch.missing_table",
		},
		{
			name: "missing column", sql: "analyze table nation(missing_column)",
			ctx: NewMockCompilerContext(true), match: "invalid input: column missing_column does not exist",
		},
		{
			name: "hidden column", sql: "analyze table nation(__mo_rowid)",
			ctx: NewMockCompilerContext(true), match: "invalid input: column __mo_rowid does not exist",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := buildPrepare(tree.NewPrepareString("analyze_stmt", testCase.sql), testCase.ctx)
			require.ErrorContains(t, err, testCase.match)
		})
	}

	t.Run("implicit list requires a visible column", func(t *testing.T) {
		base := NewMockCompilerContext(true)
		ctx := &preparedAnalyzeCompilerContext{
			CompilerContext: base,
			defaultDatabase: "tpch",
			resolve: func(databaseName, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
				objRef, tableDef, err := base.Resolve(databaseName, tableName, snapshot)
				for _, col := range tableDef.Cols {
					col.Hidden = true
				}
				return objRef, tableDef, err
			},
		}
		_, err := buildPrepare(tree.NewPrepareString("analyze_stmt", "analyze table nation"), ctx)
		require.ErrorContains(t, err, "ANALYZE TABLE: no visible columns found for table nation")
	})
}
