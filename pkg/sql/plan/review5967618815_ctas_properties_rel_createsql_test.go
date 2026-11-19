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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// rel_createsql is persisted to the catalog and reparsed when the DDL has to
// be rebuilt from the stored statement, so every table option must reserialize
// as SQL the parser accepts. Review 5967618815 on PR 32906: the shared
// PROPERTIES option serialized bare key/value tokens, while the PROPERTIES
// grammar (property_elem: STRING '=' STRING) only accepts string literals, so
// the planner-produced rel_createsql could not be parsed back.
func TestCTASPropertiesRelCreateSQLReparses(t *testing.T) {
	sql := "CREATE TABLE cq PROPERTIES ('k'='va l''ue') AS SELECT 1 AS a"
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	create, ok := stmt.(*tree.CreateTable)
	require.True(t, ok, sql)

	// The multi-statement request fallback serializes this statement's
	// canonical AST, which is the exact rel_createsql owner consumed by the
	// catalog. The mock compiler context keeps GetRootSql empty so the
	// canonical AST serialization path runs.
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	createSQL := createTableSQLForCatalog(ctx, create)
	require.NotEmpty(t, createSQL)
	require.Contains(t, createSQL, "properties", createSQL)
	require.Contains(t, createSQL, "'k' = 'va l''ue'", createSQL)

	reStmt, reErr := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
	require.NoError(t, reErr, createSQL)

	reCreate, ok := reStmt.(*tree.CreateTable)
	require.True(t, ok, createSQL)
	var got [][2]string
	for _, opt := range reCreate.Options {
		if props, ok := opt.(*tree.TableOptionProperties); ok {
			for _, p := range props.Preperties {
				got = append(got, [2]string{p.Key, p.Value})
			}
		}
	}
	// The reparsed catalog statement preserves the exact key/value pair,
	// including the escaped inner quote, and re-serializes identically
	// through the same canonicalization used for rel_createsql.
	require.Equal(t, [][2]string{{"k", "va l'ue"}}, got, createSQL)
	require.Equal(t, createSQL, canonicalCreateTableSQL(reCreate), createSQL)
}

// CTAS handling keeps working when the planner is asked to bind the
// statement itself; PROPERTIES rides the tree like every other option.
func TestCTASPropertiesPlanBuildStillSucceeds(t *testing.T) {
	sql := "CREATE TABLE cq2 PROPERTIES ('k'='v') AS SELECT 1 AS a"
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	p, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false, nil)
	require.NoError(t, err, sql)
	ddl := p.GetDdl()
	require.NotNil(t, ddl, sql)
	require.NotNil(t, ddl.GetCreateTable(), sql)
}
