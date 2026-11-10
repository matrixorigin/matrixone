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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// CREATE EXTERNAL TABLE shares InitNullMap with LOAD, and InitNullMap keeps
// row-variable wrapper assignments attached to the statement so LOAD replans
// can rebind them. The property published into the catalog must satisfy the
// opposite requirement: UpdateExpr.Expr is a tree.Expr interface, which
// encoding/json cannot reconstruct, and both SHOW CREATE TABLE and every
// external scan decode this property before serving the table (build_show_util
// and the execution-side getExternParam). The published payload must therefore
// keep the reader options and the NULLIF sentinel map while never carrying an
// expression interface. The statement itself keeps its parsed expressions: the
// strip happens on a serialization copy, so the plan builder stays reusable.
func TestCreateExternalTableParamMetadataRoundTrips(t *testing.T) {
	buildProperty := func(t *testing.T, sql string) (*tree.CreateTable, *tree.ExternParam) {
		ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(func() { stmt.Free() })
		create, ok := stmt.(*tree.CreateTable)
		require.True(t, ok)
		require.NotNil(t, create.Param)

		pn, err := BuildPlan(ctx, create, false, nil)
		require.NoError(t, err, "the SET form is accepted by CREATE EXTERNAL TABLE")

		var property string
		for _, def := range pn.GetDdl().GetCreateTable().GetTableDef().GetDefs() {
			for _, p := range def.GetProperties().GetProperties() {
				if p.Key == catalog.SystemRelAttr_CreateSQL {
					require.Empty(t, property, "expected one create-sql property")
					property = p.Value
				}
			}
		}
		require.NotEmpty(t, property, "the external-table property must be published")

		published := &tree.ExternParam{}
		require.NoError(t, json.Unmarshal([]byte(property), published),
			"the published external-table metadata must round-trip")
		return create, published
	}

	t.Run("row variable form keeps published metadata decodable", func(t *testing.T) {
		// Base-accepted form: a row-variable NULLIF passes InitNullMap on
		// CREATE EXTERNAL TABLE. It binds nothing at scan time (an external
		// table has no load column list), so only the decodability of the
		// definition is observable again on SHOW CREATE TABLE and every
		// scan. Publication must strip the expression interface on a copy.
		const sql = `CREATE EXTERNAL TABLE e31232_meta (s VARCHAR(32)) ` +
			`INFILE {'filepath'='input.csv','format'='csv'} FIELDS TERMINATED BY ',' ` +
			`SET s=NULLIF(@v,'sentinel')`
		create, published := buildProperty(t, sql)

		require.NotNil(t, create.Param.Tail.Assignments[0].Expr,
			"publication strips on a copy; the statement keeps its expressions")
		require.NotNil(t, published.Tail)
		require.NotEmpty(t, published.Tail.Assignments,
			"the assignment entry persists as inert metadata exactly as before")
		for _, assignment := range published.Tail.Assignments {
			if assignment == nil {
				continue
			}
			require.Nil(t, assignment.Expr,
				"a published metadata payload must not carry a tree.Expr interface")
		}
		require.Empty(t, published.NullMap,
			"a row-variable form contributes no column sentinel rules")
		require.NoError(t, InitInfileParam(published),
			"SHOW CREATE and scans re-derive file/format from the published options")
		require.Equal(t, "csv", published.Format)
	})

	t.Run("column NULLIF metadata keeps its sentinel map and options", func(t *testing.T) {
		const sql = `CREATE EXTERNAL TABLE e31232_col (flag INT) ` +
			`INFILE {'filepath'='input.csv','format'='csv'} FIELDS TERMINATED BY ',' ` +
			`SET flag=NULLIF(flag,0)`
		create, published := buildProperty(t, sql)

		require.NotNil(t, create.Param.Tail.Assignments[0])
		require.Nil(t, create.Param.Tail.Assignments[0].Expr,
			"the column-NULLIF form stays consumed into the sentinel map on the statement")
		require.Equal(t, map[string][]string{"flag": {"0"}}, published.NullMap,
			"the scan-side sentinel map must survive publication")
		require.NotNil(t, published.Tail)
		require.NotNil(t, published.Tail.Fields)
		require.Equal(t, ",", published.Tail.Fields.Terminated.Value,
			"the reader-visible FIELDS option must survive publication")
		require.NotEmpty(t, published.Tail.Assignments)
		for _, assignment := range published.Tail.Assignments {
			if assignment == nil {
				continue
			}
			require.Nil(t, assignment.Expr)
		}
		require.NoError(t, InitInfileParam(published),
			"SHOW CREATE and scans re-derive file/format from the published options")
		require.Equal(t, "csv", published.Format,
			"the published options must re-derive the format the table was created with")
	})
}
