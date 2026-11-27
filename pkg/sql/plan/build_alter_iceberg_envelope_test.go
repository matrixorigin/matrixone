// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// An external Iceberg envelope renders a valid CREATE on the formatter's early
// success return without producing a separate replay spelling, and
// buildAlterTable does not screen envelope-backed tables the way Mongo, ESQL,
// Datastream, Kafka and read-only plain-file mappings are screened. ALTER ADD on
// such a table selects COPY, so the temporary CREATE must retain the valid
// returned DDL instead of consuming an empty replay output (#33356/#33357).
func TestAlterCopyRetainsIcebergEnvelopeReplay(t *testing.T) {
	originalDDL := "CREATE TABLE t_ice (a INT PRIMARY KEY, b VARCHAR(10))"
	mock := newCheckAlterOptimizer(t, originalDDL)
	source := mock.ctxt.tables["t_ice"]
	source.TableType = catalog.SystemExternalRel
	source.Createsql = originalDDL + " /* MO_ICEBERG: version=1; kind=iceberg_table; catalog=c1; namespace=ns1; table=tbl1; default_ref=main */"
	mock.ctxt.tables["t_ice"] = source

	p, err := runOneStmt(mock, t, "ALTER TABLE t_ice ADD COLUMN marker INT DEFAULT 7 FIRST")
	require.NoError(t, err)
	alter := p.GetDdl().GetAlterTable()
	require.NotNil(t, alter)

	require.NotEmpty(t, alter.CreateTmpTableSql,
		"the COPY plan must retain the rendered CREATE for envelope-backed external tables")
	require.Contains(t, alter.CreateTmpTableSql, "ENGINE = ICEBERG WITH")
	require.Contains(t, alter.CreateTmpTableSql, "\"catalog\" = 'c1'")
	require.Contains(t, alter.CreateTmpTableSql, "marker")

	// The retained CREATE must parse cleanly and keep the table/shape the
	// planner built; it is the exact display spelling the renderer returned.
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, alter.CreateTmpTableSql, 1)
	require.NoError(t, err, alter.CreateTmpTableSql)
	create, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	require.Equal(t, alter.CopyTableDef.Name, string(create.Table.ObjectName))

	displayDDL, _, err := constructCreateTableSQL(
		&mock.ctxt, alter.CopyTableDef, nil, true, nil, true, nil,
		false, "", nil,
	)
	require.NoError(t, err)
	require.Equal(t, displayDDL, alter.CreateTmpTableSql)
}
