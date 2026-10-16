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

package frontend

import (
	"context"
	"testing"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

type limitZeroFrontendCatalogContext struct {
	*sqlplan.MockCompilerContext
	tableDef     *sqlplan.TableDef
	fastCalls    int
	resolveCalls int
}

func (c *limitZeroFrontendCatalogContext) BuildTableDefByMoColumns(db, table string) (*sqlplan.TableDef, error) {
	c.fastCalls++
	accountID, err := c.GetAccountId()
	if err != nil {
		return nil, err
	}
	return buildTableDefFromMoColumns(c.GetContext(), uint64(accountID), db, table, nil)
}

func (c *limitZeroFrontendCatalogContext) Resolve(db, table string, _ *sqlplan.Snapshot) (*sqlplan.ObjectRef, *sqlplan.TableDef, error) {
	c.resolveCalls++
	return &sqlplan.ObjectRef{
		DbName:     db,
		SchemaName: db,
		ObjName:    table,
		Db:         10,
		Obj:        100,
	}, c.tableDef, nil
}

func TestLimitZeroFrontendCatalogAdmissionUsesCompleteMetadata(t *testing.T) {
	for _, tc := range []struct {
		name            string
		columnCharset   uint8
		columnRevision  uint8
		defaultCharset  uint32
		defaultRevision uint32
		tableKeyFormat  uint32
		indexKeyFormat  uint32
		wantError       bool
	}{
		{name: "legacy columns", columnCharset: types.CharsetUTF8, defaultCharset: uint32(types.CharsetUTF8)},
		{name: "utf8 UCA400 control", columnCharset: 9, columnRevision: 1, defaultCharset: 9, defaultRevision: 1},
		{name: "utf8mb4 UCA400 control", columnCharset: 10, columnRevision: 1, defaultCharset: 10, defaultRevision: 1},
		{name: "native column control", columnCharset: 4, columnRevision: 1, defaultCharset: uint32(types.CharsetUTF8), wantError: true},
		{name: "table key format 1", columnCharset: types.CharsetUTF8, defaultCharset: uint32(types.CharsetUTF8), tableKeyFormat: 1, wantError: true},
		{name: "disabled table default", columnCharset: types.CharsetUTF8, defaultCharset: 4, defaultRevision: 1, wantError: true},
		{name: "index key format 1", columnCharset: types.CharsetUTF8, defaultCharset: uint32(types.CharsetUTF8), indexKeyFormat: 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestCtx := context.Background()
			background := &backgroundExecTest{}
			background.init()
			stub := gostub.StubFunc(&NewShareTxnBackgroundExec, background)
			t.Cleanup(stub.Reset)

			columnType := types.Type{
				Oid:              types.T_varchar,
				Width:            25,
				Charset:          tc.columnCharset,
				CollationVersion: tc.columnRevision,
			}
			columnTypeBytes, err := columnType.Marshal()
			require.NoError(t, err)
			defaultBytes, err := types.Encode(&planpb.Default{})
			require.NoError(t, err)
			query, err := getTableColumnDefSql(0, "tpch", "nation")
			require.NoError(t, err)
			background.sql2result[query] = newMrsForTableColumnDef([][]interface{}{{
				"n_name",
				string(columnTypeBytes),
				uint64(1),
				int64(0),
				string(defaultBytes),
				int64(0),
				int64(0),
			}})

			// This is the production frontend decoder and its actual mo_columns
			// projection: per-column collation survives, table/index metadata does not.
			decoded, err := buildTableDefFromMoColumns(requestCtx, 0, "tpch", "nation", nil)
			require.NoError(t, err)
			require.Len(t, decoded.Cols, 1)
			require.Equal(t, uint32(tc.columnCharset), decoded.Cols[0].Typ.Charset)
			require.Equal(t, uint32(tc.columnRevision), decoded.Cols[0].Typ.CollationVersion)
			require.Equal(t, uint64(100), decoded.TblId)
			require.Equal(t, uint64(10), decoded.DbId)
			require.Equal(t, uint32(3), decoded.Version)
			require.Zero(t, decoded.DefaultCharset)
			require.Zero(t, decoded.CollationVersion)
			require.Zero(t, decoded.KeyFormat)
			require.Empty(t, decoded.Indexes)

			base := sqlplan.NewMockCompilerContext(false, testutil.NewProcess(t))
			base.SetContext(requestCtx)
			fullTableDef := &sqlplan.TableDef{
				Name:             "nation",
				DbName:           "tpch",
				TableType:        catalog.SystemOrdinaryRel,
				TblId:            100,
				DbId:             10,
				Version:          3,
				DefaultCharset:   tc.defaultCharset,
				CollationVersion: tc.defaultRevision,
				KeyFormat:        tc.tableKeyFormat,
				Cols: []*sqlplan.ColDef{{
					TblName:    "nation",
					DbName:     "tpch",
					ColId:      1,
					Name:       "n_name",
					OriginName: "n_name",
					Typ: sqlplan.Type{
						Id:               int32(types.T_varchar),
						Width:            25,
						Charset:          uint32(tc.columnCharset),
						CollationVersion: uint32(tc.columnRevision),
					},
				}},
			}
			if tc.indexKeyFormat != 0 {
				fullTableDef.Indexes = []*planpb.IndexDef{{IndexName: "idx_n_name", KeyFormat: tc.indexKeyFormat}}
			}
			compilerContext := &limitZeroFrontendCatalogContext{
				MockCompilerContext: base,
				tableDef:            fullTableDef,
			}
			before, err := fullTableDef.Marshal()
			require.NoError(t, err)
			build := func(sql string) (*sqlplan.Plan, error) {
				stmt, err := parsers.ParseOne(requestCtx, dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				return buildPlan(requestCtx, nil, compilerContext, stmt)
			}

			for _, sql := range []string{"select 1 from nation", "select * from nation limit 0"} {
				p, err := build(sql)
				if tc.wantError {
					require.ErrorContains(t, err, "disabled", "SQL: %s", sql)
					require.Nil(t, p, "rejected catalog metadata must not publish a plan")
				} else {
					require.NoError(t, err, "SQL: %s", sql)
					require.NotNil(t, p)
					if sql == "select * from nation limit 0" {
						columns := sqlplan.GetResultColumnsFromPlan(p)
						require.Len(t, columns, 1)
						require.Equal(t, "n_name", columns[0].Name)
						require.Equal(t, int32(25), columns[0].Typ.Width)
						require.Equal(t, uint32(tc.columnCharset), columns[0].Typ.Charset)
						require.Equal(t, uint32(tc.columnRevision), columns[0].Typ.CollationVersion)
						require.Len(t, p.GetQuery().CatalogDependencies, 1)
						dependency := p.GetQuery().CatalogDependencies[0]
						require.Equal(t, int64(100), dependency.Obj)
						require.Equal(t, int64(3), dependency.Server)
					}
				}
			}
			after, err := fullTableDef.Marshal()
			require.NoError(t, err)
			require.Equal(t, before, after, "planning must not rewrite catalog metadata")

			require.Zero(t, compilerContext.fastCalls, "LIMIT 0 must not admit the partial mo_columns result")
			require.Equal(t, 2, compilerContext.resolveCalls, "normal and LIMIT 0 must share complete Resolve metadata")
			if tc.wantError {
				legacy := sqlplan.DeepCopyTableDef(fullTableDef, true)
				legacy.DefaultCharset, legacy.CollationVersion, legacy.KeyFormat = uint32(types.CharsetUTF8), 0, 0
				legacy.Indexes = nil
				legacy.Cols[0].Typ.Charset, legacy.Cols[0].Typ.CollationVersion = uint32(types.CharsetUTF8), 0
				compilerContext.tableDef = legacy
				p, err := build("select * from nation limit 0")
				require.NoError(t, err, "rejection must not poison compiler-context reuse")
				require.NotNil(t, p)
				require.Zero(t, compilerContext.fastCalls)
				require.Equal(t, 3, compilerContext.resolveCalls)
			}
		})
	}
}
