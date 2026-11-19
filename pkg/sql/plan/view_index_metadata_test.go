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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestProtectHistoricalTableListing(t *testing.T) {
	ctx := context.Background()
	snapshot := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}}
	for _, tc := range []struct {
		name, sql string
		wantErr   bool
	}{
		{"released source", "create view information_schema.tables as select tbl.relname from mo_catalog.mo_tables tbl where tbl.account_id = current_account_id()", false},
		{"unknown source", "create view information_schema.tables as select relname from mo_catalog.mo_database", true},
		{"join", "create view information_schema.tables as select a.relname from mo_catalog.mo_tables a join mo_catalog.mo_tables b on a.rel_id=b.rel_id", true},
		{"cte", "create view information_schema.tables as with t as (select * from mo_catalog.mo_tables) select * from t", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(ctx, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			view := stmt.(*tree.CreateView)
			err = protectHistoricalTableListing(ctx, "information_schema", "tables", tc.sql, snapshot, view.AsSource)
			if tc.wantErr {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
				return
			}
			require.NoError(t, err)
			formatted := tree.String(view.AsSource, dialect.MYSQL)
			require.Contains(t, formatted, "mo_is_physical_index")
			require.Contains(t, formatted, "tbl.relname")
			require.Contains(t, formatted, "tbl.relkind")
			require.Contains(t, formatted, "tbl.extra_info")
			require.Contains(t, formatted, "current_account_id")
		})
	}
	// Already-filtered current views and non-snapshot queries need no rewrite.
	require.NoError(t, protectHistoricalTableListing(ctx, "information_schema", "tables", sysview.InformationSchemaTablesCurrentDDL, snapshot, nil))
	require.NoError(t, protectHistoricalTableListing(ctx, "information_schema", "tables", "unknown", nil, nil))
	require.NoError(t, protectHistoricalTableListing(ctx, "other", "tables", "unknown", snapshot, nil))
}
