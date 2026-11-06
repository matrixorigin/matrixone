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

package sysview

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotColumnsNamespace(t *testing.T) {
	for name, ddl := range map[string]string{
		"current":     InformationSchemaColumnsDDL,
		"legacy":      InformationSchemaColumnsV100DDL(),
		"replacement": strings.Replace(InformationSchemaColumnsDDL, "CREATE VIEW", "CREATE OR REPLACE VIEW", 1),
	} {
		for _, derived := range []bool{false, true} {
			t.Run(name, func(t *testing.T) {
				got, ok := SnapshotColumnsDDL(ddl, 17, derived)
				require.True(t, ok)
				require.Contains(t, got, "WHERE tbl.account_id = 17 AND (current_account_id() = 0 AND current_role_id() = 0)")
				require.Contains(t, got, "where mc.account_id = 17")
				require.Contains(t, got, "mt.account_id = 17")
				require.NotContains(t, got, "__mo_active_roles", "caller role IDs must not authorize target objects")
				require.NotContains(t, got, "mo_role_privs")
				require.Equal(t, 3, strings.Count(got, "current_account_id() = 0 AND current_role_id() = 0"), "local, subscription table and pre-APPLY view admission")
				if derived {
					require.Contains(t, got, "mo_view_columns(mt.reldatabase, mt.relname, mt.rel_id)")
				}
			})
		}
	}
	for _, ddl := range []string{"", "CREATE VIEW information_schema.COLUMNS AS SELECT 1", InformationSchemaColumnsDDL + " "} {
		got, ok := SnapshotColumnsDDL(ddl, 17, false)
		require.False(t, ok)
		require.Empty(t, got)
	}
}
