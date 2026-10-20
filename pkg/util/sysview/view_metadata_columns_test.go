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

func TestViewMetadataColumnsKnownTemplatesPreserveRelationalContract(t *testing.T) {
	for name, definition := range map[string]string{
		"current":     InformationSchemaColumnsDDL,
		"v41":         InformationSchemaColumnsV41DDL,
		"v46":         InformationSchemaColumnsV46DDL,
		"v46 upgrade": InformationSchemaColumnsV46UpgradeDDL,
		"v58":         InformationSchemaColumnsV58DDL(),
		"v100":        InformationSchemaColumnsV100DDL(),
		"v109":        InformationSchemaColumnsV109DDL(),
	} {
		t.Run(name, func(t *testing.T) {
			for _, input := range []string{definition, strings.Replace(definition, "information_schema.COLUMNS", "information_schema.columns", 1)} {
				adapted, ok := ViewMetadataColumnsDDL(input)
				require.True(t, ok)
				require.Contains(t, adapted, "mo_view_columns(mt.reldatabase, mt.relname, mt.rel_id)")
				require.NotContains(t, adapted, "mo_view_columns(mt.rel_id)")
				// Restore just the argument representation. Everything else,
				// including authorization, projections, UNION and predicates,
				// must be identical to the existing derived template.
				restored := strings.NewReplacer(
					"mo_view_columns(mt.reldatabase, mt.relname, mt.rel_id)", "mo_view_columns(mt.rel_id)",
					"mo_subscription_view_columns(mt.reldatabase, mt.relname, mt.rel_id)", "mo_subscription_view_columns(mt.publisher_account_id, mt.rel_id)",
				).Replace(adapted)
				baseline := definition
				if name != "current" && name != "v100" && name != "v109" {
					baseline, ok = AdaptLegacyInformationSchemaColumnsDDL(definition)
					require.True(t, ok)
					readapted, supported := ViewMetadataColumnsDDL(baseline)
					require.True(t, supported)
					require.Equal(t, adapted, readapted)
				}
				require.Equal(t, baseline, restored)
			}
		})
	}
}

func TestViewMetadataColumnsRejectsUnknownDefinitions(t *testing.T) {
	for _, definition := range []string{"", "CREATE VIEW information_schema.COLUMNS AS SELECT 1", InformationSchemaColumnsDDL + " where true"} {
		adapted, ok := ViewMetadataColumnsDDL(definition)
		require.False(t, ok)
		require.Empty(t, adapted)
	}
}
