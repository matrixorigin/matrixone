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
			lowerName := strings.Replace(definition, "information_schema.COLUMNS", "information_schema.columns", 1)
			for _, input := range []string{
				definition, lowerName,
				strings.Replace(definition, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
				strings.Replace(lowerName, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
			} {
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

// The derived COLUMNS template must guarantee that user View rows can never
// come from the persisted mo_catalog.mo_columns snapshot: the persisted branch
// excludes them explicitly, on-demand branches re-add them through description,
// and subscription candidates are authorized and filtered left of the APPLY.
// System-database Views stay on the persisted path (fixed physical columns).
func TestDerivedColumnsTemplateSeparatesPersistedAndDerivedAuthority(t *testing.T) {
	const systemDatabases = "'mo_catalog','information_schema','mysql','system','system_metrics','mo_task','mo_debug'"
	const userViewPredicate = "mt.relkind = 'v' AND mt.reldatabase NOT IN (" + systemDatabases + ")"
	const subscriptionViewPredicate = "mc.relkind = 'v' AND mc.att_database NOT IN (" + systemDatabases + ")"

	for name, definition := range map[string]string{
		"current": InformationSchemaColumnsDDL,
		"v100":    InformationSchemaColumnsV100DDL(),
		"v109":    InformationSchemaColumnsV109DDL(),
	} {
		t.Run(name, func(t *testing.T) {
			// The persisted local branch keeps ordinary tables and system Views.
			require.Contains(t, definition, strings.TrimSpace(informationSchemaColumnsLocalFromSQL()))
			// User Views are excluded from persisted authorities...
			require.Contains(t, definition, " AND NOT ("+userViewPredicate+")")
			require.Contains(t, definition, " AND NOT ("+subscriptionViewPredicate+")")
			// ...and re-added only through on-demand description.
			require.Contains(t, definition, "from __mo_visible_tables mt cross apply mo_view_columns(mt.rel_id) mc ")
			require.Contains(t, definition, " AND ("+userViewPredicate+")")
			require.Contains(t, definition, "cross apply mo_subscription_view_columns(mt.publisher_account_id, mt.rel_id) mc ")
			// Subscription Views are authorized and restricted left of the APPLY;
			// a post-APPLY WHERE could not stop invisible Views consuming budget.
			cte := strings.Index(definition, "__mo_visible_subscription_views AS (")
			applyAt := strings.Index(definition, "cross apply mo_subscription_view_columns")
			require.Positive(t, cte)
			require.Positive(t, applyAt)
			require.Less(t, cte, applyAt)
			require.Contains(t, definition, "FROM mo_subscription_tables() mt WHERE mt.relkind = 'v' AND (")
		})
	}

	// Historical single-branch templates gain only the local derived branch;
	// they must not invent subscription rows.
	t.Run("legacy single branch", func(t *testing.T) {
		derived := informationSchemaDerivedColumnsDDL(InformationSchemaColumnsV41DDL)
		require.Contains(t, derived, "cross apply mo_view_columns(mt.rel_id) mc ")
		require.Contains(t, derived, " AND NOT ("+userViewPredicate+")")
		require.NotContains(t, derived, "mo_subscription_view_columns")
		require.NotContains(t, derived, "__mo_visible_subscription_views")
	})
}

func TestViewMetadataColumnsRejectsUnknownDefinitions(t *testing.T) {
	for _, definition := range []string{"", "CREATE VIEW information_schema.COLUMNS AS SELECT 1", InformationSchemaColumnsDDL + " where true"} {
		for _, input := range []string{definition, strings.Replace(definition, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1)} {
			adapted, ok := ViewMetadataColumnsDDL(input)
			require.False(t, ok)
			require.Empty(t, adapted)
		}
	}
}
