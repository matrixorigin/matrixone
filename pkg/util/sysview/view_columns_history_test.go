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

func TestAdaptLegacyInformationSchemaColumnsDDL(t *testing.T) {
	for _, tc := range []struct {
		name       string
		definition string
		withSub    bool
	}{
		{"V41", InformationSchemaColumnsV41DDL, false},
		{"V46", InformationSchemaColumnsV46DDL, true},
		{"V46 upgrade", InformationSchemaColumnsV46UpgradeDDL, true},
		{"V58", InformationSchemaColumnsV58DDL(), true},
		{"V100", InformationSchemaColumnsV100DDL(), true},
		{"V109", InformationSchemaColumnsV109DDL(), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(tc.definition)
			require.True(t, ok)
			require.Contains(t, strings.ToLower(adapted), "cross apply mo_view_columns(")
			require.Equal(t, tc.withSub, strings.Contains(adapted, "mo_subscription_view_columns("))
			// The original ordinary-table projection remains byte-for-byte
			// unchanged, including each version's type/charset formatters.
			local := strings.TrimPrefix(tc.definition, "CREATE VIEW information_schema.COLUMNS AS "+informationSchemaMetadataVisibilityCTE())
			projection := strings.SplitN(local, "from mo_catalog.mo_columns", 2)[0]
			require.Contains(t, adapted, projection)
			lowerName := strings.Replace(tc.definition, "information_schema.COLUMNS", "information_schema.columns", 1)
			for _, input := range []string{
				lowerName,
				strings.Replace(tc.definition, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
				strings.Replace(lowerName, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
			} {
				got, ok := AdaptLegacyInformationSchemaColumnsDDL(input)
				require.True(t, ok)
				require.Equal(t, adapted, got)
			}
		})
	}
	v109 := InformationSchemaColumnsV109DDL()
	v109Adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(v109)
	require.True(t, ok)
	require.Equal(t, v109, v109Adapted, "the derived v109 template must be retained exactly")
	require.Contains(t, v109, "WHEN 4 then 'utf8'")
	require.NotContains(t, v109, "like 'MEDIUMINT%'")
	lowerV109 := strings.Replace(v109, "information_schema.COLUMNS", "information_schema.columns", 1)
	lowerAdapted, ok := AdaptLegacyInformationSchemaColumnsDDL(lowerV109)
	require.True(t, ok)
	require.Equal(t, v109, lowerAdapted, "only the historical lowercase view name is normalized")
	// The v109 lineage plus the MEDIUMINT DATA_TYPE arm is exactly the
	// pre-change current template (T0); #31461's binary-NULL selector
	// revision of it is the second frozen variant (T1): a binary from the
	// #31461 era persists T1 before the v4_0_18 entry recreates the view
	// with the attributed-DECIMAL decoration. Both frozen identities equal
	// their literal constructions, T1 equals the live pre-decoration
	// derivation, and both differ from the decorated current template.
	preAttributes := informationSchemaColumnsPreAttributesDDL()
	preAttributesBinaryNull := informationSchemaColumnsPreAttributesBinaryNullDDL()
	require.Equal(t,
		strings.ReplaceAll(v109, columnsV109DataTypeArm, columnsV109DataTypeArm+columnsV110DataTypeArm),
		preAttributes)
	require.Equal(t,
		strings.ReplaceAll(preAttributes, "WHEN 2 then 'binary'", "WHEN 2 then NULL"),
		preAttributesBinaryNull)
	require.Equal(t, informationSchemaCurrentColumnsDDL(), preAttributesBinaryNull,
		"the #31461-era derivation of the pre-change template is the second frozen variant")
	require.NotEqual(t, InformationSchemaColumnsDDL, preAttributes,
		"the decorated current template differs from the pre-change identity")
	require.NotEqual(t, InformationSchemaColumnsDDL, preAttributesBinaryNull,
		"the decorated current template differs from the second pre-change variant")
	// Each frozen pre-change variant passes through byte-stable, with the
	// same lowercase view-name normalization as the other pinned identities,
	// and it is a known template for the explicitly owned derived-metadata
	// statements.
	for _, frozen := range []string{preAttributes, preAttributesBinaryNull} {
		adaptedFrozen, ok := AdaptLegacyInformationSchemaColumnsDDL(frozen)
		require.True(t, ok)
		require.Equal(t, frozen, adaptedFrozen, "the pre-change current template is a pass-through identity")
		lowerFrozen := strings.Replace(frozen, "information_schema.COLUMNS", "information_schema.columns", 1)
		lowerFrozenAdapted, ok := AdaptLegacyInformationSchemaColumnsDDL(lowerFrozen)
		require.True(t, ok)
		require.Equal(t, frozen, lowerFrozenAdapted, "only the historical lowercase view name is normalized")
		metadataFrozen, ok := ViewMetadataColumnsDDL(frozen)
		require.True(t, ok, "the pre-change current template is known to the derived-metadata adapter")
		require.Contains(t, metadataFrozen, "mo_view_columns(mt.reldatabase, mt.relname, mt.rel_id)")
		require.False(t, IsCurrentInformationSchemaColumnsDDL(frozen),
			"the pre-change template is historical, not the current identity")
		require.False(t, IsInformationSchemaColumnsMigrationDDL(frozen),
			"the pre-change template is an adapter input, not a migration gate")
	}
	require.False(t, IsInformationSchemaColumnsMigrationDDL(v109))
	require.False(t, IsCurrentInformationSchemaColumnsDDL(preAttributes),
		"the pre-change template is historical, not the current identity")
	require.False(t, IsInformationSchemaColumnsMigrationDDL(preAttributes),
		"the pre-change template is an adapter input, not a migration gate")

	adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(InformationSchemaColumnsV58DDL())
	require.True(t, ok)
	// The current view knows native Unicode selector identities, while a
	// persisted V58 definition must retain its historical projection.
	require.NotEqual(t, InformationSchemaColumnsDDL, adapted)
	require.NotContains(t, adapted, "WHEN 4 then")
	require.NotContains(t, adapted, "WHEN 5 then")
	adapted, ok = AdaptLegacyInformationSchemaColumnsDDL(InformationSchemaColumnsV100DDL())
	require.True(t, ok)
	require.Equal(t, InformationSchemaColumnsV100DDL(), adapted)
	require.NotContains(t, adapted, "WHEN 4 then")
	require.NotContains(t, adapted, "WHEN 5 then")
	for _, unknown := range []string{"", "CREATE VIEW information_schema.COLUMNS AS SELECT 1", InformationSchemaColumnsDDL, v109 + " ", preAttributes + " ", preAttributesBinaryNull + " ", strings.Replace(v109, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1) + " "} {
		adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(unknown)
		require.False(t, ok)
		require.Empty(t, adapted)
	}
	require.True(t, IsCurrentInformationSchemaColumnsDDL(InformationSchemaColumnsDDL))
	require.True(t, IsCurrentInformationSchemaColumnsDDL(strings.Replace(InformationSchemaColumnsDDL,
		"information_schema.COLUMNS", "information_schema.columns", 1)))
	require.False(t, IsCurrentInformationSchemaColumnsDDL(InformationSchemaColumnsV58DDL()))
	require.True(t, IsInformationSchemaColumnsMigrationDDL(InformationSchemaColumnsV100DDL()))
	require.True(t, IsInformationSchemaColumnsMigrationDDL(strings.Replace(InformationSchemaColumnsV100DDL(),
		"information_schema.COLUMNS", "information_schema.columns", 1)))
	require.True(t, IsInformationSchemaColumnsMigrationDDL(InformationSchemaColumnsDDL))
	require.False(t, IsInformationSchemaColumnsMigrationDDL(InformationSchemaColumnsV58DDL()))
	for _, tc := range []struct {
		definition string
		current    bool
		migration  bool
	}{
		{InformationSchemaColumnsDDL, true, true},
		{InformationSchemaColumnsV100DDL(), false, true},
		{InformationSchemaColumnsV58DDL(), false, false},
		{InformationSchemaColumnsDDL + " ", false, false},
	} {
		replacement := strings.Replace(tc.definition, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1)
		require.Equal(t, tc.current, IsCurrentInformationSchemaColumnsDDL(replacement))
		require.Equal(t, tc.migration, IsInformationSchemaColumnsMigrationDDL(replacement))
	}
}
