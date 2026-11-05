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
	// their literal constructions - and equal the historical templates this
	// tree already recognizes as those generations (the V110 template and
	// the base-era NULL-selector variant), so the two construction routes
	// converge byte-for-byte.
	preAttributes := informationSchemaColumnsPreAttributesDDL()
	preAttributesBinaryNull := informationSchemaColumnsPreAttributesBinaryNullDDL()
	require.Equal(t,
		strings.ReplaceAll(v109, columnsV109DataTypeArm, columnsV109DataTypeArm+columnsV110DataTypeArm),
		preAttributes)
	require.Equal(t,
		strings.ReplaceAll(preAttributes, "WHEN 2 then 'binary'", "WHEN 2 then NULL"),
		preAttributesBinaryNull)
	require.Equal(t, preAttributes, InformationSchemaColumnsV110DDL(),
		"the frozen T0 text is exactly the historical V110 template")
	require.Equal(t, preAttributesBinaryNull, InformationSchemaColumnsBinaryFamilyNullDDL(),
		"the frozen T1 text is exactly the base-era NULL-selector identity")
	// The pre-decoration derivation of the merged current identity starts
	// from T1: the #31160 NULL selector stays and the label-aware and
	// unbounded-expression overloads layer over it, before the
	// attributed-DECIMAL decoration completes the current identity.
	require.Equal(t, informationSchemaColumnsUnboundedGeneratedExpressionDDL(
		informationSchemaColumnsEnumSetDDL(preAttributesBinaryNull)),
		informationSchemaCurrentColumnsDDL(),
		"the #31461-era derivation of the pre-change template is the pre-decoration current")
	require.Equal(t, informationSchemaColumnsWithUnsignedDecimalDDL(), InformationSchemaColumnsDDL,
		"the current identity is the attributed-DECIMAL decoration over the merged pre-decoration chain")
	require.NotEqual(t, InformationSchemaColumnsDDL, preAttributes,
		"the decorated current template differs from the pre-change identity")
	require.NotEqual(t, InformationSchemaColumnsDDL, preAttributesBinaryNull,
		"the decorated current template differs from the second pre-change variant")
	// Each frozen pre-change variant passes through byte-stable, with the
	// same lowercase view-name normalization as the other pinned identities,
	// and it stays a known template for the explicitly owned
	// derived-metadata statements. It also stays a migration gate: a
	// pre-upgrade tenant whose live definition is still one of these
	// generations keeps adapting historical snapshots instead of serving
	// them unchanged.
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
		require.True(t, IsInformationSchemaColumnsMigrationDDL(frozen),
			"the pre-change template stays a migration gate for pre-upgrade tenants")
	}
	require.False(t, IsInformationSchemaColumnsMigrationDDL(v109))
	require.False(t, IsCurrentInformationSchemaColumnsDDL(preAttributes),
		"the pre-change template is historical, not the current identity")
	require.True(t, IsInformationSchemaColumnsMigrationDDL(preAttributes),
		"the pre-change template stays a migration gate for pre-upgrade tenants")
	require.Contains(t, InformationSchemaColumnsDDL, "internal_char_length(mc.atttyp, mc.attr_enum)")
	require.NotContains(t, InformationSchemaColumnsV110DDL(), "internal_char_length(mc.atttyp, mc.attr_enum)")
	// The current definition renders the full stored generated expression
	// while the historical predecessor templates keep the 500-character
	// bound.
	require.NotContains(t, InformationSchemaColumnsDDL, "as varchar(500)) as GENERATION_EXPRESSION")
	require.Contains(t, InformationSchemaColumnsV110DDL(), "as varchar(500)) as GENERATION_EXPRESSION")
	// The current definition keeps the #31160 binary-family NULL selector;
	// the V110 template keeps its byte-stable historical selector.
	require.Contains(t, InformationSchemaColumnsDDL, "WHEN 2 then NULL")
	require.NotContains(t, InformationSchemaColumnsDDL, "WHEN 2 then 'binary'")
	require.Contains(t, InformationSchemaColumnsV110DDL(), "WHEN 2 then 'binary'")
	require.Equal(t, strings.ReplaceAll(InformationSchemaColumnsV110DDL(),
		"WHEN 2 then 'binary'", "WHEN 2 then NULL"), InformationSchemaColumnsBinaryFamilyNullDDL(),
		"the base-era current identity is exactly the V110 template with the NULL selector")
	// The V110 MEDIUMINT template is the current identity of the first
	// MEDIUMINT binary generation; it must stay byte-stable while binding
	// historical snapshots.
	v110 := InformationSchemaColumnsV110DDL()
	v110Adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(v110)
	require.True(t, ok)
	require.Equal(t, v110, v110Adapted, "the historical v110 template must be retained exactly")
	lowerV110 := strings.Replace(v110, "information_schema.COLUMNS", "information_schema.columns", 1)
	lowerAdapted, ok = AdaptLegacyInformationSchemaColumnsDDL(lowerV110)
	require.True(t, ok)
	require.Equal(t, v110, lowerAdapted, "only the historical lowercase view name is normalized")
	// Tenants whose persisted COLUMNS definition is still an on-demand-format
	// generation keep their historical snapshot migration; the gate follows
	// the definition recent binaries actually persisted as current.
	require.True(t, IsInformationSchemaColumnsMigrationDDL(v110))

	// H1a: the base-era NULL-selector MEDIUMINT identity - the exact text
	// base binaries published as their current definition - is recognized
	// at every reader seam: byte-stable snapshot adaptation, identical
	// lowercase normalization, and migration-gate membership, so a
	// pre-upgrade snapshot recorded under it keeps binding after the live
	// view refreshes to the merged identity.
	binaryNull := InformationSchemaColumnsBinaryFamilyNullDDL()
	binaryNullAdapted, ok := AdaptLegacyInformationSchemaColumnsDDL(binaryNull)
	require.True(t, ok)
	require.Equal(t, binaryNull, binaryNullAdapted,
		"the base-era current identity must be retained exactly")
	lowerBinaryNull := strings.Replace(binaryNull, "information_schema.COLUMNS", "information_schema.columns", 1)
	lowerAdapted, ok = AdaptLegacyInformationSchemaColumnsDDL(lowerBinaryNull)
	require.True(t, ok)
	require.Equal(t, binaryNull, lowerAdapted, "only the historical lowercase view name is normalized")
	require.True(t, IsInformationSchemaColumnsMigrationDDL(binaryNull),
		"the base-era current identity keeps the migration gate base granted it")
	lowerGate := IsInformationSchemaColumnsMigrationDDL(
		strings.Replace(binaryNull, "information_schema.COLUMNS", "information_schema.columns", 1))
	require.True(t, lowerGate)

	// D14-1: the upstream 4.0.18 published identity is frozen from the
	// binary-null pre-attributes template - the decoration applied
	// verbatim, never the mutable current constructor - so it preserves the
	// era-correct 500-character bound and NULL selectors, stays distinct
	// from the merged current identity, and binds at every reader seam
	// after the 4.0.19 refresh rewrote live views.
	published4018 := InformationSchemaColumns4018DDL()
	require.Equal(t, informationSchemaColumnsUnsignedDecimalColumnsTypeOver(preAttributesBinaryNull),
		published4018, "the 4.0.18 identity is the decoration over the frozen binary-null template")
	require.NotEqual(t, InformationSchemaColumnsDDL, published4018,
		"the merged current identity is label-aware and unbounded; the 4.0.18 identity is not")
	require.Contains(t, published4018, "case mc.att_is_unsigned when 3 then ' unsigned zerofill' when 1 then ' unsigned' else '' end")
	require.Contains(t, published4018, "as varchar(500)) as GENERATION_EXPRESSION")
	require.Contains(t, published4018, "WHEN 2 then NULL")
	require.Contains(t, published4018, "like 'MEDIUMINT%'")
	require.NotContains(t, published4018, "internal_char_length(mc.atttyp, mc.attr_enum)")
	adapted4018, ok := AdaptLegacyInformationSchemaColumnsDDL(published4018)
	require.True(t, ok)
	require.Equal(t, published4018, adapted4018, "the 4.0.18 published identity is a pass-through identity")
	lower4018 := strings.Replace(published4018, "information_schema.COLUMNS", "information_schema.columns", 1)
	lowerAdapted4018, ok := AdaptLegacyInformationSchemaColumnsDDL(lower4018)
	require.True(t, ok)
	require.Equal(t, published4018, lowerAdapted4018, "only the historical lowercase view name is normalized")
	metadata4018, ok := ViewMetadataColumnsDDL(published4018)
	require.True(t, ok, "the 4.0.18 published identity is known to the derived-metadata adapter")
	require.Contains(t, metadata4018, "mo_view_columns(mt.reldatabase, mt.relname, mt.rel_id)")
	require.False(t, IsCurrentInformationSchemaColumnsDDL(published4018),
		"the 4.0.18 identity is historical, not the current identity")
	require.True(t, IsInformationSchemaColumnsMigrationDDL(published4018),
		"the 4.0.18 identity gates historical snapshot adaptation until the 4.0.19 refresh")

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
	for _, unknown := range []string{"", "CREATE VIEW information_schema.COLUMNS AS SELECT 1", InformationSchemaColumnsDDL, v109 + " ", preAttributes + " ", preAttributesBinaryNull + " ", published4018 + " ", strings.Replace(v109, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1) + " "} {
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
		{InformationSchemaColumns4018DDL(), false, true},
		{InformationSchemaColumnsDDL + " ", false, false},
	} {
		replacement := strings.Replace(tc.definition, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1)
		require.Equal(t, tc.current, IsCurrentInformationSchemaColumnsDDL(replacement))
		require.Equal(t, tc.migration, IsInformationSchemaColumnsMigrationDDL(replacement))
	}
}
