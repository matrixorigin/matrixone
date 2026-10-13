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
	"crypto/sha256"
	"fmt"
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
			lowerAdapted, ok := AdaptLegacyInformationSchemaColumnsDDL(lowerName)
			require.True(t, ok)
			require.Equal(t, adapted, lowerAdapted)
		})
	}
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
	for _, unknown := range []string{"", "CREATE VIEW information_schema.COLUMNS AS SELECT 1", InformationSchemaColumnsDDL} {
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
}

func TestPreLifecycleInformationSchemaColumnsDefinitions(t *testing.T) {
	// These digests pin the persisted templates at main 21f711a7b852. Build
	// the old inputs independently of the production compatibility helper;
	// a future template edit must not silently replace historical fixtures.
	const localFilter = " and not regexp_like(lower(mt.relname), '^__mo_lifecycle_restore_[0-9a-f]{32}$')"
	const subscriptionFilter = " and not regexp_like(lower(mc.att_relname), '^__mo_lifecycle_restore_[0-9a-f]{32}$')"
	for _, tc := range []struct {
		name           string
		definition     string
		digest         string
		alreadyDerived bool
	}{
		{"V41", InformationSchemaColumnsV41DDL, "ecf42f9c00c752e28e575069790ff263b115c5d114cd1228e95b78dee335d6e8", false},
		{"V46", InformationSchemaColumnsV46DDL, "b2a9fa0e37582847ce80ba77777cf161f32599fd0c8e05c1f00f59b9ff4a79cb", false},
		{"V46 upgrade", InformationSchemaColumnsV46UpgradeDDL, "fa08271bbd2860748fac0acd894f54af2d8f02e837107d66dcad13f42d8ff193", false},
		{"V58", InformationSchemaColumnsV58DDL(), "67d8633251411d3761eb30ad52bf553d1a91f874fe7b7142ab5f5d553c0315d7", false},
		{"V100", InformationSchemaColumnsV100DDL(), "b85fd86796a1d2d32cf3c304ea05e49da52c8818da78a8512b4ccad45756109a", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeLifecycle := strings.ReplaceAll(tc.definition, localFilter, "")
			beforeLifecycle = strings.ReplaceAll(beforeLifecycle, subscriptionFilter, "")
			require.Equal(t, tc.digest, fmt.Sprintf("%x", sha256.Sum256([]byte(beforeLifecycle))))
			require.NotContains(t, beforeLifecycle, "__mo_lifecycle_restore_")
			for _, definition := range []string{
				beforeLifecycle,
				strings.Replace(beforeLifecycle, "information_schema.COLUMNS", "information_schema.columns", 1),
			} {
				require.False(t, IsCurrentInformationSchemaColumnsDDL(definition))
				require.Equal(t, tc.alreadyDerived, IsInformationSchemaColumnsMigrationDDL(definition))
				adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(definition)
				require.True(t, ok)
				if tc.alreadyDerived {
					require.Equal(t, beforeLifecycle, adapted, "on-demand historical templates must remain byte stable")
				} else {
					require.Equal(t, informationSchemaDerivedColumnsDDL(beforeLifecycle), adapted)
				}
				require.NotContains(t, adapted, "__mo_lifecycle_restore_")
			}
			unknown := beforeLifecycle + " WHERE false"
			require.False(t, IsCurrentInformationSchemaColumnsDDL(unknown))
			require.False(t, IsInformationSchemaColumnsMigrationDDL(unknown))
			adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(unknown)
			require.False(t, ok)
			require.Empty(t, adapted)
		})
	}
	// The current definition has evolved beyond the historical V100 projection.
	// Its pre-Lifecycle form is current too, without rewriting historical bytes.
	currentBeforeLifecycle := strings.ReplaceAll(InformationSchemaColumnsDDL, localFilter, "")
	currentBeforeLifecycle = strings.ReplaceAll(currentBeforeLifecycle, subscriptionFilter, "")
	for _, definition := range []string{
		currentBeforeLifecycle,
		strings.Replace(currentBeforeLifecycle, "information_schema.COLUMNS", "information_schema.columns", 1),
	} {
		require.True(t, IsCurrentInformationSchemaColumnsDDL(definition))
		require.True(t, IsInformationSchemaColumnsMigrationDDL(definition))
		adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(definition)
		require.False(t, ok)
		require.Empty(t, adapted)
		require.False(t, IsInformationSchemaColumnsMigrationDDL(definition+" WHERE false"))
	}
	// A partially rewritten current definition is neither historical template.
	partial := strings.Replace(InformationSchemaColumnsDDL, localFilter, "", 1)
	require.False(t, IsCurrentInformationSchemaColumnsDDL(partial))
	require.False(t, IsInformationSchemaColumnsMigrationDDL(partial))
	adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(partial)
	require.False(t, ok)
	require.Empty(t, adapted)
}
