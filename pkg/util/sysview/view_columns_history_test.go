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
	require.Equal(t, InformationSchemaColumnsDDL, adapted)
	for _, unknown := range []string{"", "CREATE VIEW information_schema.COLUMNS AS SELECT 1", InformationSchemaColumnsDDL} {
		adapted, ok := AdaptLegacyInformationSchemaColumnsDDL(unknown)
		require.False(t, ok)
		require.Empty(t, adapted)
	}
	require.True(t, IsCurrentInformationSchemaColumnsDDL(InformationSchemaColumnsDDL))
	require.True(t, IsCurrentInformationSchemaColumnsDDL(strings.Replace(InformationSchemaColumnsDDL,
		"information_schema.COLUMNS", "information_schema.columns", 1)))
	require.False(t, IsCurrentInformationSchemaColumnsDDL(InformationSchemaColumnsV58DDL()))
}
