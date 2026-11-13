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
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The current COLUMNS template renders the physical-BOOL COLUMN_TYPE as
// MySQL's tinyint(1) (issue #31749) on every one of its four branches: the
// local mo_columns branch, the derived mo_view_columns branch, the
// mo_subscription_columns branch and the mo_subscription_view_columns
// branch. DATA_TYPE keeps the historical 'TINYINT' (MySQL's DATA_TYPE for
// BOOLEAN stays tinyint), and the physical type name, the wire OID, the SHOW
// CREATE/dump renderers and every frozen historical template stay untouched.
func TestInformationSchemaColumnsPhysicalBoolColumnTypeIsTinyIntOne(t *testing.T) {
	const boolLatch = "case when upper(mo_show_visible_bin(mc.atttyp,2)) = 'BOOL' then 'tinyint(1)' else " +
		"(case when length(mc.attr_enum) > 0 then mo_show_visible_bin_enum(mc.atttyp, mc.attr_enum) " +
		"else mo_show_visible_bin(mc.atttyp,3) end) end"
	// Every current branch carries exactly one latch arm.
	require.Equal(t, 4, strings.Count(InformationSchemaColumnsDDL, boolLatch))
	// The three non-local branches keep the arm directly as COLUMN_TYPE.
	require.Equal(t, 3, strings.Count(InformationSchemaColumnsDDL, boolLatch+" as COLUMN_TYPE,"))
	// The local mo_columns branch keeps its VARCHAR(65535) header cast and
	// the attributed-DECIMAL suffix: the latch only rewrites the shared inner
	// projection inside the concatenation.
	require.Contains(t, InformationSchemaColumnsDDL,
		"cast(concat(case when upper(mo_show_visible_bin(mc.atttyp,2)) = 'BOOL' then 'tinyint(1)' else "+
			"(case when length(mc.attr_enum) > 0 then mo_show_visible_bin_enum(mc.atttyp, mc.attr_enum) else mo_show_visible_bin(mc.atttyp,3) end) end, "+
			"case when upper(mo_show_visible_bin(mc.atttyp,2)) like 'DECIMAL%' then "+
			"case mc.att_is_unsigned when 3 then ' unsigned zerofill' when 1 then ' unsigned' else '' end "+
			"else '' end) as varchar(65535)) as COLUMN_TYPE,")
	// DATA_TYPE keeps its historical arm in all four branches: mode-2 BOOL
	// renders 'TINYINT'; MySQL parity stops at COLUMN_TYPE.
	require.Equal(t, 4, strings.Count(InformationSchemaColumnsDDL,
		"upper(mo_show_visible_bin(mc.atttyp,2)) = 'BOOL' then 'TINYINT' "+
			"when upper(mo_show_visible_bin(mc.atttyp,3)) like 'MEDIUMINT%' then 'MEDIUMINT' "))
	require.Equal(t, 0, strings.Count(InformationSchemaColumnsDDL, "'tinyint(1)' as DATA_TYPE"))
	// The frozen historical families do not gain the latch.
	for _, frozen := range []string{
		InformationSchemaColumnsV41DDL, InformationSchemaColumnsV46DDL, InformationSchemaColumnsV46UpgradeDDL,
		InformationSchemaColumnsV58DDL(), InformationSchemaColumnsV100DDL(),
		InformationSchemaColumnsV109DDL(), InformationSchemaColumnsV110DDL(),
		InformationSchemaColumnsBinaryFamilyNullDDL(),
		InformationSchemaColumns4018DDL(), InformationSchemaColumnsPreDefaultGeneratedDDL(),
	} {
		require.NotContains(t, frozen, "'tinyint(1)'", "frozen historical templates stay byte-stable")
	}
}

// The pre-change identity of this revision must stop passing as current while
// every historical reader keeps recognizing it byte-stably, so a pre-upgrade
// tenant's live definition and its recorded snapshots keep binding and the
// shared upgrade entry converges the live view exactly once. On the un-fixed
// tree the pre-change derivation still IS the current identity, so the
// IsCurrent negative and the latch asserts above form the RED pair; the
// recognition asserts here hold the boundary after the fix ships.
func TestInformationSchemaColumnsPreBoolTinyIntOneIdentityReaders(t *testing.T) {
	pre := informationSchemaColumnsPhysicalIndexDDL()
	require.False(t, IsCurrentInformationSchemaColumnsDDL(pre),
		"the pre-change identity of this revision must not pass as current")
	require.True(t, IsInformationSchemaColumnsMigrationDDL(pre),
		"the pre-change identity keeps the migration gate")
	adaptedByLegacy, ok := AdaptLegacyInformationSchemaColumnsDDL(pre)
	require.True(t, ok, "the pre-change identity is a pass-through identity")
	require.Equal(t, pre, adaptedByLegacy)
	adaptedByMetadata, ok := ViewMetadataColumnsDDL(pre)
	require.True(t, ok, "the pre-change identity is known to the derived-metadata adapter")
	require.Equal(t,
		strings.NewReplacer(
			"mo_view_columns(mt.rel_id)", "mo_view_columns(mt.reldatabase, mt.relname, mt.rel_id)",
			"mo_subscription_view_columns(mt.publisher_account_id, mt.rel_id)", "mo_subscription_view_columns(mt.reldatabase, mt.relname, mt.rel_id)",
		).Replace(pre),
		adaptedByMetadata)
	require.NotEqual(t, pre, InformationSchemaColumnsDDL)
	require.True(t, IsCurrentInformationSchemaColumnsDDL(InformationSchemaColumnsDDL))
}

// The frozen pre-change identity equals, byte for byte, the live current
// definition recorded before this change (batch evidence
// baseline/prechange-current-columns-ddl.txt): the frozen constructor keeps
// publishing the exact text a pre-change binary persisted as its live
// definition, through its own fixed construction route rather than by
// undoing the new identity, so the recorded fixture and the constructor can
// be cross-checked during review.
func TestInformationSchemaColumnsPreBoolTinyIntOneFrozenFixture(t *testing.T) {
	pre := InformationSchemaColumnsPreBoolTinyIntOneDDL()
	sum := sha256.Sum256([]byte(pre))
	require.Equal(t, "4f3867805cc3258ce3ab90b0886d5b50404dd5b0b64b100f48a424e57ac7bdef",
		hex.EncodeToString(sum[:]),
		"the frozen pre-change identity equals the recorded pre-change fixture")
	require.Len(t, pre, 19557)
	require.Equal(t, informationSchemaColumnsPhysicalIndexDDL(), pre,
		"the frozen identity is exactly the previous fixed derivation")
	// The frozen identity is unlatched: its four branches keep the plain
	// COLUMN_TYPE projection, so the change never rewrites bound history.
	const enumOrVisibleColumnType = "(case when length(mc.attr_enum) > 0 then " +
		"mo_show_visible_bin_enum(mc.atttyp, mc.attr_enum) else mo_show_visible_bin(mc.atttyp,3) end)"
	require.Equal(t, 4, strings.Count(pre, enumOrVisibleColumnType))
	require.Equal(t, 0, strings.Count(pre, "'tinyint(1)'"))
	// The current identity is exactly the tinyint(1) derivation over the
	// frozen pre-change identity.
	require.Equal(t, informationSchemaColumnsWithBoolTinyIntOneDDL(), InformationSchemaColumnsDDL)
	require.Equal(t,
		informationSchemaColumnsBoolTinyIntOneColumnsTypeOver(pre), InformationSchemaColumnsDDL)
	// Authoring variants of the frozen identity pass the same readers with
	// the documented authoring-form and view-name normalization only.
	lower := strings.Replace(pre, "information_schema.COLUMNS", "information_schema.columns", 1)
	for _, variant := range []string{
		lower,
		strings.Replace(pre, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
		strings.Replace(lower, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
	} {
		adaptedByLegacy, ok := AdaptLegacyInformationSchemaColumnsDDL(variant)
		require.True(t, ok)
		require.Equal(t, pre, adaptedByLegacy)
		require.True(t, IsInformationSchemaColumnsMigrationDDL(variant))
		adaptedByMetadata, ok := ViewMetadataColumnsDDL(variant)
		require.True(t, ok)
		require.Equal(t, strings.NewReplacer(
			"mo_view_columns(mt.rel_id)", "mo_view_columns(mt.reldatabase, mt.relname, mt.rel_id)",
			"mo_subscription_view_columns(mt.publisher_account_id, mt.rel_id)", "mo_subscription_view_columns(mt.reldatabase, mt.relname, mt.rel_id)",
		).Replace(pre), adaptedByMetadata)
	}
}
