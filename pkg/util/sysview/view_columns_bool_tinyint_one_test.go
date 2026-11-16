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
