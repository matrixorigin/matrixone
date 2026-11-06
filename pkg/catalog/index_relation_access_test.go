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

package catalog

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
)

func TestPhysicalIndexRelationClassification(t *testing.T) {
	id := "0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd"
	session := uuid.MustParse("018f1f76-7b9d-7f35-b2d9-9b8d7774bde8")
	index := SecondaryIndexTableNamePrefix + id
	cases := []struct {
		name, kind string
		want       bool
	}{
		{index, "", true},
		{strings.ToUpper(index), "", true},
		{"nonreserved", SystemIndexRel, true},
		{defines.GenTempTableName(session, "db", "generation_"+index), "", true},
		{defines.GenTempTableName(session, index, "ordinary"), "", false},
		{"orders__mo_index_secondary_not_an_index", SystemOrdinaryRel, false},
		{"ordinary", SystemOrdinaryRel, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, RequiresPrivilegedRelationAccess(tc.name, tc.kind, 0))
		})
	}
	for _, kind := range append(userVisibleRelationKinds[:], "", SystemTemporaryTable, SystemPersistRel, SystemTransientRel, "V") {
		t.Run("core/"+kind, func(t *testing.T) {
			require.False(t, RequiresPrivilegedRelationAccess("ordinary", kind, 0))
			require.False(t, RequiresPrivilegedRelationAccess("ordinary", kind, features.Partition))
			require.True(t, RequiresPrivilegedRelationAccess("ordinary", kind, features.IndexTable))
		})
	}
	for _, kind := range []string{SystemIndexRel, "unregistered_plugin_kind", Cagra_TblType_Storage, Ivfpq_TblType_Metadata} {
		require.True(t, RequiresPrivilegedRelationAccess("ordinary", kind, 0), kind)
	}
}

func TestNonIndexRelationSQLPredicate(t *testing.T) {
	require.Equal(t, "not mo_is_physical_index(coalesce(relname, ''), coalesce(relkind, ''), coalesce(extra_info, ''))", NonIndexRelationSQLPredicate(""))
	require.Equal(t, "not mo_is_physical_index(coalesce(tbl.relname, ''), coalesce(tbl.relkind, ''), coalesce(tbl.extra_info, ''))", NonIndexRelationSQLPredicate("tbl"))
	require.Equal(t, "not mo_is_physical_index(coalesce(mc.att_relname, ''), coalesce(mc.relkind, ''), coalesce(mc.extra_info, ''))", NonIndexRelationSQLPredicateForColumns("mc.att_relname", "mc.relkind", "mc.extra_info"))
}
