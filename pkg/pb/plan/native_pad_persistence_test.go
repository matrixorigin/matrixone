// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package plan

import (
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativePADExecutableAdmissionCollectsTableDefaults(t *testing.T) {
	for _, identity := range []uint32{2, 3} {
		table := &TableDef{DefaultCharset: identity, CollationVersion: 1}
		separate, err := RequiredRemoteExpressionFeatures(table)
		require.NoError(t, err)
		combined, err := RequiredExecutableRemoteExpressionFeatures(table)
		require.NoError(t, err)
		require.True(t, separate.NativePADCollation)
		require.Equal(t, separate, combined, "无native结果列的table default也必须保留协议需求")
	}
}

func TestNativePADPersistedMetadataKeepsPhysicalKeysClosed(t *testing.T) {
	for _, identity := range []uint32{2, 3} {
		base := &TableDef{Name: "t", DefaultCharset: identity, CollationVersion: 1, Cols: []*ColDef{
			{Name: "s", Typ: Type{Id: planVarcharTypeID, Charset: identity, CollationVersion: 1}},
			{Name: "i", Typ: Type{Id: 22}},
		}}
		require.NoError(t, RequirePersistedCollations(base))
		require.NoError(t, RequireExecutableCollations(base))
		require.Error(t, RequireLegacyCollations(base))
		for _, tc := range []struct {
			name     string
			mutate   func(*TableDef)
			rejected bool
		}{
			{"primary_flag", func(d *TableDef) { d.Cols[0].Primary = true }, true},
			{"cluster_flag", func(d *TableDef) { d.Cols[0].ClusterBy = true }, true},
			{"int_pk", func(d *TableDef) { d.Pkey = &PrimaryKeyDef{Names: []string{"i"}, PkeyColName: "i"} }, false},
			{"native_pk", func(d *TableDef) { d.Pkey = &PrimaryKeyDef{Names: []string{"i", "s"}, PkeyColName: "__mo_cpkey_col"} }, true},
			{"single_pk", func(d *TableDef) { d.Pkey = &PrimaryKeyDef{PkeyColName: "s"} }, true},
			{"int_index", func(d *TableDef) { d.Indexes = []*IndexDef{{Parts: []string{"i"}}} }, false},
			{"native_index", func(d *TableDef) { d.Indexes = []*IndexDef{{Parts: []string{"s"}}} }, true},
			{"native_unique", func(d *TableDef) { d.Indexes = []*IndexDef{{Unique: true, Parts: []string{"i", "s"}}} }, true},
			{"alias", func(d *TableDef) { d.Indexes = []*IndexDef{{Parts: []string{"__mo_alias_S"}}} }, true},
			{"int_cluster", func(d *TableDef) { d.ClusterBy = &ClusterByDef{Name: "i"} }, false},
			{"native_cluster", func(d *TableDef) { d.ClusterBy = &ClusterByDef{Name: "s"} }, true},
			{"native_composite_cluster", func(d *TableDef) { d.ClusterBy = &ClusterByDef{Name: "__mo_cbkey_001i001s"} }, true},
			{"int_composite_cluster", func(d *TableDef) { d.ClusterBy = &ClusterByDef{Name: "__mo_cbkey_001i"} }, false},
			{"malformed_cluster", func(d *TableDef) { d.ClusterBy = &ClusterByDef{Name: "__mo_cbkey_100i"} }, true},
			{"unknown_revision", func(d *TableDef) { d.Cols[0].Typ.CollationVersion = 2 }, true},
			{"unknown_format", func(d *TableDef) { d.KeyFormat = 2 }, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				d := proto.Clone(base).(*TableDef)
				tc.mutate(d)
				before := proto.Clone(d)
				err := RequirePersistedCollations(d)
				if tc.rejected {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.True(t, proto.Equal(before, d), "准入不得重标记schema")
			})
		}
	}
}
