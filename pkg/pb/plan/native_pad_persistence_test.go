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

func TestNativePADAndDependentFeatureCollection(t *testing.T) {
	for _, native := range []bool{false, true} {
		typ := Type{Id: planVarcharTypeID, Charset: 3}
		if native {
			typ.CollationVersion = 1
		}
		owner := &Query{Correlation: &CorrelationSpec{Programs: []*CorrelationProgram{{InputTypes: []Type{typ}, OutputTypes: []Type{typ}}}}, Nodes: []*Node{{ProjectList: []*Expr{{Typ: Type{Id: 22}, Expr: &Expr_Dependent{Dependent: &DependentExpr{ProgramId: 1, Inputs: []*Expr{{Typ: typ}}}}}}}}}
		separate, err := RequiredRemoteExpressionFeatures(owner)
		require.NoError(t, err)
		combined, err := RequiredExecutableRemoteExpressionFeatures(owner)
		require.NoError(t, err)
		require.Equal(t, separate, combined)
		require.True(t, combined.DependentPrograms)
		require.Equal(t, native, combined.NativePADCollation)
		// 裸program没有Expr，不能依赖dependent表达式恰巧引用其全部类型。
		bare, err := RequiredExecutableRemoteExpressionFeatures(owner.Correlation)
		require.NoError(t, err)
		require.Equal(t, native, bare.NativePADCollation)
	}
}

func TestNativePADDoesNotReinterpretUnicodePhysicalMarker(t *testing.T) {
	table := &TableDef{DefaultCharset: 3, CollationVersion: 1, KeyFormat: 1, Cols: []*ColDef{{Name: "u", Typ: Type{Id: planVarcharTypeID, Charset: 10, CollationVersion: 1}}}, Indexes: []*IndexDef{{Unique: true, KeyFormat: 1, Parts: []string{"u"}}}}
	require.NoError(t, RequireLegacyCollations(table))
	for _, collect := range []func(any) (RemoteExpressionFeatures, error){RequiredRemoteExpressionFeatures, RequiredExecutableRemoteExpressionFeatures} {
		features, err := collect(table)
		require.NoError(t, err)
		require.False(t, features.NativePADCollation, "旧表V1物理格式标记不是新的PAD默认语义")
	}
}

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
