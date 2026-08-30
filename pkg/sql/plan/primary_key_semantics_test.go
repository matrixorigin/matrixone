// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKeyColumnPositionsFailClosedOnMalformedMetadata(t *testing.T) {
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	tests := []struct {
		name      string
		table     *planpb.TableDef
		want      []int32
		wantValid bool
	}{
		{
			name: "column scan fallback",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "ID", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
			want:      []int32{0},
			wantValid: true,
		},
		{
			name: "legacy simple key without names",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "ID", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id"},
			},
			want:      []int32{0},
			wantValid: true,
		},
		{
			name: "missing storage key identity",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "id", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}},
			},
		},
		{
			name: "case varied fake key",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "id", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: "__MO_FAKE_PK_COL",
					Names:       []string{"id"},
				},
			},
		},
		{
			name: "case varied hidden composite without components",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "__MO_CPKEY_COL", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: "__MO_CPKEY_COL",
				},
			},
		},
		{
			name: "legacy hidden composite without components",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "__mo_cpkey_002id006tenant", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: "__mo_cpkey_002id006tenant",
				},
			},
		},
		{
			name: "simple key storage identity disagrees with names",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "tenant", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: "tenant",
					Names:       []string{"id"},
				},
			},
		},
		{
			name: "composite components without hidden storage identity",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "tenant", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: "id",
					Names:       []string{"id", "tenant"},
				},
			},
		},
		{
			name: "legacy encoded composite storage identity",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "tenant", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: catalog.PrefixPriColName + "002id006tenant",
					Names:       []string{"id", "tenant"},
				},
			},
			want:      []int32{0, 1},
			wantValid: true,
		},
		{
			name: "composite cluster by identity is not a primary key",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "tenant", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: catalog.PrefixCBColName + "002id006tenant",
					Names:       []string{"id", "tenant"},
				},
			},
		},
		{
			name: "legacy encoded components disagree with names",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "tenant", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: catalog.PrefixPriColName + "002id005wrong",
					Names:       []string{"id", "tenant"},
				},
			},
		},
		{
			name: "duplicate composite component",
			table: &planpb.TableDef{
				Cols:          []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "tenant", Typ: intType}},
				Name2ColIndex: map[string]int32{"id": 0, "tenant": 1},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: catalog.CPrimaryKeyColName,
					Names:       []string{"id", "id"},
				},
			},
		},
		{
			name: "empty current composite component",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "", Typ: intType}, {Name: "tenant", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: catalog.CPrimaryKeyColName,
					Names:       []string{"", "tenant"},
				},
			},
		},
		{
			name: "conflicting name index",
			table: &planpb.TableDef{
				Cols:          []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "payload", Typ: intType}},
				Name2ColIndex: map[string]int32{"id": 1},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
		},
		{
			name: "indexed name cannot hide ambiguous columns",
			table: &planpb.TableDef{
				Cols:          []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "ID", Typ: intType}},
				Name2ColIndex: map[string]int32{"id": 0},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
		},
		{
			name: "nil column definition",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{nil, {Name: "payload", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
		},
		{
			name: "ambiguous fallback columns",
			table: &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "ID", Typ: intType}},
				Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			positions, ok := primaryKeyColumnPositions(test.table)
			require.Equal(t, test.wantValid, ok)
			require.Equal(t, test.want, positions)
		})
	}
}

func TestPrimaryKeyTypeSupportsSQLEqualityProof(t *testing.T) {
	for _, oid := range []types.T{
		types.T_int64,
		types.T_decimal128,
		types.T_varchar,
		types.T_varbinary,
		types.T_timestamp,
		types.T_uuid,
	} {
		require.True(t, primaryKeyTypeSupportsSQLEqualityProof(oid), oid.String())
	}

	for _, oid := range []types.T{
		types.T_float32,
		types.T_float64,
		types.T_char,
		types.T_json,
		types.T_enum,
	} {
		require.False(t, primaryKeyTypeSupportsSQLEqualityProof(oid), oid.String())
	}
}

func TestSQLEqualityCompatiblePrimaryKeyRejectsMalformedDecimalMetadata(t *testing.T) {
	table := &planpb.TableDef{
		Cols: []*planpb.ColDef{{
			Name: "id",
			Typ: planpb.Type{
				Id: int32(types.T_decimal64), Width: 19, Scale: 2, NotNullable: true,
			},
		}},
		Name2ColIndex: map[string]int32{"id": 0},
		Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}

	_, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(table)
	require.False(t, ok)

	table.Cols[0].Typ.Width = 18
	positions, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(table)
	require.True(t, ok)
	require.Equal(t, []int32{0}, positions)
}

func TestSQLEqualityCompatiblePrimaryKeyRequiresRawVarcharCollation(t *testing.T) {
	table := &planpb.TableDef{
		Cols: []*planpb.ColDef{{
			Name: "id",
			Typ:  planpb.Type{Id: int32(types.T_varchar), Width: 16, NotNullable: true},
		}},
		Name2ColIndex: map[string]int32{"id": 0},
		Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}

	for _, charset := range []uint8{types.CharsetLegacy, types.CharsetBinary} {
		table.Cols[0].Typ.Charset = uint32(charset)
		_, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(table)
		require.True(t, ok)
	}
	for _, charset := range []uint8{types.CharsetUTF8MB4Bin, types.CharsetUTF8} {
		table.Cols[0].Typ.Charset = uint32(charset)
		_, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(table)
		require.False(t, ok)
	}
}

func TestSQLEqualityJoinUsesOneIdentityDomain(t *testing.T) {
	require.True(t, sqlEqualityJoinUsesOneIdentityDomain(
		planpb.Type{Id: int32(types.T_varchar), Width: 8},
		planpb.Type{Id: int32(types.T_varchar), Width: 32}))
	require.False(t, sqlEqualityJoinUsesOneIdentityDomain(
		planpb.Type{Id: int32(types.T_datetime)},
		planpb.Type{Id: int32(types.T_timestamp)}))
	require.False(t, sqlEqualityJoinUsesOneIdentityDomain(
		planpb.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 2},
		planpb.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 3}))
	require.False(t, sqlEqualityJoinUsesOneIdentityDomain(
		planpb.Type{Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetLegacy)},
		planpb.Type{Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetUTF8)}))
}
