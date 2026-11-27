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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestEnumAssignmentDDLIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, ddl         string
		wantIdentity      uint32
		wantOrdinal       types.Enum
		inherited, reject bool
	}{
		{"explicit binary", "create table t(e enum('A','a') collate utf8mb4_bin)", types.EnumCollationUTF8Bin, 2, false, false},
		{"explicit general ci", "create table t(e enum('A','a') collate utf8mb4_general_ci)", types.EnumCollationGeneralCI, 1, false, false},
		{"same CREATE default", "create table t(e enum('A','a')) collate=utf8mb4_bin", types.EnumCollationUTF8Bin, 2, false, false},
		{"explicit overrides CREATE default", "create table t(e enum('A','a') collate utf8mb4_bin) collate=utf8mb4_general_ci", types.EnumCollationUTF8Bin, 2, false, false},
		{"bare stays legacy", "create table t(e enum('A','a'))", types.EnumCollationLegacy, 1, false, false},
		{"typed legacy overrides displayed options", "create table t(e enum('A','a')) collate=utf8mb4_bin", types.EnumCollationLegacy, 1, true, false},
		{"unsupported explicit collation", "create table t(e enum('A','a') collate utf8mb4_0900_bin)", 0, 0, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			parsed, err := mysql.ParseOne(ctx, tc.ddl, 1)
			require.NoError(t, err)
			stmt := parsed.(*tree.CreateTable)
			if tc.inherited {
				stmt.InheritedEnums = map[string]Type{
					"e": {Id: int32(types.T_enum), Enumvalues: "A,a", EnumEncoding: types.EnumEncodingLegacy},
				}
			}
			col, err := getCreateColumnType(ctx, stmt, stmt.Defs[0].(*tree.ColumnTableDef))
			if tc.reject {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantIdentity, col.EnumCollation)
			members, err := types.DecodeEnumMembers(col.Enumvalues, col.EnumEncoding)
			require.NoError(t, err)
			got, err := members.ParseCollated("a", col.EnumCollation)
			require.NoError(t, err)
			require.Equal(t, tc.wantOrdinal, got)
		})
	}
}

func TestEnumAssignmentDomainIncludesCollation(t *testing.T) {
	legacy := Type{Id: int32(types.T_enum), Enumvalues: "A,a"}
	bin := legacy
	bin.EnumCollation = types.EnumCollationUTF8Bin
	require.False(t, isSameColumnType(legacy, bin), "different assignment domains cannot pass through an ordinal")
	require.True(t, isSameColumnType(bin, *DeepCopyType(&bin)), "typed plan copies retain identity")
}
