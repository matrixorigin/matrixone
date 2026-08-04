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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestLongRemainsUsableAsIdentifier(t *testing.T) {
	_, err := ParseOne(context.Background(), "select long", 1)
	require.NoError(t, err)
}

func TestLongStringTypeAliases(t *testing.T) {
	tests := []struct {
		name         string
		typeSQL      string
		family       tree.Family
		familyString string
		oid          defines.MysqlType
	}{
		{
			name:         "long varchar",
			typeSQL:      "long varchar",
			family:       tree.BlobFamily,
			familyString: "mediumtext",
			oid:          defines.MYSQL_TYPE_TEXT,
		},
		{
			name:         "long varbinary",
			typeSQL:      "long varbinary",
			family:       tree.BlobFamily,
			familyString: "mediumblob",
			oid:          defines.MYSQL_TYPE_MEDIUM_BLOB,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typ := parseColumnType(t, test.typeSQL)
			require.Equal(t, test.family, typ.InternalType.Family)
			require.Equal(t, test.familyString, typ.InternalType.FamilyString)
			require.Equal(t, uint32(test.oid), typ.InternalType.Oid)
		})
	}
}
