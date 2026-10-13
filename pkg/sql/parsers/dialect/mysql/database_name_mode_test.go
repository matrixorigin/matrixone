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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestDropDatabaseUsesCreateDatabaseCatalogName(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode int64
		want string
	}{
		{name: "case sensitive", mode: 0, want: "QaMode2Base"},
		{name: "stored lowercase", mode: 1, want: "qamode2base"},
		{name: "case preserving", mode: 2, want: "QaMode2Base"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			create, err := ParseOne(context.Background(), "CREATE DATABASE QaMode2Base", tc.mode)
			require.NoError(t, err)
			createdName := string(create.(*tree.CreateDatabase).Name)
			require.Equal(t, tc.want, createdName)

			for _, sql := range []string{
				"DROP DATABASE QaMode2Base",
				"DROP SCHEMA IF EXISTS `QaMode2Base`",
			} {
				drop, err := ParseOne(context.Background(), sql, tc.mode)
				require.NoError(t, err)
				require.Equal(t, createdName, string(drop.(*tree.DropDatabase).Name), sql)
			}

			show, err := ParseOne(context.Background(), "SHOW TABLES FROM `QaMode2Base`", tc.mode)
			require.NoError(t, err)
			require.Equal(t, createdName, show.(*tree.ShowTables).DBName)
		})
	}
}
