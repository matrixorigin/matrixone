// Copyright 2021 Matrix Origin
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

package ckp

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/stretchr/testify/assert"
)

func TestNormalizeCreateTableDDLName(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "compat_ckp",
		TableName:    "employees",
	}

	tests := []struct {
		name string
		ddl  string
		want string
	}{
		{
			name: "plain table name",
			ddl:  "CREATE TABLE employees_copy_123 (id INT)",
			want: "CREATE TABLE `compat_ckp`.`employees` (id INT)",
		},
		{
			name: "qualified table name",
			ddl:  "CREATE TABLE `compat_ckp`.`employees_copy_123` (id INT)",
			want: "CREATE TABLE `compat_ckp`.`employees` (id INT)",
		},
		{
			name: "if not exists",
			ddl:  "CREATE TABLE IF NOT EXISTS old_name (id INT)",
			want: "CREATE TABLE IF NOT EXISTS `compat_ckp`.`employees` (id INT)",
		},
		{
			name: "escaped target name",
			ddl:  "CREATE TABLE old_name (id INT)",
			want: "CREATE TABLE `compat``ckp`.`employees` (id INT)",
		},
	}

	tests[3].want = "CREATE TABLE `compat``ckp`.`employees` (id INT)"
	tests[3].ddl = "CREATE TABLE old_name (id INT)"
	tests[3].name = "escaped database name"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "escaped database name" {
				table.DatabaseName = "compat`ckp"
			} else {
				table.DatabaseName = "compat_ckp"
			}
			assert.Equal(t, tt.want, normalizeCreateTableDDLName(tt.ddl, table))
		})
	}
}
