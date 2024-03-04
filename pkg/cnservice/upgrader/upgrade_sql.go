// Copyright 2023 Matrix Origin
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

package upgrader

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

type sqlWithSomeDetails struct {
	sql     string
	schema  string
	table   string
	tenant  string
	account uint32

	modified func(*table.Table) bool
}

var sqls = []sqlWithSomeDetails{
	{
		sql:     "alter table `mo_task`.`sys_async_task` modify task_id bigint auto_increment",
		schema:  "mo_task",
		table:   "sys_async_task",
		tenant:  frontend.GetDefaultTenant(),
		account: catalog.System_Account,
		modified: func(tbl *table.Table) bool {
			for _, col := range tbl.Columns {
				if col.Name != "task_id" {
					continue
				}
				if col.ColType == table.TInt64 {
					return true
				}
			}
			return false
		},
	},
}
