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

package frontend

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_newCdcSqlFormat(t *testing.T) {
	id, _ := uuid.Parse("019111fd-aed1-70c0-8760-9abadd8f0f4a")
	d := time.Date(2024, 8, 2, 15, 20, 0, 0, time.UTC)
	sql := getSqlForNewCdcTask(
		3,
		id,
		"task1",
		"src uri",
		"123",
		"dst uri",
		"mysql",
		"456",
		"ca path",
		"cert path",
		"key path",
		"db1:t1",
		"xfilter",
		"op filters",
		"error",
		"common",
		123,
		456,
		"conf path",
		d,
		"running",
		125,
		"xxx",
		"yyy",
	)
	wantSql := `insert into mo_catalog.mo_cdc_task values(3,"019111fd-aed1-70c0-8760-9abadd8f0f4a","task1","src uri","123","dst uri","mysql","456","ca path","cert path","key path","db1:t1","xfilter","op filters","error","common",123,"123",456,"456","conf path","2024-08-02 15:20:00","running",125,"125","xxx","yyy","","","","","")`
	assert.Equal(t, wantSql, sql)

	sql2 := getSqlForRetrievingCdcTask(3, id)
	wantSql2 := `select sink_uri, sink_type, sink_password, tables from mo_catalog.mo_cdc_task where account_id = 3 and task_id = "019111fd-aed1-70c0-8760-9abadd8f0f4a"`
	assert.Equal(t, wantSql2, sql2)
}
