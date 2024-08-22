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
	"context"
	"regexp"
	"sort"
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
	wantSql2 := `select sink_uri, sink_type, sink_password, tables, start_ts_str, checkpoint_str from mo_catalog.mo_cdc_task where account_id = 3 and task_id = "019111fd-aed1-70c0-8760-9abadd8f0f4a"`
	assert.Equal(t, wantSql2, sql2)
}

func Test_parseTables(t *testing.T) {
	rows := [][]string{
		{"acc1", "users", "t1"},
		{"acc1", "users", "t2"},
		{"acc2", "users", "t1"},
		{"acc2", "users", "t2"},
		{"acc3", "items", "t1"},
		{"acc3", "items", "table1"},
		{"acc3", "items", "table2"},
		{"acc3", "items", "table3"},
		{"sys", "test", "test"},
		{"sys", "test1", "test"},
		{"sys", "test2", "test"},
		{"sys", "test", "t1"},
		{"sys", "test", "t11"},
		{"sys", "test", "t111"},
		{"sys", "test", "t1111"},
	}

	tables := []string{
		"acc1.users.t1",
		"acc1.users.t*",
		"acc*.users.t?",
		"acc*./users|items/./t.*[12]/",
		"acc*.*./table./",
		"acc*.*.table*",
		"/sys|acc.*/.*.t*",
		"/sys|acc.*/.*./t.$/",
		"/sys|acc.*/.test*./t1{1,3}$/,/acc[23]/.items./.*/",
	}

	want := [][]int{
		{0},
		{0, 1},
		{0, 1, 2, 3},
		{0, 1, 2, 3, 4, 5, 6},
		{5, 6, 7},
		{5, 6, 7},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
		{0, 1, 2, 3, 4, 11},
		{4, 5, 6, 7, 11, 12, 13},
	}

	for wantIdx, table := range tables {
		matchedIdx := []int{}
		pts, err := string2patterns(table)
		assert.Equal(t, err, nil)
		for _, pt := range pts {
			for idx := range rows {
				row := rows[idx]
				accountMatched, err := regexp.MatchString(pt.SourceAccount, row[0])
				assert.Equal(t, err, nil)
				databaseMatched, err := regexp.MatchString(pt.SourceDatabase, row[1])
				assert.Equal(t, err, nil)
				tableMatched, err := regexp.MatchString(pt.SourceTable, row[2])
				assert.Equal(t, err, nil)
				if accountMatched && databaseMatched && tableMatched {
					matchedIdx = append(matchedIdx, idx)
				}
			}
		}
		sort.Ints(matchedIdx)
		assert.Equal(t, want[wantIdx], matchedIdx)
	}
}

func Test_privilegeCheck(t *testing.T) {
	var tenantInfo *TenantInfo
	var err error
	var pts []*PatternTuple
	ctx := context.Background()
	ses := &Session{}

	tenantInfo = &TenantInfo{
		Tenant:      sysAccountName,
		DefaultRole: moAdminRoleName,
	}
	ses.tenant = tenantInfo
	pts = []*PatternTuple{
		{SourceAccount: "acc1"},
		{SourceAccount: sysAccountName},
	}
	err = canCreateCdcTask(ctx, ses, "Cluster", "", pts)
	assert.Nil(t, err)

	pts = []*PatternTuple{
		{SourceAccount: sysAccountName, SourceDatabase: moCatalog},
	}
	err = canCreateCdcTask(ctx, ses, "Cluster", "", pts)
	assert.NotNil(t, err)

	pts = []*PatternTuple{
		{SourceAccount: sysAccountName},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", pts)
	assert.NotNil(t, err)

	pts = []*PatternTuple{
		{SourceAccount: "acc2"},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", pts)
	assert.NotNil(t, err)

	pts = []*PatternTuple{
		{},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", pts)
	assert.Nil(t, err)

	pts = []*PatternTuple{
		{SourceAccount: "acc1"},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", pts)
	assert.Nil(t, err)

	tenantInfo = &TenantInfo{
		Tenant:      "acc1",
		DefaultRole: accountAdminRoleName,
	}
	ses.tenant = tenantInfo

	pts = []*PatternTuple{
		{SourceAccount: "acc1"},
		{},
	}
	err = canCreateCdcTask(ctx, ses, "Cluster", "", pts)
	assert.NotNil(t, err)

	err = canCreateCdcTask(ctx, ses, "Account", "acc2", pts)
	assert.NotNil(t, err)

	err = canCreateCdcTask(ctx, ses, "Account", "acc1", pts)
	assert.Nil(t, err)

	pts = []*PatternTuple{
		{SourceAccount: "acc2"},
		{SourceAccount: sysAccountName},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", pts)
	assert.NotNil(t, err)

}
