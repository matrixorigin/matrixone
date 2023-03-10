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
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_getSqlForAccountInfo(t *testing.T) {
	type arg struct {
		s    string
		want string
	}
	args := []arg{
		{
			s:    "show accounts;",
			want: "select account_id as `account_id`, account_name as `account_name`, created_time as `created`, status as `status`, suspended_time as `suspended_time`, comments as `comment` from mo_catalog.mo_account ;",
		},
		{
			s:    "show accounts like '%abc';",
			want: "select account_id as `account_id`, account_name as `account_name`, created_time as `created`, status as `status`, suspended_time as `suspended_time`, comments as `comment` from mo_catalog.mo_account where account_name like '%abc';",
		},
	}

	for _, a := range args {
		one, err := parsers.ParseOne(context.Background(), dialect.MYSQL, a.s, 1)
		assert.NoError(t, err)
		sa1 := one.(*tree.ShowAccounts)
		r1 := getSqlForAllAccountInfo(sa1.Like)
		assert.Equal(t, a.want, r1)
	}
}

func getColumnDef(name string, typ defines.MysqlType) Column {
	return &MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       name,
			columnType: typ,
		},
	}
}

func newAccountInfo() *MysqlResultSet {
	rsFromMoAccout := &MysqlResultSet{}
	rsFromMoAccout.AddColumn(getColumnDef("account_id", defines.MYSQL_TYPE_LONG))
	rsFromMoAccout.AddColumn(getColumnDef("account_name", defines.MYSQL_TYPE_VARCHAR))
	rsFromMoAccout.AddColumn(getColumnDef("created", defines.MYSQL_TYPE_TIMESTAMP))
	rsFromMoAccout.AddColumn(getColumnDef("status", defines.MYSQL_TYPE_VARCHAR))
	rsFromMoAccout.AddColumn(getColumnDef("suspended_time", defines.MYSQL_TYPE_TIMESTAMP))
	rsFromMoAccout.AddColumn(getColumnDef("comment", defines.MYSQL_TYPE_VARCHAR))
	rsFromMoAccout.AddRow(make([]interface{}, rsFromMoAccout.GetColumnCount()))
	return rsFromMoAccout
}

func newTableStatsResult() *MysqlResultSet {
	rs1 := &MysqlResultSet{}
	rs1.AddColumn(getColumnDef("admin_name", defines.MYSQL_TYPE_VARCHAR))
	rs1.AddColumn(getColumnDef("db_count", defines.MYSQL_TYPE_LONG))
	rs1.AddColumn(getColumnDef("table_count", defines.MYSQL_TYPE_LONG))
	rs1.AddColumn(getColumnDef("row_count", defines.MYSQL_TYPE_LONG))
	rs1.AddColumn(getColumnDef("size", defines.MYSQL_TYPE_DECIMAL))
	rs1.AddRow(make([]interface{}, rs1.GetColumnCount()))
	return rs1
}

func Test_mergeResult(t *testing.T) {
	rsFromMoAccount := newAccountInfo()
	rs1 := newTableStatsResult()
	ans1 := &MysqlResultSet{}
	err := mergeOutputResult(context.Background(), ans1, rsFromMoAccount, []*MysqlResultSet{rs1})
	assert.NoError(t, err)
	assert.Equal(t, ans1.GetColumnCount(), uint64(finalColumnCount))
}

func Test_tableStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	sql2result := make(map[string]ExecResult)

	sql := getSqlForTableStats(10)
	sql2result[sql] = newTableStatsResult()

	bh := newBh(ctrl, sql2result)

	rs, err := getTableStats(ctx, bh, 10)
	assert.NoError(t, err)
	assert.NotNil(t, rs)
}

func TestGetAccountInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	sql2result := make(map[string]ExecResult)
	sql := getSqlForAccountInfo(10)
	info := newAccountInfo()
	info.Data[0][idxOfAccountId] = 10
	sql2result[sql] = info

	bh := newBh(ctrl, sql2result)

	rs, ids, err := getAccountInfo(ctx, bh, sql, true)
	assert.NoError(t, err)
	assert.NotNil(t, rs)
	assert.Equal(t, ids[0], uint64(10))
}

func TestDoShowAccounts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Dispose()

	tenant := &TenantInfo{
		Tenant:   sysAccountName,
		TenantID: sysAccountID,
	}
	ses.SetTenantInfo(tenant)

	sa := &tree.ShowAccounts{}
	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
	defer bhStub.Reset()

	bh.sql2result["begin;"] = nil
	bh.sql2result["commit;"] = nil
	bh.sql2result["rollback;"] = nil

	sql := getSqlForAllAccountInfo(nil)
	info := newAccountInfo()
	info.Data[0][idxOfAccountId] = 10
	bh.sql2result[sql] = info

	sql = getSqlForTableStats(10)
	bh.sql2result[sql] = newTableStatsResult()

	err := doShowAccounts(ctx, ses, sa)
	assert.NoError(t, err)
	rs := ses.GetMysqlResultSet()
	assert.Equal(t, rs.GetColumnCount(), uint64(finalColumnCount))
}
