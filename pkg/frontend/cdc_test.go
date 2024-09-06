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
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	cdc2 "github.com/matrixorigin/matrixone/pkg/cdc"
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
		"",
		"",
		"conf path",
		d,
		"running",
		125,
		"xxx",
		"yyy",
	)
	wantSql := "insert into mo_catalog.mo_cdc_task values(3,\"019111fd-aed1-70c0-8760-9abadd8f0f4a\",\"task1\",\"src uri\",\"123\",\"dst uri\",\"mysql\",\"456\",\"ca path\",\"cert path\",\"key path\",\"db1:t1\",\"xfilter\",\"op filters\",\"error\",\"common\",\"\",\"\",\"conf path\",\"2024-08-02 15:20:00\",\"running\",125,\"125\",\"xxx\",\"yyy\",\"\",\"\",\"\",\"\",\"\")"
	assert.Equal(t, wantSql, sql)

	sql2 := getSqlForRetrievingCdcTask(3, id)
	wantSql2 := "select sink_uri, sink_type, sink_password, tables, filters, start_ts from mo_catalog.mo_cdc_task where account_id = 3 and task_id = \"019111fd-aed1-70c0-8760-9abadd8f0f4a\""
	assert.Equal(t, wantSql2, sql2)
}

func Test_parseTables(t *testing.T) {
	//tables := []string{
	//	"acc1.users.t1:acc1.users.t1",
	//	"acc1.users.t*:acc1.users.t*",
	//	"acc*.users.t?:acc*.users.t?",
	//	"acc*.users|items.*[12]/:acc*.users|items.*[12]/",
	//	"acc*.*./table./",
	//	//"acc*.*.table*",
	//	//"/sys|acc.*/.*.t*",
	//	//"/sys|acc.*/.*./t.$/",
	//	//"/sys|acc.*/.test*./t1{1,3}$/,/acc[23]/.items./.*/",
	//}

	type tableInfo struct {
		account, db, table string
		tableIsRegexp      bool
	}

	isSame := func(info tableInfo, account, db, table string, isRegexp bool, tip string) {
		assert.Equalf(t, info.account, account, tip)
		assert.Equalf(t, info.db, db, tip)
		assert.Equalf(t, info.table, table, tip)
		assert.Equalf(t, info.tableIsRegexp, isRegexp, tip)
	}

	type kase struct {
		input   string
		wantErr bool
		src     tableInfo
		dst     tableInfo
	}

	kases := []kase{
		{
			input:   "acc1.users.t1:acc1.users.t1",
			wantErr: false,
			src: tableInfo{
				account: "acc1",
				db:      "users",
				table:   "t1",
			},
			dst: tableInfo{
				account: "acc1",
				db:      "users",
				table:   "t1",
			},
		},
		{
			input:   "acc1.users.t*:acc1.users.t*",
			wantErr: false,
			src: tableInfo{
				account: "acc1",
				db:      "users",
				table:   "t*",
			},
			dst: tableInfo{
				account: "acc1",
				db:      "users",
				table:   "t*",
			},
		},
		{
			input:   "acc*.users.t?:acc*.users.t?",
			wantErr: false,
			src: tableInfo{
				account: "acc*",
				db:      "users",
				table:   "t?",
			},
			dst: tableInfo{
				account: "acc*",
				db:      "users",
				table:   "t?",
			},
		},
		{
			input:   "acc*.users|items.*[12]/:acc*.users|items.*[12]/",
			wantErr: false,
			src: tableInfo{
				account: "acc*",
				db:      "users|items",
				table:   "*[12]/",
			},
			dst: tableInfo{
				account: "acc*",
				db:      "users|items",
				table:   "*[12]/",
			},
		},
		{
			input:   "acc*.*./table./",
			wantErr: true,
			src:     tableInfo{},
			dst:     tableInfo{},
		},
		{
			input:   "acc*.*.table*:acc*.*.table*",
			wantErr: false,
			src: tableInfo{
				account: "acc*",
				db:      "*",
				table:   "table*",
			},
			dst: tableInfo{
				account: "acc*",
				db:      "*",
				table:   "table*",
			},
		},
	}

	for _, tkase := range kases {
		pirs, err := extractTablePairs(context.Background(), tkase.input)
		if tkase.wantErr {
			assert.Errorf(t, err, tkase.input)
		} else {
			assert.NoErrorf(t, err, tkase.input)
			assert.Equal(t, len(pirs.Pts), 1, tkase.input)
			pir := pirs.Pts[0]
			isSame(tkase.src, pir.Source.Account, pir.Source.Database, pir.Source.Table, pir.Source.TableIsRegexp, tkase.input)
			isSame(tkase.dst, pir.Sink.Account, pir.Sink.Database, pir.Sink.Table, pir.Sink.TableIsRegexp, tkase.input)
		}
	}
}

func Test_privilegeCheck(t *testing.T) {
	var tenantInfo *TenantInfo
	var err error
	var pts []*cdc2.PatternTuple
	ctx := context.Background()
	ses := &Session{}

	gen := func(pts []*cdc2.PatternTuple) *cdc2.PatternTuples {
		return &cdc2.PatternTuples{Pts: pts}
	}

	tenantInfo = &TenantInfo{
		Tenant:      sysAccountName,
		DefaultRole: moAdminRoleName,
	}
	ses.tenant = tenantInfo
	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc1"}},
		{Source: cdc2.PatternTable{Account: sysAccountName}},
	}
	err = canCreateCdcTask(ctx, ses, "Cluster", "", gen(pts))
	assert.Nil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: sysAccountName, Database: moCatalog}},
	}
	err = canCreateCdcTask(ctx, ses, "Cluster", "", gen(pts))
	assert.NotNil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: sysAccountName}},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.NotNil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc2"}},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.NotNil(t, err)

	pts = []*cdc2.PatternTuple{
		{},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.Nil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc1"}},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.Nil(t, err)

	tenantInfo = &TenantInfo{
		Tenant:      "acc1",
		DefaultRole: accountAdminRoleName,
	}
	ses.tenant = tenantInfo

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc1"}},
		{},
	}
	err = canCreateCdcTask(ctx, ses, "Cluster", "", gen(pts))
	assert.NotNil(t, err)

	err = canCreateCdcTask(ctx, ses, "Account", "acc2", gen(pts))
	assert.NotNil(t, err)

	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.Nil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc2"}},
		{Source: cdc2.PatternTable{Account: sysAccountName}},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.NotNil(t, err)

}

func Test_extractTableInfo(t *testing.T) {
	type args struct {
		ctx                 context.Context
		input               string
		mustBeConcreteTable bool
	}
	tests := []struct {
		name        string
		args        args
		wantAccount string
		wantDb      string
		wantTable   string
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "t1-1",
			args: args{
				input:               "acc1.db.t1",
				mustBeConcreteTable: true,
			},
			wantAccount: "acc1",
			wantDb:      "db",
			wantTable:   "t1",
			wantErr:     nil,
		},
		{
			name: "t1-2",
			args: args{
				input:               "acc1.db.t*",
				mustBeConcreteTable: true,
			},
			wantAccount: "acc1",
			wantDb:      "db",
			wantTable:   "t*",
			wantErr:     nil,
		},
		{
			name: "t1-3-table pattern needs //",
			args: args{
				input:               "acc1.db.t*",
				mustBeConcreteTable: false,
			},
			wantAccount: "acc1",
			wantDb:      "db",
			wantTable:   "t*",
			wantErr:     nil,
		},
		{
			name: "t1-4",
			args: args{
				input:               "acc1.db./t*/",
				mustBeConcreteTable: false,
			},
			wantAccount: "acc1",
			wantDb:      "db",
			wantTable:   "/t*/",
			wantErr:     nil,
		},
		{
			name: "t2-1",
			args: args{
				input:               "db.t1",
				mustBeConcreteTable: true,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "t1",
			wantErr:     nil,
		},
		{
			name: "t2-2-table pattern needs //",
			args: args{
				input:               "db.t*",
				mustBeConcreteTable: true,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "t*",
			wantErr:     nil,
		},
		{
			name: "t2-3-table name can be 't*'",
			args: args{
				input:               "db.t*",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "t*",
			wantErr:     nil,
		},
		{
			name: "t2-4",
			args: args{
				input:               "db./t*/",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "/t*/",
			wantErr:     nil,
		},
		{
			name: "t2-5",
			args: args{
				input:               "db./t*/",
				mustBeConcreteTable: true,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "/t*/",
			wantErr:     nil,
		},
		{
			name: "t3--invalid format",
			args: args{
				input:               "nodot",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
		{
			name: "t3--invalid account name",
			args: args{
				input:               "1234*90.db.t1",
				mustBeConcreteTable: false,
			},
			wantAccount: "1234*90",
			wantDb:      "db",
			wantTable:   "t1",
			wantErr:     nil,
		},
		{
			name: "t3--invalid database name",
			args: args{
				input:               "acc.12ddg.t1",
				mustBeConcreteTable: false,
			},
			wantAccount: "acc",
			wantDb:      "12ddg",
			wantTable:   "t1",
			wantErr:     nil,
		},
		{
			name: "t4--invalid database name",
			args: args{
				input:               "acc*./users|items/./t.*[12]/",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
		{
			name: "t4-- X ",
			args: args{
				input:               "/sys|acc.*/.*.t*",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
		{
			name: "t4-- XX",
			args: args{
				input:               "/sys|acc.*/.*./t.$/",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
		{
			name: "t4-- XXX",
			args: args{
				input:               "/sys|acc.*/.test*./t1{1,3}$/,/acc[23]/.items./.*/",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAccount, gotDb, gotTable, _, err := extractTableInfo(tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)
			if tt.wantErr != nil && tt.wantErr(t, err, fmt.Sprintf("extractTableInfo(%v, %v, %v)", tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)) {
				return
			} else {
				assert.Equalf(t, tt.wantAccount, gotAccount, "extractTableInfo(%v, %v, %v)", tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)
				assert.Equalf(t, tt.wantDb, gotDb, "extractTableInfo(%v, %v, %v)", tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)
				assert.Equalf(t, tt.wantTable, gotTable, "extractTableInfo(%v, %v, %v)", tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)
			}
		})
	}
}
