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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGetSqlForShowCreatePub(t *testing.T) {
	ctx := context.Background()
	kases := []struct {
		name string
		want string
		err  bool
	}{
		{
			name: "pub",
			want: "select pub_name,database_name,all_account,account_list,comment from mo_catalog.mo_pubs where pub_name='pub';",
			err:  false,
		},
		{
			name: ":pub1",
			want: "",
			err:  true,
		},
	}
	for _, kase := range kases {
		got, err := getSqlForShowCreatePub(ctx, kase.name)
		require.Equal(t, kase.err, err != nil)
		require.Equal(t, kase.want, got)
	}
}

func TestDoShowPublications(t *testing.T) {
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

	sa := &tree.ShowPublications{}
	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
	defer bhStub.Reset()

	bh.sql2result["begin;"] = nil
	bh.sql2result[getPublicationsInfoFormat] = &MysqlResultSet{
		Columns: showPublicationOutputColumns[:],
		Data:    [][]interface{}{{"pub1", "db1"}, {"pub2", "db2"}},
	}
	bh.sql2result["commit;"] = nil
	bh.sql2result["rollback;"] = nil

	err := doShowPublications(ctx, ses, sa)
	require.NoError(t, err)
	rs := ses.GetMysqlResultSet()
	require.Equal(t, rs.GetColumnCount(), uint64(len(showPublicationOutputColumns)))
}

func TestDoShowCreatePublication(t *testing.T) {
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

	sa := &tree.ShowCreatePublications{Name: "pub1"}
	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
	defer bhStub.Reset()

	sql, err := getSqlForShowCreatePub(ctx, sa.Name)
	require.NoError(t, err)
	bh.sql2result[sql] = &MysqlResultSet{
		Data: [][]interface{}{
			{"pub1", "db1", "true", "", "comment111"},
		},
		Columns: []Column{
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "pub_name",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "database_name",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "all_account",
					columnType: defines.MYSQL_TYPE_BOOL,
				},
			},
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "account_list",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "comment",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
		},
		Name2Index: map[string]uint64{
			"pub_name":      0,
			"database_name": 1,
			"all_account":   2,
			"account_list":  3,
			"comment":       4,
		},
	}

	bh.sql2result["begin;"] = nil
	bh.sql2result["commit;"] = nil
	bh.sql2result["rollback;"] = nil

	err = doShowCreatePublications(ctx, ses, sa)
	require.NoError(t, err)
	rs := ses.GetMysqlResultSet()
	require.Equal(t, rs.GetColumnCount(), uint64(len(showPublicationOutputColumns)))
	require.Equal(t, rs.GetRowCount(), uint64(1))
	output := "CREATE PUBLICATION `pub1` DATABASE `db1` ACCOUNT ALL COMMENT 'comment111'"
	got, err := rs.GetString(ctx, 0, 1)
	require.NoError(t, err)
	require.Equal(t, output, got)
}
