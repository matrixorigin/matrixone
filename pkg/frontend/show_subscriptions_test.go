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

func TestDoShowSubscriptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)

	sa := &tree.ShowSubscriptions{}
	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
	defer bhStub.Reset()

	sql, err := getSqlForShowSubscriptions(ctx, tenant.TenantID)
	require.NoError(t, err)
	bh.sql2result[sql] = &MysqlResultSet{
		Data: [][]interface{}{
			{"sub1", "create database sub1 from acc0 publication p1"},
		},
		Columns: []Column{
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "datname",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "dat_createsql",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
		},
		Name2Index: map[string]uint64{
			"datname":       0,
			"dat_createsql": 1,
		},
	}

	bh.sql2result["begin;"] = nil
	bh.sql2result["commit;"] = nil
	bh.sql2result["rollback;"] = nil

	err = doShowSubscriptions(ctx, ses, sa)
	require.NoError(t, err)
	rs := ses.GetMysqlResultSet()
	require.Equal(t, rs.GetColumnCount(), uint64(len(showPublicationOutputColumns)))
	require.Equal(t, rs.GetRowCount(), uint64(1))
	output := []string{"sub1", "acc0"}
	got, err := rs.GetString(ctx, 0, 0)
	require.NoError(t, err)
	require.Equal(t, output[0], got)
	got, err = rs.GetString(ctx, 0, 1)
	require.NoError(t, err)
	require.Equal(t, output[1], got)
}
