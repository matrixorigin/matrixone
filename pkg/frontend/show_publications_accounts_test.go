// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestShowPublicationsAccountResolution(t *testing.T) {
	queryErr := errors.New("catalog read failed")
	for _, tc := range []struct {
		name       string
		subs       []int64
		want       string
		failQuery  int
		queryError error
	}{
		{name: "healthy_sorted", subs: []int64{2, 1}, want: "alpha,zeta"},
		{name: "no_subscribers"},
		{name: "missing_only", subs: []int64{99}},
		{name: "mixed", subs: []int64{2, 99, 1}, want: "alpha,zeta"},
		{name: "publication_read_error", failQuery: 0, queryError: queryErr},
		{name: "account_read_error", failQuery: 1, queryError: queryErr},
		{name: "subscription_read_error", failQuery: 2, queryError: queryErr},
		{name: "subscription_canceled", failQuery: 2, queryError: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newSes(nil, ctrl)
			t.Cleanup(ses.Close)
			ctx := defines.AttachAccount(t.Context(), sysAccountID, rootID, moAdminRoleID)
			const timestamp = "2026-01-01 00:00:00"
			pubs := &MysqlResultSet{Columns: make([]Column, 12), Data: [][]interface{}{
				{int64(0), "sys", "affected", "db1", uint64(1), "*", "all", timestamp, nil, uint64(0), uint64(0), "keep comment"},
				{int64(0), "sys", "healthy", "db2", uint64(2), "t", "alpha", timestamp, timestamp, uint64(0), uint64(0), ""},
			}}
			accounts := &MysqlResultSet{Columns: make([]Column, 5), Data: [][]interface{}{
				{int64(1), "alpha", "open", uint64(1), nil},
				{int64(2), "zeta", "open", uint64(1), nil},
				// A recreated account must not inherit the missing ID's subscription.
				{int64(100), "reused_name", "open", uint64(1), nil},
			}}
			subs := &MysqlResultSet{Columns: make([]Column, 12)}
			for _, id := range tc.subs {
				subs.AddRow([]interface{}{id, "reused_name", "sub_db", timestamp, int64(0), "sys", "affected", "db1", "*", timestamp, "", int64(1)})
			}
			healthy := &MysqlResultSet{Columns: make([]Column, 12), Data: [][]interface{}{
				{int64(1), "alpha", "sub_other", timestamp, int64(0), "sys", "healthy", "db2", "t", timestamp, "", int64(0)},
			}}
			queries := []string{
				fmt.Sprintf(getPubInfoSql, sysAccountID) + " order by update_time desc, created_time desc",
				getAccountIdNamesSql,
				getSubsSql + " and pub_account_name = 'sys' and pub_name = 'affected' and sub_name is not null",
				getSubsSql + " and pub_account_name = 'sys' and pub_name = 'healthy' and sub_name is not null",
			}
			results := []*MysqlResultSet{pubs, accounts, subs, healthy}
			bh := mock_frontend.NewMockBackgroundExec(ctrl)
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			t.Cleanup(stub.Reset)
			bh.EXPECT().Close()
			var calls []*gomock.Call
			for i, query := range queries {
				calls = append(calls, bh.EXPECT().ClearExecResultSet())
				if tc.queryError != nil && i == tc.failQuery {
					calls = append(calls, bh.EXPECT().Exec(gomock.Any(), query).Return(tc.queryError))
					break
				}
				calls = append(calls,
					bh.EXPECT().Exec(gomock.Any(), query).Return(nil),
					bh.EXPECT().GetExecResultSet().Return([]interface{}{results[i]}),
				)
			}
			// Only these reads are allowed; unexpected cleanup writes fail the test.
			gomock.InOrder(calls...)
			err := doShowPublications(ctx, ses, &tree.ShowPublications{})
			if tc.queryError != nil {
				require.ErrorIs(t, err, tc.queryError)
				return
			}
			require.NoError(t, err)
			require.Len(t, ses.mrs.Columns, 8)
			require.Equal(t, [][]interface{}{
				{"affected", "db1", "*", "*", tc.want, timestamp, nil, "keep comment"},
				{"healthy", "db2", "t", "alpha", "alpha", timestamp, timestamp, ""},
			}, ses.mrs.Data)
		})
	}
}
