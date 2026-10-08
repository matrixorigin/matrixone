// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

type subscriptionRestoreExec struct {
	*backgroundExecTest
	accounts []uint32
}

func (b *subscriptionRestoreExec) Exec(ctx context.Context, sql string) error {
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	b.accounts = append(b.accounts, account)
	return b.backgroundExecTest.Exec(ctx, sql)
}

func TestRestoreSubscriptionPropagatesErrors(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 10)
	accountSQL, err := getSqlForAccountIdAndStatus(ctx, "publisher", true)
	require.NoError(t, err)
	queries := []string{
		fmt.Sprintf(getSubsSqlFmt, "{MO_TS = 42}") + " and sub_account_id = 10 and sub_name = 'subdb'",
		accountSQL,
		"select 1 from mo_catalog.mo_columns where account_id = 0 and att_database = 'mo_catalog' and att_relname = 'mo_pubs' and attname = 'account_name'",
		fmt.Sprintf(getPubInfoSql, 30) + " and pub_name = 'p'",
		"select datname, '', cast(creator as char), cast(owner as char) from mo_catalog.mo_database {MO_TS = 42} where account_id = 10 and datname = 'subdb'",
		"create database subdb from publisher publication p",
	}
	fixture := func() *subscriptionRestoreExec {
		bh := &backgroundExecTest{}
		bh.init()
		rows := [][][]interface{}{
			{{int64(10), "subscriber", "subdb", "", int64(1), "publisher", "p", "app", "*", "", "", int64(0)}},
			{{int64(30), "open", uint64(0)}},
			{{int64(1)}},
			{{int64(30), "publisher", "p", "app", uint64(300), "*", "subscriber", "", nil, uint64(2), uint64(0), ""}},
			{{"subdb", "", "2", "2"}},
		}
		for i, row := range rows {
			names := make([]string, len(row[0]))
			bh.sql2result[queries[i]] = newMrsForRestoreStringRows(names, row)
		}
		return &subscriptionRestoreExec{backgroundExecTest: bh}
	}
	record := NewSubDbRestoreRecord("subdb", 10, 20, queries[5], 42)
	t.Run("source metadata and target creation", func(t *testing.T) {
		bh := fixture()
		require.NoError(t, restoreToSubDb(ctx, "", bh, "s", record))
		require.Equal(t, queries, bh.executedSQLs)
		require.Equal(t, []uint32{0, 0, 0, 0, 20}, bh.accounts)
	})
	for _, failure := range []error{errors.New("catalog read failed"), context.Canceled, context.DeadlineExceeded} {
		for i, query := range queries {
			t.Run(fmt.Sprintf("step %d %v", i, failure), func(t *testing.T) {
				bh := fixture()
				bh.sql2err[query] = failure
				require.ErrorIs(t, restoreToSubDb(ctx, "", bh, "s", record), failure)
				require.Equal(t, queries[:i+1], bh.executedSQLs)
			})
		}
	}
	for _, i := range []int{1, 3} {
		t.Run(fmt.Sprintf("missing publication dependency %d", i), func(t *testing.T) {
			bh := fixture()
			bh.sql2result[queries[i]] = newMrsForRestoreStringRows(nil, nil)
			exists, err := checkPubValid(ctx, "", bh, "publisher", "p")
			require.NoError(t, err)
			require.False(t, exists)
			bh.executedSQLs = nil
			require.NoError(t, restoreToSubDb(ctx, "", bh, "s", record))
			require.NotContains(t, bh.executedSQLs, queries[5])
		})
	}
	t.Run("missing snapshot subscription metadata is not absent publication", func(t *testing.T) {
		bh := fixture()
		bh.sql2result[queries[0]] = newMrsForRestoreStringRows(nil, nil)
		require.ErrorContains(t, restoreToSubDb(ctx, "", bh, "s", record), "there is no subscription")
		require.Equal(t, queries[:1], bh.executedSQLs)
	})
}

func TestRestorePublicationsRejectsMissingTarget(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[getAccountIdNamesSql] = newMrsForRestoreStringRows(nil, nil)
	pub := &pubsub.PubInfo{PubAccountId: 1, PubAccountName: "publisher", PubName: "p"}
	require.ErrorContains(t, createPubs(t.Context(), "", bh, "s", []*pubsub.PubInfo{pub}), "target account publisher does not exist")
	require.Equal(t, []string{getAccountIdNamesSql}, bh.executedSQLs)
	failure := errors.New("account lookup failed")
	bh.sql2err[getAccountIdNamesSql] = failure
	require.ErrorIs(t, createPubs(t.Context(), "", bh, "s", []*pubsub.PubInfo{pub}), failure)
}
