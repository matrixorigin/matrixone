// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestPublicationStatementForReplayPreservesAuthorization(t *testing.T) {
	for _, tables := range []string{pubsub.TableAll, "", "review-items,other-items"} {
		for _, accounts := range []string{"", pubsub.AccountAll, "review-sub,other-sub"} {
			t.Run(tables+"/accounts="+accounts, func(t *testing.T) {
				pub := &pubsub.PubInfo{PubName: "review-pub", DbName: "review-db", TablesStr: tables,
					SubAccountsStr: accounts, Comment: "owner's C:\\copy\\"}
				original := *pub
				stmt, err := publicationStatementForReplay(t.Context(), pub)
				require.NoError(t, err)
				t.Cleanup(stmt.Free)
				require.Equal(t, pub.PubName, string(stmt.Name))
				require.Equal(t, pub.DbName, string(stmt.Database))
				require.Equal(t, pub.Comment, stmt.Comment)
				require.False(t, stmt.IfNotExists)
				if tables == pubsub.TableAll || tables == "" {
					require.Empty(t, stmt.Table)
				} else {
					require.Len(t, stmt.Table, 2)
					require.Equal(t, "review-items", string(stmt.Table[0].Name()))
					require.Equal(t, "other-items", string(stmt.Table[1].Name()))
				}
				require.NotNil(t, stmt.AccountsSet)
				require.Equal(t, accounts == pubsub.AccountAll, stmt.AccountsSet.All)
				if accounts == "" || accounts == pubsub.AccountAll {
					require.Empty(t, stmt.AccountsSet.SetAccounts)
				} else {
					require.Len(t, stmt.AccountsSet.SetAccounts, 2)
					require.Equal(t, "review-sub", string(stmt.AccountsSet.SetAccounts[0]))
					require.Equal(t, "other-sub", string(stmt.AccountsSet.SetAccounts[1]))
				}
				require.Equal(t, original, *pub, "replay must not rewrite historical authorization metadata")
			})
		}
	}
}

func TestCreatePublicationFromRestoreInfoPropagatesError(t *testing.T) {
	for _, accounts := range []string{"", pubsub.AccountAll} {
		t.Run("accounts="+accounts, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			failure := errors.New("account metadata unavailable")
			bh.sql2err[getAccountIdNamesSql] = failure
			pub := &pubsub.PubInfo{PubName: "p", DbName: "db", TablesStr: pubsub.TableAll, SubAccountsStr: accounts}
			ctx := defines.AttachAccountId(t.Context(), sysAccountID)
			require.ErrorIs(t, createPublicationFromRestoreInfo(ctx, bh, pub), failure)
			require.Equal(t, []string{getAccountIdNamesSql}, bh.executedSQLs, "replay must abort before any catalog write")
		})
	}
}

func TestCreatePublicationReplayRetainsTableSelection(t *testing.T) {
	for _, tables := range []string{pubsub.TableAll, "", "t1"} {
		t.Run("tables="+tables, func(t *testing.T) {
			bh, ctx, _ := newCreatePublicationIfNotExistsTest(t, false, false)
			pub := &pubsub.PubInfo{PubName: "pub1", DbName: "db1", TablesStr: tables, SubAccountsStr: pubsub.AccountAll}
			want, err := getSqlForInsertIntoMoPubs(ctx, sysAccountID, sysAccountName, "pub1", "db1", 7,
				tables == pubsub.TableAll, tables, pubsub.AccountAll, "", true)
			require.NoError(t, err)
			insertion := errors.New("publication insertion reached")
			bh.sql2err[want] = insertion
			require.ErrorIs(t, createPublicationFromRestoreInfo(ctx, bh, pub), insertion)
			require.Equal(t, want, bh.executedSQLs[len(bh.executedSQLs)-1])
			for _, sql := range bh.executedSQLs {
				require.False(t, strings.Contains(sql, "mo_subs"), "failed insertion must not change subscriptions")
			}
		})
	}
}
