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

package pubsub_test

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestPublicationCreateSQLPreservesComments(t *testing.T) {
	for _, comment := range []string{"", "ordinary", "owner's copy", `C:\new\table\`, "owner's \\copy 界", "'; drop database unrelated; --"} {
		t.Run(comment, func(t *testing.T) {
			pub := &pubsub.PubInfo{PubName: "p", DbName: "db", TablesStr: pubsub.TableAll,
				SubAccountsStr: pubsub.AccountAll, Comment: comment}
			statements, err := mysql.Parse(t.Context(), pub.GetCreateSql(), 1)
			require.NoError(t, err)
			t.Cleanup(func() {
				for _, statement := range statements {
					statement.Free()
				}
			})
			require.Len(t, statements, 1)
			create := statements[0].(*tree.CreatePublication)
			require.Equal(t, comment, create.Comment)
		})
	}
}

func TestPublicationCreateSQLPreservesIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		name, publication, database, tables, accounts string
	}{
		{"ordinary", "p", "db", pubsub.TableAll, pubsub.AccountAll},
		{"publication", "review-pub", "db", pubsub.TableAll, pubsub.AccountAll},
		{"database", "p", "review-db", pubsub.TableAll, pubsub.AccountAll},
		{"table-list", "p", "db", "review-items,other-items", pubsub.AccountAll},
		{"account-list", "p", "db", pubsub.TableAll, "review-sub,other-sub"},
		{"whitespace-account-list", "p", "db", pubsub.TableAll, "review-sub, other-sub"},
		{"all-keyword", "p", "db", pubsub.TableAll, "ALL"},
		{"backticks-and-keywords", "review`pub", "review`db", "review`items,select", "review`sub,select"},
		{"statement-looking-name", "p`; drop database unrelated; --", "db", pubsub.TableAll, pubsub.AccountAll},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := &pubsub.PubInfo{PubName: tc.publication, DbName: tc.database,
				TablesStr: tc.tables, SubAccountsStr: tc.accounts, Comment: "owner's copy"}
			statements, err := mysql.Parse(t.Context(), pub.GetCreateSql(), 1)
			require.NoError(t, err)
			t.Cleanup(func() {
				for _, statement := range statements {
					statement.Free()
				}
			})
			require.Len(t, statements, 1)
			create := statements[0].(*tree.CreatePublication)
			require.Equal(t, tc.publication, string(create.Name))
			require.Equal(t, tc.database, string(create.Database))
			require.Equal(t, pub.Comment, create.Comment)
			if tc.tables == pubsub.TableAll {
				require.Empty(t, create.Table)
			} else {
				tables := strings.Split(tc.tables, pubsub.Sep)
				require.Len(t, create.Table, len(tables))
				for i, table := range tables {
					require.Equal(t, table, string(create.Table[i].Name()))
				}
			}
			require.NotNil(t, create.AccountsSet)
			if strings.EqualFold(tc.accounts, pubsub.AccountAll) {
				require.True(t, create.AccountsSet.All)
			} else {
				require.False(t, create.AccountsSet.All)
				accounts := pub.GetSubAccountNames()
				require.Len(t, create.AccountsSet.SetAccounts, len(accounts))
				for i, account := range accounts {
					require.Equal(t, account, string(create.AccountsSet.SetAccounts[i]))
				}
			}
		})
	}
}
