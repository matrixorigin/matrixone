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
