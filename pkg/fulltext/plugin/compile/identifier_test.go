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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestPopulateSQLQuotesIdentifiers(t *testing.T) {
	table := &plan.TableDef{Name: "table`name", Pkey: &plan.PrimaryKeyDef{PkeyColName: "key`id"}}
	index := &plan.IndexDef{IndexTableName: "index`name", Parts: []string{"tick`col", "order", "body"}}
	statements, err := genInsertSQL(table, index, "db`name")
	require.NoError(t, err)
	require.Len(t, statements, 1)
	require.Contains(t, statements[0], "`db``name`.`index``name`")
	require.Contains(t, statements[0], "`src`.`key``id`, `src`.`tick``col`,`src`.`order`,`src`.`body`")
	parsed, err := parsers.Parse(context.Background(), dialect.MYSQL, statements[0], 1)
	require.NoError(t, err)
	require.Len(t, parsed, 1)
}
