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

package iscp

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestFulltextCDCWriterQuotesIdentifiers(t *testing.T) {
	table := newTestFulltextTableDef("key`id", types.T_int64, "tick`col", types.T_varchar, 256)
	table.DbName = "db`name"
	table.Indexes[0].IndexTableName = "index`name"
	table.Indexes[0].IndexAlgoParams = `{"parser":"ngram","note":"it'\\works"}`
	writer, err := NewFulltextSqlWriter("fulltext", newTestJobID(), newTestConsumerInfo(), table, table.Indexes)
	require.NoError(t, err)
	require.NoError(t, writer.Upsert(context.Background(), []any{int64(1), []byte("alpha"), nil}))
	result, err := writer.ToSql()
	require.NoError(t, err)
	sql := string(result)
	require.Contains(t, sql, "`db``name`.`index``name`")
	require.Contains(t, sql, "fulltext_index_tokenize("+sqlquote.String(table.Indexes[0].IndexAlgoParams)+", 23, `key``id`, `tick``col`)")
	statements, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
}
