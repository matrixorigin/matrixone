// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDataStreamCreateExternalTable(t *testing.T) {
	ctx := context.Background()

	sql := "create external table t (col1 int, col2 timestamp, col3 varchar(100), col4 text) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'src_t', 'recheck' = 'true')"
	stmt, err := ParseOne(ctx, sql, 1)
	require.NoError(t, err)
	ct, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	require.NotNil(t, ct.DataStreamParam)
	require.Len(t, ct.DataStreamParam.Options, 4)
	require.Equal(t, "server", string(ct.DataStreamParam.Options[0].Key))
	require.Equal(t, "127.0.0.1", ct.DataStreamParam.Options[0].Val)
	require.Equal(t, "recheck", string(ct.DataStreamParam.Options[3].Key))

	// format round-trip must reparse identically
	formatted := tree.String(stmt, dialect.MYSQL)
	require.Contains(t, formatted, "engine = datastream with (")
	stmt2, err := ParseOne(ctx, formatted, 1)
	require.NoError(t, err, formatted)
	require.Equal(t, formatted, tree.String(stmt2, dialect.MYSQL))

	// no options form parses too (rejected later at DDL build time)
	stmt3, err := ParseOne(ctx, "create external table t (a int) engine = datastream", 1)
	require.NoError(t, err)
	require.NotNil(t, stmt3.(*tree.CreateTable).DataStreamParam)
	require.Nil(t, stmt3.(*tree.CreateTable).DataStreamParam.Options)

	// datastream stays usable as a regular identifier
	_, err = ParseOne(ctx, "create table datastream (datastream int)", 1)
	require.NoError(t, err)
}
