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

func TestKafkaCreateExternalTable(t *testing.T) {
	ctx := context.Background()

	sql := "create external table kt (a int, b varchar(10)) engine = kafka with ('brokers'='localhost:9092','topic'='t1','partition'='0','autocommit'='false','format'='csv','separator'='|')"
	stmt, err := ParseOne(ctx, sql, 1)
	require.NoError(t, err)
	ct, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	require.NotNil(t, ct.KafkaParam)
	require.Len(t, ct.KafkaParam.Options, 6)
	require.Equal(t, "brokers", string(ct.KafkaParam.Options[0].Key))
	require.Equal(t, "localhost:9092", ct.KafkaParam.Options[0].Val)

	// the formatted statement re-parses
	formatted := tree.String(stmt, dialect.MYSQL)
	require.Contains(t, formatted, "engine = kafka with (")
	_, err = ParseOne(ctx, formatted, 1)
	require.NoError(t, err, formatted)

	// optionless forms, with and without '='
	for _, s := range []string{
		"create external table kt2 (a int) engine = kafka",
		"create external table kt2 (a int) engine kafka",
	} {
		st, err := ParseOne(ctx, s, 1)
		require.NoError(t, err, s)
		require.NotNil(t, st.(*tree.CreateTable).KafkaParam)
		require.Nil(t, st.(*tree.CreateTable).KafkaParam.Options)
	}

	// ordinary tables keep ENGINE = kafka as a plain option, and kafka stays
	// usable as an identifier
	stmtPlain, err := ParseOne(ctx, "create table t (a int) engine = kafka", 1)
	require.NoError(t, err)
	require.Nil(t, stmtPlain.(*tree.CreateTable).KafkaParam)
	_, err = ParseOne(ctx, "create table kafka (kafka int)", 1)
	require.NoError(t, err)
	_, err = ParseOne(ctx, "select kafka from t", 1)
	require.NoError(t, err)

	// a secret-shaped option key is redacted on re-render
	stmtSec, err := ParseOne(ctx, "create external table kt3 (a int) engine = kafka with ('brokers'='h:9092','topic'='t','sasl_password'='hunter2')", 1)
	require.NoError(t, err)
	fSec := tree.String(stmtSec, dialect.MYSQL)
	require.NotContains(t, fSec, "hunter2")
	require.Contains(t, fSec, "<redacted>")
}
