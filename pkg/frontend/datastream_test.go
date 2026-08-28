// Copyright 2026 Matrix Origin
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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestDataStreamCreateStatementLoggingRedactsApiKey(t *testing.T) {
	sql := "create external table t (a int) engine = datastream with " +
		"('server'='127.0.0.1','port'='4444','table'='src','apikey'='top-secret-key')"
	stmt, err := mysql.ParseOne(context.Background(), sql, 1)
	require.NoError(t, err)

	redacted := redactStatementTextForLogging(stmt, sql)
	require.NotContains(t, redacted, "top-secret-key")
	require.Contains(t, redacted, "<redacted>")

	// a datastream table without an apikey still logs cleanly (returns the
	// re-rendered statement, no secret to hide)
	plainSQL := "create external table t (a int) engine = datastream with " +
		"('server'='127.0.0.1','port'='4444','table'='src')"
	plainStmt, err := mysql.ParseOne(context.Background(), plainSQL, 1)
	require.NoError(t, err)
	require.NotContains(t, strings.ToLower(redactStatementTextForLogging(plainStmt, plainSQL)), "apikey")

	// an ordinary CREATE TABLE is passed through verbatim (not re-rendered)
	ordinary := "create table t (a int)"
	ordinaryStmt, err := mysql.ParseOne(context.Background(), ordinary, 1)
	require.NoError(t, err)
	require.Equal(t, ordinary, redactStatementTextForLogging(ordinaryStmt, ordinary))
}
