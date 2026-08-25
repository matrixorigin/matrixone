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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestMongoDBSQLSurfaceAndRedaction(t *testing.T) {
	ctx := context.Background()
	sqls := []string{
		"create mongodb connection if not exists sensor with ('hosts'='mongo:27017','credential_secret_ref'='secret://env/MONGO','tls_mode'='disabled')",
		"create mongodb connection if not exists sensor with ('hosts'='127.0.0.1:27017','replica_set'='rs0','auth_source'='mongodb_source','auth_mechanism'='SCRAM-SHA-256','credential_secret_ref'='secret://env/MONGO','tls_mode'='disabled','read_preference'='primary','read_concern'='majority','options_json'='{\"direct\":true}')",
		"alter mongodb connection sensor set ('credential_secret_ref'='secret://env/MONGO_NEXT')",
		"alter mongodb connection sensor disable",
		"alter mongodb connection sensor enable",
		"drop mongodb connection if exists sensor",
		"show mongodb connections",
		"create external table events(ts datetime mongodb_path 'ts', measurement double mongodb_path 'payload.measurement' mongodb_convert 'try_null') engine=mongodb with ('connection'='sensor', 'database'='telemetry', 'collection'='events', 'max_parallelism'='1')",
	}
	for _, sql := range sqls {
		for _, lowerCaseTableNames := range []int64{0, 1, 2} {
			stmt, err := ParseOne(ctx, sql, lowerCaseTableNames)
			require.NoError(t, err, sql)
			formatted := tree.String(stmt, dialect.MYSQL)
			_, err = ParseOne(ctx, formatted, lowerCaseTableNames)
			require.NoError(t, err, formatted)
			require.NotContains(t, strings.ToLower(formatted), "mongo_next")
			require.NotContains(t, strings.ToLower(formatted), "mongo:27017")
		}
	}
}

func TestGapFillParserFormatRoundTrip(t *testing.T) {
	ctx := context.Background()
	sql := "select _wstart, id, sum(v) from t group by id interval(ts, 1, minute) gapfill(partition) fill(null)"
	stmt, err := ParseOne(ctx, sql, 1)
	require.NoError(t, err)
	formatted := tree.String(stmt, dialect.MYSQL)
	require.Contains(t, formatted, "gapfill(partition)")
	require.Contains(t, formatted, "fill(null)")
	_, err = ParseOne(ctx, formatted, 1)
	require.NoError(t, err)
}
