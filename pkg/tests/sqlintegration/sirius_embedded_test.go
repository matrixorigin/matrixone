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

//go:build sirius && sirius_integration && cgo && linux && amd64

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// A dedicated native configuration is necessary: the shared CPU fixture must
// never acquire or retain the process-global Sirius GPU runtime.
func TestEmbeddedSiriusPublicMOReader(t *testing.T) {
	config := os.Getenv("MO_SIRIUS_TEST_CONFIG")
	require.NotEmpty(t, config, "sirius_integration requires MO_SIRIUS_TEST_CONFIG")
	config, err := filepath.Abs(config)
	require.NoError(t, err)
	c, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(op embed.ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.Sirius = cnservice.SiriusConfig{Enabled: true, Backend: "embedded", InputMode: "mo", NativeConfigPath: config, GPUStreams: 2}
			})
		}
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	for _, statement := range []string{
		"create database sirius_reader_contract", "use sirius_reader_contract",
		"create table t (n bigint not null primary key, s varchar(80))",
		"insert into t values (0,NULL),(1,'a'),(2,'1234567890123456789012345'),(3,'tail')",
	} {
		execSQLRequire(t, ctx, db, statement)
	}
	t.Cleanup(func() { cleanupSQLIntegration(t, cn, "drop database if exists sirius_reader_contract") })
	compare := func(statement string) {
		t.Helper()
		names, values := readSiriusPublicRows(t, ctx, db, statement)
		gpuNames, gpuValues := readSiriusPublicRows(t, ctx, db, "/*+ SIDECAR GPU */ "+statement)
		require.Equal(t, names, gpuNames)
		require.Equal(t, values, gpuValues)
	}
	compare("select n,s from t order by n")
	compare("select n,s from t where n>0 order by n limit 1 offset 1")
	execSQLRequire(t, ctx, db, "delete from t where n=1")
	compare("select n,s from t order by n")
	// A normal MO reader, unlike the old direct-TAE admission, also carries
	// this transaction's own uncommitted writes and statement offset.
	execSQLRequire(t, ctx, db, "begin")
	execSQLRequire(t, ctx, db, "insert into t values (4,'own write')")
	compare("select n,s from t order by n")
	execSQLRequire(t, ctx, db, "rollback")
	compare("select n,s from t order by n")
}

func readSiriusPublicRows(t *testing.T, ctx context.Context, db *sql.DB, statement string) ([]string, [][]sql.NullString) {
	t.Helper()
	rows, err := db.QueryContext(ctx, statement)
	require.NoError(t, err)
	defer rows.Close()
	names, err := rows.Columns()
	require.NoError(t, err)
	columns, err := rows.ColumnTypes()
	require.NoError(t, err)
	// Include every metadata field the public driver actually exposes in the
	// independent native/embedded comparison, not only headings and values.
	for i, col := range columns {
		nullable, nullableKnown := col.Nullable()
		length, lengthKnown := col.Length()
		precision, scale, decimalKnown := col.DecimalSize()
		names[i] = fmt.Sprintf("%s|%s|nullable=%v/%v|length=%d/%v|decimal=%d,%d/%v",
			names[i], col.DatabaseTypeName(), nullable, nullableKnown, length, lengthKnown, precision, scale, decimalKnown)
	}
	var result [][]sql.NullString
	for rows.Next() {
		row := make([]sql.NullString, len(names))
		dest := make([]any, len(names))
		for i := range row {
			dest[i] = &row[i]
		}
		require.NoError(t, rows.Scan(dest...))
		result = append(result, row)
	}
	require.NoError(t, rows.Err())
	return names, result
}
