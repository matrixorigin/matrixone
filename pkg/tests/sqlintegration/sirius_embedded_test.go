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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
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
	// USE, transactions and prepared division must share one public session.
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
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
	t.Run("exact decimal values and metadata", func(t *testing.T) {
		execSQLRequire(t, ctx, db, "create table d (id int not null primary key, a decimal(15,2), b decimal(38,6), c decimal(65,8))")
		execSQLRequire(t, ctx, db, "insert into d values (1,9007199254740.99,9007199254740993.000001,340282366920938463463374607431768211457.12345678),(2,-12.50,-25.000001,-340282366920938463463374607431768211457.12345678),(3,NULL,NULL,NULL),(4,0,0,0)")
		for _, statement := range []string{
			"select id,a,b,c from d order by id",
			"select id,a+a,b-b,c+c,-c,a*b,c/c,c div c,c mod c from d order by id",
			"select id,case when c>0 then c else -c end,coalesce(c,0),c is null,c is not null from d order by id",
			"select sum(a),avg(a),min(a),max(a),sum(b),avg(b),min(b),max(b),sum(c),avg(c),min(c),max(c),count(c) from d",
			"select sum(c),avg(c),min(c),max(c),count(c) from d where id=3",
			"select sum(c),avg(c),min(c),max(c),count(c) from d where id<0",
			"select x.id,x.c,y.c from d x join d y on x.c=y.c order by x.c,x.id,y.id",
			"select c,count(c) from d group by c order by c",
		} {
			t.Run(statement, func(t *testing.T) {
				names, values := readSiriusPublicRows(t, ctx, db, statement)
				gpuNames, gpuValues := readSiriusPublicRows(t, ctx, db, "/*+ SIDECAR GPU */ "+statement)
				require.Equal(t, names, gpuNames)
				require.Equal(t, values, gpuValues)
				var queryID string
				require.NoError(t, db.QueryRowContext(ctx, "select last_query_id()").Scan(&queryID))
				t.Logf("embedded decimal query_id=%s", queryID)
			})
		}
	})
	t.Run("numeric errors and healthy reuse", func(t *testing.T) {
		execSQLRequire(t, ctx, db, "create table numeric_errors (id int not null primary key, v decimal(38,0), c decimal(65,0))")
		execSQLRequire(t, ctx, db, "insert into numeric_errors values (1,99999999999999999999999999999999999999,99999999999999999999999999999999999999999999999999999999999999999),(2,99999999999999999999999999999999999999,99999999999999999999999999999999999999999999999999999999999999999)")
		for _, tc := range []struct {
			statement string
			number    uint16
			state     string
		}{
			{"select v*v from numeric_errors", 1690, "22003"},
			{"select sum(v*v) from numeric_errors", 1690, "22003"},
			{"select sum(c) from numeric_errors", 20301, "HY000"},
		} {
			t.Run(tc.statement, func(t *testing.T) {
				for _, prefix := range []string{"", "/*+ SIDECAR GPU */ "} {
					err := readSiriusPublicError(t, ctx, db, prefix+tc.statement)
					var mysqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &mysqlErr)
					require.Equal(t, tc.number, mysqlErr.Number)
					require.Equal(t, tc.state, string(mysqlErr.SQLState[:]))
				}
				names, values := readSiriusPublicRows(t, ctx, db, "select sum(v) from numeric_errors")
				gpuNames, gpuValues := readSiriusPublicRows(t, ctx, db, "/*+ SIDECAR GPU */ select sum(v) from numeric_errors")
				require.Equal(t, names, gpuNames)
				require.Equal(t, values, gpuValues)
			})
		}
		for _, statement := range []string{
			"select case when id<0 then v*v else v end from numeric_errors order by id",
			"select coalesce(v,v*v) from numeric_errors order by id",
			"select v/0 from numeric_errors order by id",
		} {
			t.Run(statement, func(t *testing.T) {
				names, values := readSiriusPublicRows(t, ctx, db, statement)
				gpuNames, gpuValues := readSiriusPublicRows(t, ctx, db, "/*+ SIDECAR GPU */ "+statement)
				require.Equal(t, names, gpuNames)
				require.Equal(t, values, gpuValues)
			})
		}
	})
	t.Run("prepared division rebinds values and metadata", func(t *testing.T) {
		execSQLRequire(t, ctx, db, "create table division_input (a decimal(10,2), b decimal(10,2))")
		execSQLRequire(t, ctx, db, "insert into division_input values (1,3)")
		var original int
		require.NoError(t, db.QueryRowContext(ctx, "select @@session.div_precision_increment").Scan(&original))
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			execSQLRequire(t, cleanup, db, fmt.Sprintf("set session div_precision_increment=%d", original))
		})
		control, err := db.PrepareContext(ctx, "select a/b as q from division_input")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, control.Close()) })
		embedded, err := db.PrepareContext(ctx, "/*+ SIDECAR GPU */ select a/b as q from division_input")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, embedded.Close()) })
		for _, increment := range []int{0, 4, 10, 30, 4} {
			t.Run(fmt.Sprint(increment), func(t *testing.T) {
				execSQLRequire(t, ctx, db, fmt.Sprintf("set session div_precision_increment=%d", increment))
				rows, err := control.QueryContext(ctx)
				require.NoError(t, err)
				names, values := readSiriusPublicResult(t, rows)
				rows, err = embedded.QueryContext(ctx)
				require.NoError(t, err)
				gpuNames, gpuValues := readSiriusPublicResult(t, rows)
				require.Equal(t, names, gpuNames)
				require.Equal(t, values, gpuValues)
				scale := min(2+increment, 30)
				require.Equal(t, [][]sql.NullString{{{String: "0." + strings.Repeat("3", scale), Valid: true}}}, gpuValues)
				require.Contains(t, gpuNames[0], fmt.Sprintf("decimal=%d,%d/true", 12+increment, scale))
			})
		}
	})
}

func readSiriusPublicError(t *testing.T, ctx context.Context, db *sql.DB, statement string) error {
	t.Helper()
	rows, err := db.QueryContext(ctx, statement)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		// Numeric errors may arrive after the result metadata. Drain to the
		// terminal packet without retaining any rows from a failing query.
	}
	return rows.Err()
}

func readSiriusPublicRows(t *testing.T, ctx context.Context, db *sql.DB, statement string) ([]string, [][]sql.NullString) {
	t.Helper()
	rows, err := db.QueryContext(ctx, statement)
	require.NoError(t, err)
	return readSiriusPublicResult(t, rows)
}

func readSiriusPublicResult(t *testing.T, rows *sql.Rows) ([]string, [][]sql.NullString) {
	t.Helper()
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
