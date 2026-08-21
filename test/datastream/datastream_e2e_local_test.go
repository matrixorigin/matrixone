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

// Package datastream_test is the local end-to-end harness for datastream
// external tables.  It starts the jstfu Java server itself (skipping when
// java or the jar is unavailable), so unlike BVT it needs no externally
// managed process.  The optional MO-side test additionally needs a running
// MatrixOne (MO_DATASTREAM_E2E_DSN, default dump:111@tcp(127.0.0.1:6001)/).
//
// Build the jar first:  make jstfu   (or MVN=... make jstfu)
package datastream_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	datastream "github.com/matrixorigin/matrixone/pkg/datastream/v1"
)

const fixtureCSV = "../distributed/resources/datastream/numbers.csv"

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	return filepath.Dir(filepath.Dir(wd))
}

func freePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port
}

// startJstfu launches the jar with a config covering a file datasource over
// the fixture CSV and (optionally) a jdbc datasource pointing at MO.
func startJstfu(t *testing.T, moDSNForJdbc string) (port int) {
	t.Helper()
	if _, err := exec.LookPath("java"); err != nil {
		t.Skip("java not found; skip jstfu e2e")
	}
	jar := filepath.Join(repoRoot(t), "xtool/jstfu/target/jstfu.jar")
	if _, err := os.Stat(jar); err != nil {
		t.Skip("xtool/jstfu/target/jstfu.jar not built; run `make jstfu` first")
	}
	fixture, err := filepath.Abs(fixtureCSV)
	require.NoError(t, err)

	port = freePort(t)
	jdbcSource := ""
	if moDSNForJdbc != "" {
		jdbcSource = fmt.Sprintf(`,
        { "name": "jdbc_t", "type": "jdbc",
          "connectionstring": "%s",
          "user": "dump", "password": "111",
          "sql": "select col1, col2, col3, col4 from datastream_e2e.src_t where ${FILTER}" }`, moDSNForJdbc)
	}
	config := fmt.Sprintf(`{
    "port": %d,
    "datasource": [
        { "name": "file_t", "type": "file", "path": "%s" },
        { "name": "bad_file", "type": "file", "path": "/nonexistent/file.csv" }%s
    ]
}`, port, fixture, jdbcSource)
	configPath := filepath.Join(t.TempDir(), "jstfu.json")
	require.NoError(t, os.WriteFile(configPath, []byte(config), 0o644))

	cmd := exec.Command("java", "-jar", jar, configPath)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	// wait until the port answers
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err == nil {
			conn.Close()
			return port
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("jstfu did not start listening in time")
	return 0
}

func readAll(t *testing.T, port int, table, filter string) (string, error) {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := datastream.NewDataStreamClient(conn).Read(ctx, &datastream.ReadRequest{Table: table, Filter: filter})
	require.NoError(t, err)
	var sb strings.Builder
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return sb.String(), nil
		}
		if err != nil {
			return sb.String(), err
		}
		if e := resp.GetError(); e != nil {
			return sb.String(), fmt.Errorf("server error %s: %s", e.GetCode(), e.GetMessage())
		}
		sb.Write(resp.GetChunk().GetData())
	}
}

func TestJstfuFileSource(t *testing.T) {
	port := startJstfu(t, "")

	fixture, err := os.ReadFile(fixtureCSV)
	require.NoError(t, err)

	// file source returns the file verbatim; the filter is a noop
	for _, filter := range []string{"", "(`col1` > 3)"} {
		got, err := readAll(t, port, "file_t", filter)
		require.NoError(t, err)
		require.Equal(t, string(fixture), got)
	}
}

func TestJstfuErrors(t *testing.T) {
	port := startJstfu(t, "")

	_, err := readAll(t, port, "no_such_table", "")
	require.ErrorContains(t, err, "ERROR_TABLE_NOT_FOUND")

	_, err = readAll(t, port, "bad_file", "")
	require.ErrorContains(t, err, "ERROR_DATASOURCE_ERROR")
}

// moConnect returns a DB handle to the MO under test, skipping when MO is
// not reachable.
func moConnect(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("MO_DATASTREAM_E2E_DSN")
	if dsn == "" {
		dsn = "dump:111@tcp(127.0.0.1:6001)/"
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	db.SetConnMaxLifetime(time.Minute)
	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("MatrixOne not reachable at %s: %v", dsn, err)
	}
	return db, dsn
}

// jdbcURLFromDSN converts a go-sql-driver DSN into the JDBC url jstfu needs.
func jdbcURLFromDSN(dsn string) string {
	// dump:111@tcp(127.0.0.1:6001)/  ->  jdbc:mysql://127.0.0.1:6001
	start := strings.Index(dsn, "tcp(")
	end := strings.Index(dsn, ")")
	hostPort := "127.0.0.1:6001"
	if start >= 0 && end > start {
		hostPort = dsn[start+4 : end]
	}
	return "jdbc:mysql://" + hostPort + "?useSSL=false&allowPublicKeyRetrieval=true"
}

func mustExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, stmt := range stmts {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
}

func TestDatastreamThroughMatrixOne(t *testing.T) {
	db, dsn := moConnect(t)
	defer db.Close()
	port := startJstfu(t, jdbcURLFromDSN(dsn))

	mustExec(t, db,
		"drop database if exists datastream_e2e",
		"create database datastream_e2e",
		// the source columns share the external table's column names so the
		// pushed ${FILTER} text stays valid on the source side (the documented
		// usage contract for jdbc datasources)
		`create table datastream_e2e.src_t (col1 int, col2 datetime, col3 varchar(50), col4 text)`,
		`insert into datastream_e2e.src_t values
		   (1,'2020-01-01 10:00:00','alpha','first row'),
		   (2,'2020-06-15 12:30:00','beta','second, with comma'),
		   (3,'2021-03-10 08:45:00','gamma',NULL),
		   (4,'2021-11-11 11:11:11','delta','fourth row'),
		   (5,'2022-07-04 00:00:00','epsilon','fifth row')`,
	)
	defer db.Exec("drop database if exists datastream_e2e")

	createExternal := func(name, source string, recheck bool) {
		mustExec(t, db, fmt.Sprintf(
			`create external table datastream_e2e.%s (
			   col1 int, col2 datetime, col3 varchar(50), col4 text
			 ) engine = datastream with (
			   'server' = '127.0.0.1', 'port' = '%d', 'table' = '%s', 'recheck' = '%t')`,
			name, port, source, recheck))
	}
	createExternal("ext_file", "file_t", true)
	createExternal("ext_jdbc", "jdbc_t", true)
	createExternal("ext_jdbc_norecheck", "jdbc_t", false)

	countRows := func(query string) int {
		var n int
		require.NoError(t, db.QueryRow(query).Scan(&n))
		return n
	}

	// plain scans
	require.Equal(t, 5, countRows("select count(*) from datastream_e2e.ext_file"))
	require.Equal(t, 5, countRows("select count(*) from datastream_e2e.ext_jdbc"))

	// values and NULL round-trip through the file source
	var name string
	var note sql.NullString
	require.NoError(t, db.QueryRow(
		"select col3, col4 from datastream_e2e.ext_file where col1 = 3").Scan(&name, &note))
	require.Equal(t, "gamma", name)
	require.False(t, note.Valid)

	// filter pushdown: the file source ignores the hint, recheck repairs it
	require.Equal(t, 2, countRows(
		"select count(*) from datastream_e2e.ext_file where col2 > '2021-01-01 00:00:00' and col1 < 5"))
	// jdbc applies ${FILTER} server-side; both recheck settings agree
	require.Equal(t, 3, countRows(
		"select count(*) from datastream_e2e.ext_jdbc where col2 > '2021-01-01 00:00:00'"))
	require.Equal(t, 3, countRows(
		"select count(*) from datastream_e2e.ext_jdbc_norecheck where col2 > '2021-01-01 00:00:00'"))

	// show create round-trips
	var tbl, ddl string
	require.NoError(t, db.QueryRow("show create table datastream_e2e.ext_file").Scan(&tbl, &ddl))
	require.Contains(t, ddl, "ENGINE = DATASTREAM WITH (")

	// error surfaces: unknown datasource
	createExternal("ext_missing", "no_such_source", true)
	_, err := db.Query("select count(*) from datastream_e2e.ext_missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no datasource named")

	// ETL: stream into a destination table
	mustExec(t, db,
		"create table datastream_e2e.dest (col1 int, col2 datetime, col3 varchar(50), col4 text)",
		"insert into datastream_e2e.dest select * from datastream_e2e.ext_jdbc",
	)
	require.Equal(t, 5, countRows("select count(*) from datastream_e2e.dest"))

	// parallel ETL with disjoint filters
	mustExec(t, db, "create table datastream_e2e.dest2 (col1 int, col2 datetime, col3 varchar(50), col4 text)")
	errCh := make(chan error, 2)
	for _, cond := range []string{"col1 <= 2", "col1 > 2"} {
		go func(cond string) {
			_, err := db.Exec(fmt.Sprintf(
				"insert into datastream_e2e.dest2 select * from datastream_e2e.ext_jdbc where %s", cond))
			errCh <- err
		}(cond)
	}
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)
	require.Equal(t, 5, countRows("select count(*) from datastream_e2e.dest2"))
}
