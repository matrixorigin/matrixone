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
	fixture, err := filepath.Abs(fixtureCSV)
	require.NoError(t, err)
	return startJstfuWithConfig(t, func(port int) string {
		jdbcSource := ""
		if moDSNForJdbc != "" {
			jdbcSource = fmt.Sprintf(`,
        { "name": "jdbc_t", "type": "jdbc",
          "connectionstring": "%s",
          "user": "dump", "password": "111",
          "sql": "select col1, col2, col3, col4 from datastream_e2e.src_t where ${FILTER}" }`, moDSNForJdbc)
		}
		return fmt.Sprintf(`{
    "port": %d,
    "datasource": [
        { "name": "file_t", "type": "file", "path": "%s" },
        { "name": "bad_file", "type": "file", "path": "/nonexistent/file.csv" }%s
    ]
}`, port, fixture, jdbcSource)
	})
}

// startJstfuWithConfig launches the jar with an arbitrary config rendered by
// buildConfig for a freshly allocated port.
func startJstfuWithConfig(t *testing.T, buildConfig func(port int) string) (port int) {
	t.Helper()
	if _, err := exec.LookPath("java"); err != nil {
		t.Skip("java not found; skip jstfu e2e")
	}
	jar := filepath.Join(repoRoot(t), "xtool/jstfu/target/jstfu.jar")
	if _, err := os.Stat(jar); err != nil {
		t.Skip("xtool/jstfu/target/jstfu.jar not built; run `make jstfu` first")
	}

	port = freePort(t)
	config := buildConfig(port)
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
	content, _, err := readAllChunks(t, port, table, filter)
	return content, err
}

// readAllChunks reads the full stream, returning the concatenated content and
// the individual chunks as received on the wire.
func readAllChunks(t *testing.T, port int, table, filter string) (string, [][]byte, error) {
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
	var chunks [][]byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return sb.String(), chunks, nil
		}
		if err != nil {
			return sb.String(), chunks, err
		}
		if e := resp.GetError(); e != nil {
			return sb.String(), chunks, fmt.Errorf("server error %s: %s", e.GetCode(), e.GetMessage())
		}
		chunks = append(chunks, resp.GetChunk().GetData())
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

// TestJstfuMultiChunkStreaming forces a large file through a small chunksize
// and verifies real multi-chunk streaming: many chunks on the wire, every
// chunk boundary on a record boundary (even across quoted embedded
// newlines), and lossless reassembly.
func TestJstfuMultiChunkStreaming(t *testing.T) {
	// ~1MB file: every 7th record has a quoted field with embedded newlines
	// and quotes, so chunk alignment has to respect quote state
	dir := t.TempDir()
	bigCSV := filepath.Join(dir, "big.csv")
	var sb strings.Builder
	for i := range 20000 {
		if i%7 == 0 {
			fmt.Fprintf(&sb, "%d,\"multi\nline \\\" value %d,with comma\",tail-%d\n", i, i, i)
		} else {
			fmt.Fprintf(&sb, "%d,plain value %d,tail-%d\n", i, i, i)
		}
	}
	require.NoError(t, os.WriteFile(bigCSV, []byte(sb.String()), 0o644))

	const chunkSize = 2048
	port := startJstfuWithConfig(t, func(port int) string {
		return fmt.Sprintf(`{
    "port": %d,
    "chunksize": %d,
    "datasource": [ { "name": "big", "type": "file", "path": "%s" } ]
}`, port, chunkSize, bigCSV)
	})

	content, chunks, err := readAllChunks(t, port, "big", "")
	require.NoError(t, err)

	// genuinely multi-chunk: ~1MB / 2KB
	require.Greater(t, len(chunks), 100, "expected hundreds of chunks, got %d", len(chunks))
	for i, chunk := range chunks {
		require.NotEmpty(t, chunk, "chunk %d empty", i)
		require.Equal(t, byte('\n'), chunk[len(chunk)-1], "chunk %d does not end on a record boundary", i)
		// record-aligned also means quote-balanced: an odd number of
		// unescaped quotes would mean the boundary split a quoted field
		quotes := 0
		escaped := false
		for _, b := range chunk {
			if escaped {
				escaped = false
				continue
			}
			switch b {
			case '\\':
				escaped = true
			case '"':
				quotes++
			}
		}
		require.Zero(t, quotes%2, "chunk %d splits a quoted field", i)
	}
	require.Equal(t, sb.String(), content, "reassembled stream differs from the file")
}

// TestDatastreamApiKeyAuth proves the full API-key handshake through MO: a
// key-protected server rejects a table with a wrong/absent key and accepts
// the matching one.
func TestDatastreamApiKeyAuth(t *testing.T) {
	db, _ := moConnect(t)
	defer db.Close()

	fixture, err := filepath.Abs(fixtureCSV)
	require.NoError(t, err)
	port := startJstfuWithConfig(t, func(port int) string {
		return fmt.Sprintf(`{
    "port": %d,
    "apikey": "s3cr3t-key",
    "datasource": [ { "name": "file_t", "type": "file", "path": "%s" } ]
}`, port, fixture)
	})

	mustExec(t, db,
		"drop database if exists datastream_auth",
		"create database datastream_auth",
	)
	defer db.Exec("drop database if exists datastream_auth")

	create := func(name, apikeyOpt string) {
		mustExec(t, db, fmt.Sprintf(
			`create external table datastream_auth.%s (col1 int, col2 datetime, col3 varchar(50), col4 text) `+
				`engine = datastream with ('server'='127.0.0.1','port'='%d','table'='file_t'%s)`,
			name, port, apikeyOpt))
	}

	// correct key: scan succeeds
	create("ok", ", 'apikey'='s3cr3t-key'")
	var n int
	require.NoError(t, db.QueryRow("select count(*) from datastream_auth.ok").Scan(&n))
	require.Equal(t, 5, n)

	// wrong key and no key: both rejected with an auth error
	create("wrong", ", 'apikey'='nope'")
	err = db.QueryRow("select count(*) from datastream_auth.wrong").Scan(&n)
	require.Error(t, err)
	require.Contains(t, err.Error(), "authentication failed")

	create("missing", "")
	err = db.QueryRow("select count(*) from datastream_auth.missing").Scan(&n)
	require.Error(t, err)
	require.Contains(t, err.Error(), "authentication failed")

	// SHOW CREATE must not leak the key
	var tbl, ddl string
	require.NoError(t, db.QueryRow("show create table datastream_auth.ok").Scan(&tbl, &ddl))
	require.NotContains(t, ddl, "s3cr3t-key")
	require.NotContains(t, strings.ToLower(ddl), "apikey")
}

// TestDatastreamNoBackslashEscapesSource is the differing-sql_mode negative
// test for filter pushdown: the jdbc source session runs NO_BACKSLASH_ESCAPES,
// under which a backslash-escaped quote would terminate a string literal
// early (the injection the deparser now avoids by ”-doubling). A recheck=false
// scan makes the server authoritative, so a wrong quote form would return wrong
// rows; the correct ”-doubled form must return exactly the matching row.
func TestDatastreamNoBackslashEscapesSource(t *testing.T) {
	db, dsn := moConnect(t)
	defer db.Close()

	hostPort := "127.0.0.1:6001"
	if s := strings.Index(dsn, "tcp("); s >= 0 {
		if e := strings.Index(dsn[s:], ")"); e > 0 {
			hostPort = dsn[s+4 : s+e]
		}
	}
	// the jdbc session forces NO_BACKSLASH_ESCAPES
	jdbcURL := "jdbc:mysql://" + hostPort +
		"?useSSL=false&allowPublicKeyRetrieval=true&sessionVariables=sql_mode=NO_BACKSLASH_ESCAPES"

	mustExec(t, db,
		"drop database if exists datastream_nbs",
		"create database datastream_nbs",
		// source columns share the external table's names so the pushed filter
		// text is valid on the source side (the jdbc-source contract)
		"create table datastream_nbs.src (col1 int, col2 varchar(50))",
		// a value containing an apostrophe and a value with a backslash
		"insert into datastream_nbs.src values (1, 'o''brien'), (2, 'plain'), (3, 'a\\\\b')",
	)
	defer db.Exec("drop database if exists datastream_nbs")

	port := startJstfuWithConfig(t, func(port int) string {
		return fmt.Sprintf(`{
    "port": %d,
    "datasource": [
        { "name": "nbs", "type": "jdbc",
          "connectionstring": "%s",
          "user": "dump", "password": "111",
          "sql": "select col1, col2 from datastream_nbs.src where ${FILTER}" }
    ]
}`, port, jdbcURL)
	})

	// recheck=false: MO trusts the server for pushed conjuncts, so the pushed
	// quote form must be interpreted correctly by the NO_BACKSLASH_ESCAPES source
	mustExec(t, db, fmt.Sprintf(
		`create external table datastream_nbs.ext (col1 int, col2 varchar(50)) `+
			`engine = datastream with ('server'='127.0.0.1','port'='%d','table'='nbs','recheck'='false')`,
		port))

	// apostrophe literal: deparsed as 'o''brien' (doubled), matches row 1 on
	// any sql_mode. The old backslash form 'o\'brien' would break out of the
	// literal on this NO_BACKSLASH_ESCAPES source (backslash-refusal is
	// covered by the deparser unit tests).
	var id int
	require.NoError(t, db.QueryRow(
		"select col1 from datastream_nbs.ext where col2 = 'o''brien'").Scan(&id))
	require.Equal(t, 1, id)

	// a plain predicate still round-trips correctly through the same source
	require.NoError(t, db.QueryRow(
		"select col1 from datastream_nbs.ext where col2 = 'plain'").Scan(&id))
	require.Equal(t, 2, id)
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
	// pushdown-liveness canary: with recheck=false on the file source, a
	// successfully deparsed+pushed conjunct is trimmed locally, so the full
	// file comes back (5).  If deparsing silently broke, the conjunct would
	// stay local and this would return 2 — the jdbc equivalence checks below
	// cannot distinguish those cases, this one can.
	createExternal("ext_file_nr", "file_t", false)
	require.Equal(t, 5, countRows(
		"select count(*) from datastream_e2e.ext_file_nr where col1 > 3"))
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
	var ignored int
	err := db.QueryRow("select count(*) from datastream_e2e.ext_missing").Scan(&ignored)
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
