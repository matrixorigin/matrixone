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

package foreigntvf

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"io"

	// Registered database/sql drivers for sql_tvf. Oracle and SQL Server are
	// deferred; adding one is a blank import plus a driverAliases entry.
	_ "github.com/go-sql-driver/mysql" // driver name: "mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // driver name: "pgx"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// driverAliases maps the user-facing driver name in the sql_tvf config to the
// registered database/sql driver name.
var driverAliases = map[string]string{
	"mysql":      "mysql",
	"postgres":   "pgx",
	"postgresql": "pgx",
	"pgx":        "pgx",
}

type sqlConfig struct {
	Driver string `json:"driver"`
	DSN    string `json:"dsn"`
}

// SqlConn is a connection to a foreign SQL database opened through database/sql.
type SqlConn struct {
	db *sql.DB
}

var _ Conn = (*SqlConn)(nil)

// parseSQLConfig validates the {"driver","dsn"} JSON shape without dialing and
// returns the registered database/sql driver name and the DSN.
func parseSQLConfig(ctx context.Context, configJSON string) (driver, dsn string, err error) {
	var sc sqlConfig
	if err := json.Unmarshal([]byte(configJSON), &sc); err != nil {
		return "", "", moerr.NewInvalidInputf(ctx, "sql_tvf: invalid config: %v", err)
	}
	driver, ok := driverAliases[sc.Driver]
	if !ok {
		return "", "", moerr.NewInvalidInputf(ctx, "sql_tvf: unsupported driver %q (supported: mysql, postgres)", sc.Driver)
	}
	if sc.DSN == "" {
		return "", "", moerr.NewInvalidInput(ctx, "sql_tvf: config is missing a dsn")
	}
	return driver, sc.DSN, nil
}

func validateSQLConfig(ctx context.Context, configJSON string) error {
	_, _, err := parseSQLConfig(ctx, configJSON)
	return err
}

func connectSQL(ctx context.Context, configJSON string) (Conn, error) {
	driver, dsn, err := parseSQLConfig(ctx, configJSON)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "sql_tvf: cannot open %s connection: %v", driver, err)
	}
	// A single-instance TVF needs only a small pool; keep the foreign server
	// footprint minimal.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, moerr.NewInternalErrorf(ctx, "sql_tvf: cannot connect to %s: %v", driver, err)
	}
	return &SqlConn{db: db}, nil
}

func (c *SqlConn) Kind() Kind { return KindSQL }

func (c *SqlConn) Close() error { return c.db.Close() }

// Query runs queryText and streams the result rows as MySQL-dialect CSV so the
// external CSV reader can materialize them: fields are double-quote enclosed
// with backslash escaping, and SQL NULL is written as an unquoted \N.
func (c *SqlConn) Query(ctx context.Context, queryText string) (io.ReadCloser, error) {
	//nolint:rowserrcheck // rows.Err is checked in encodeRowsCSV, which the goroutine below runs.
	rows, err := c.db.QueryContext(ctx, queryText)
	if err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "sql_tvf: query failed: %v", err)
	}
	pr, pw := io.Pipe()
	go func() {
		encErr := encodeRowsCSV(pw, rows)
		// Closing rows after encoding releases the pooled connection.
		_ = rows.Close()
		if encErr != nil {
			// Attribute mid-stream driver errors (Scan/rows.Err/network) to
			// sql_tvf; raw driver errors like "invalid connection" would
			// otherwise reach the user with no hint of their origin. A write
			// error caused by the reader closing the pipe early keeps its
			// io.ErrClosedPipe identity (the reader is gone; nobody sees it).
			if encErr != io.ErrClosedPipe {
				encErr = moerr.NewInternalErrorf(ctx, "sql_tvf: reading foreign result: %v", encErr)
			}
		}
		// A nil error closes the pipe with io.EOF for the reader.
		_ = pw.CloseWithError(encErr)
	}()
	return pr, nil
}

// encodeRowsCSV writes rows as MySQL-dialect CSV to w. It uses sql.RawBytes to
// avoid per-value driver conversions and to distinguish NULL (nil RawBytes)
// from an empty string.
func encodeRowsCSV(w io.Writer, rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	n := len(cols)
	raw := make([]sql.RawBytes, n)
	dest := make([]any, n)
	for i := range dest {
		dest[i] = &raw[i]
	}

	bw := bufio.NewWriter(w)
	line := make([]byte, 0, 256)
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		line = encodeCSVRow(line[:0], raw)
		if _, err := bw.Write(line); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return bw.Flush()
}

// encodeCSVRow appends one MySQL-dialect CSV record for fields (a nil field is
// SQL NULL, written as an unquoted \N) followed by a newline.
func encodeCSVRow(dst []byte, fields []sql.RawBytes) []byte {
	for i := range fields {
		if i > 0 {
			dst = append(dst, ',')
		}
		if fields[i] == nil {
			dst = append(dst, '\\', 'N') // NULL sentinel
		} else {
			dst = appendMySQLQuoted(dst, fields[i])
		}
	}
	return append(dst, '\n')
}

// appendMySQLQuoted appends val as a double-quoted, backslash-escaped CSV field
// matching the parser's MySQL escape decoding (\\ \" \n \r).
func appendMySQLQuoted(dst, val []byte) []byte {
	dst = append(dst, '"')
	for _, b := range val {
		switch b {
		case '\\':
			dst = append(dst, '\\', '\\')
		case '"':
			dst = append(dst, '\\', '"')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		default:
			dst = append(dst, b)
		}
	}
	dst = append(dst, '"')
	return dst
}
