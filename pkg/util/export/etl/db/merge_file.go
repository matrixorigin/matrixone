// Copyright 2022 Matrix Origin
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

package db_holder

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	mergeSQLTarget  = 4 * mpool.MB
	mergeSQLLimit   = 24 * mpool.MB
	mergeLoadPrefix = "LOAD DATA INLINE FORMAT='csv', DATA='"
)

// Reserve at most one slot of the shared writer pool across source reads.
var mergeFileSlot = make(chan struct{}, 1)

// WriteMergeFile consumes borrowed rows synchronously. A file that takes more
// than one LOAD uses one transaction, so a failed import cannot leave a first
// row that would cause the next run to skip the rest.
func WriteMergeFile(ctx context.Context, tbl *table.Table, first []string, next func() ([]string, error), timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case mergeFileSlot <- struct{}{}:
		defer func() { <-mergeFileSlot }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	reconnect := !DBConnErrCount.Check()
	dbConn, err := GetOrInitDBConn(reconnect, reconnect)
	if err != nil {
		v2.TraceMOLoggerErrorConnDBCounter.Inc()
		_ = DBConnErrCount.Count()
		return err
	}
	queryCtx, cancel := context.WithTimeoutCause(ctx, timeout, moerr.CauseWriteRowRecords)
	defer cancel()
	conn, err := dbConn.Conn(queryCtx)
	if err != nil {
		_ = DBConnErrCount.Count()
		return err
	}
	defer conn.Close()

	var packetLimit int
	err = conn.QueryRowContext(queryCtx, "SELECT @@session.max_allowed_packet").Scan(&packetLimit)
	cancel()
	if err != nil {
		_ = DBConnErrCount.Count()
		return err
	}
	// The driver reserves one byte from the negotiated limit and counts the
	// COM_QUERY command byte against the remaining budget.
	err = writeMergeFile(ctx, conn, tbl, first, next, min(mergeSQLLimit, packetLimit-2), mergeSQLTarget, timeout)
	if err == nil {
		DBConnErrCount.Reset()
	}
	return err
}

func writeMergeFile(ctx context.Context, conn *sql.Conn, tbl *table.Table, row []string, next func() ([]string, error), limit, target int, timeout time.Duration) (result error) {
	suffix := "' INTO TABLE " + tbl.Database + "." + tbl.Table + " FIELDS TERMINATED BY ','"
	overhead := len(mergeLoadPrefix) + len(suffix)
	target = min(limit, target)
	var tx *sql.Tx
	defer func() {
		if tx != nil {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				result = errors.Join(result, err)
			}
		}
	}()
	buf := append([]byte(nil), mergeLoadPrefix...)
	quotes := make([]bool, len(tbl.Columns))

	flush := func() error {
		if len(buf) == len(mergeLoadPrefix) {
			return nil
		}
		query := string(append(buf, suffix...))
		v2.TraceMOLoggerExportSqlHistogram.Observe(float64(len(query)))
		queryCtx, cancel := context.WithTimeoutCause(ctx, timeout, moerr.CauseWriteRowRecords)
		defer cancel()
		var err error
		if tx != nil {
			_, err = tx.ExecContext(queryCtx, query)
		} else {
			_, err = conn.ExecContext(queryCtx, query)
		}
		if err != nil {
			_ = DBConnErrCount.Count()
			return moerr.AttachCause(queryCtx, err)
		}
		if cap(buf) > target {
			buf = append([]byte(nil), mergeLoadPrefix...)
		} else {
			buf = buf[:len(mergeLoadPrefix)]
		}
		return nil
	}

	for row != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(quotes) < len(row) {
			quotes = slices.Grow(quotes, len(row)-len(quotes))[:len(row)]
		}
		n := mergeRowSize(row, quotes)
		if n > limit-overhead {
			return moerr.NewInvalidInputf(ctx, "ETL CSV row needs %d SQL bytes, exceeding the %d byte limit", n+overhead, limit)
		}
		need := len(buf) + n + len(suffix)
		if len(buf) > len(mergeLoadPrefix) && need > target {
			if tx == nil {
				var err error
				tx, err = conn.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
			}
			if err := flush(); err != nil {
				return err
			}
			need = overhead + n
		}
		if need > cap(buf) {
			capacity := need
			if need <= target {
				capacity = max(need, min(target, max(4096, 2*cap(buf))))
			}
			grown := make([]byte, len(buf), capacity)
			copy(grown, buf)
			buf = grown
		}
		buf = appendMergeRow(buf, row, quotes)
		var err error
		row, err = next()
		if err != nil {
			return err
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if tx != nil {
		// Any commit error retains the source. In particular, a lost reply
		// does not establish whether the server committed the file.
		return tx.Commit()
	}
	return nil
}

// The old writer escaped each field for SQL and then passed it to encoding/csv.
// Compute the complete encoded length before growing the output buffer.
func mergeRowSize(row []string, quotes []bool) int {
	n := max(1, len(row)) // field separators and final newline
	for i, field := range row {
		n += len(field)
		quoted := false
		for j := 0; j < len(field); j++ {
			switch field[j] {
			case '"':
				n++
				quoted = true
			case '\\', '\'':
				n++
			case ',', '\r', '\n':
				quoted = true
			}
		}
		if !quoted && len(field) > 0 {
			r, _ := utf8.DecodeRuneInString(field)
			quoted = unicode.IsSpace(r)
		}
		// encoding/csv's special case for a lone backslash-dot did not
		// apply here: SQL escaping had already doubled the backslash.
		quotes[i] = quoted
		if quoted {
			n += 2
		}
	}
	return n
}

func appendMergeRow(dst []byte, row []string, quotes []bool) []byte {
	for i, field := range row {
		if i > 0 {
			dst = append(dst, ',')
		}
		if quotes[i] {
			dst = append(dst, '"')
		}
		for len(field) != 0 {
			j := strings.IndexAny(field, "\\'\"")
			if j < 0 {
				dst = append(dst, field...)
				break
			}
			dst = append(dst, field[:j]...)
			dst = append(dst, field[j], field[j])
			field = field[j+1:]
		}
		if quotes[i] {
			dst = append(dst, '"')
		}
	}
	return append(dst, '\n')
}
