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

package sqlWriter

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"go.uber.org/zap"
)

const PACKET_LARGE_ERROR = "packet for query is too large"

var _ SqlWriter = (*BaseSqlWriter)(nil)

// BaseSqlWriter SqlWriter is a writer that writes data to a SQL database.
type BaseSqlWriter struct {
	db      *sql.DB
	address string
	ctx     context.Context
	dbMux   sync.Mutex
}

type SqlWriter interface {
	table.RowWriter
	WriteRows(rows string, tbl *table.Table) (int, error)
}

func (sw *BaseSqlWriter) GetContent() string {
	return ""
}
func (sw *BaseSqlWriter) WriteRow(row *table.Row) error {
	return nil
}

func generateInsertStatement(records [][]string, tbl *table.Table) (string, int, error) {

	sb := strings.Builder{}
	sb.WriteString("INSERT INTO")
	sb.WriteString(" `" + tbl.Database + "`." + tbl.Table + " ")

	// write columns
	sb.WriteString("(")
	for i, col := range tbl.Columns {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString("`" + col.Name + "`")
	}
	sb.WriteString(") ")

	// write values
	sb.WriteString("VALUES ")
	for idx, row := range records {
		if len(row) == 0 {
			continue
		}
		sb.WriteString("(")
		for i, field := range row {
			if i != 0 {
				sb.WriteString(",")
			}
			if tbl.Columns[i].ColType == table.TJson {
				var js interface{}
				err := json.Unmarshal([]byte(field), &js)
				if err != nil {
					return "", 0, err
				}
				escapedJSON, _ := json.Marshal(js)
				sb.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(strings.ReplaceAll(string(escapedJSON), "\\", "\\\\"), "'", "\\'")))
			} else {
				// escape single quote abd backslash
				escapedStr := strings.ReplaceAll(strings.ReplaceAll(field, "\\", "\\\\'"), "'", "\\'")
				// truncate string if it's too long caused by escape for varchar
				if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr[:tbl.Columns[i].Scale-1]))
				} else {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr))
				}
			}
		}
		if idx == len(records)-1 {
			sb.WriteString(");")
		} else {
			sb.WriteString("),")
		}
	}
	return sb.String(), len(records), nil
}

func chunkRecords(records [][]string, chunkSize int) [][][]string {
	var chunks [][][]string
	for i := 0; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunks = append(chunks, records[i:end])
	}
	return chunks
}

func bulkInsert(db *sql.DB, records [][]string, tbl *table.Table, maxLen int) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	chunkSize := maxLen
	chunks := chunkRecords(records, chunkSize)

	totalInserted := 0
	var err error
	for _, chunk := range chunks {

		stmt, cnt, _ := generateInsertStatement(chunk, tbl)
		_, err = db.Exec(stmt)
		if err != nil {
			if strings.Contains(err.Error(), PACKET_LARGE_ERROR) {
				chunkSize = len(chunk) / 2
				_, err = bulkInsert(db, chunk, tbl, chunkSize)
			} else {
				// simple retry
				_, err = db.Exec(stmt)
			}
		}
		if err != nil {
			return totalInserted, err
		}
		totalInserted += cnt
	}

	return totalInserted, err
}

func (sw *BaseSqlWriter) WriteRows(rows string, tbl *table.Table) (int, error) {
	r := csv.NewReader(strings.NewReader(rows))
	records, err := r.ReadAll()
	if err != nil {
		return 0, err
	}
	db, dbErr := sw.initOrRefreshDBConn(false)
	if dbErr != nil {
		logutil.Error("sqlWriter db init failed", zap.String("address", sw.address), zap.Error(dbErr))
		return 0, dbErr
	}
	stmt, cnt, _ := generateInsertStatement(records, tbl)
	_, err = db.Exec(stmt)
	if err != nil {
		// if table not exist return, no need to retry
		// todo: create table if not exist
		if strings.Contains(err.Error(), "no such table") {
			return 0, err
		}

		// refresh connection if invalid connection
		if strings.Contains(err.Error(), "invalid connection") {
			newDb, newDbErr := sw.initOrRefreshDBConn(true)
			if newDbErr != nil {
				logutil.Error("sqlWriter db init failed", zap.String("address", sw.address), zap.Error(newDbErr))
				return 0, newDbErr
			} else {
				db = newDb
			}
		}

		if strings.Contains(err.Error(), PACKET_LARGE_ERROR) {
			cnt, err = bulkInsert(db, records, tbl, 1000)
		} else {
			_, err = db.Exec(stmt)
		}
		if err != nil {
			logutil.Error("sqlWriter db insert retry failed", zap.String("address", sw.address), zap.Error(err))
			return 0, err
		}
		return cnt, nil
	}
	return cnt, nil
}

func (sw *BaseSqlWriter) FlushAndClose() (int, error) {
	sw.dbMux.Lock()
	defer sw.dbMux.Unlock()
	var err error
	if sw.db == nil {
		err = sw.db.Close()
	}
	sw.db = nil
	return 0, err
}

func (sw *BaseSqlWriter) initOrRefreshDBConn(forceNewConn bool) (*sql.DB, error) {
	sw.dbMux.Lock()
	defer sw.dbMux.Unlock()

	initFunc := func() error {
		dbUser, _ := GetSQLWriterDBUser()
		if dbUser == nil {
			sw.db = nil
			return errNotReady
		}

		addressFunc := GetSQLWriterDBAddressFunc()
		if addressFunc == nil {
			sw.db = nil
			return errNotReady
		}
		dbAddress, err := addressFunc(context.Background())
		if err != nil {
			return err
		}
		dsn :=
			fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=30s&writeTimeout=60s&timeout=30s",
				dbUser.UserName,
				dbUser.Password,
				dbAddress)
		db, err := sql.Open("mysql", dsn)
		logutil.Info("sqlWriter db initialized", zap.String("address", dbAddress))
		if err != nil {
			logutil.Info("sqlWriter db initialized err", zap.String("address", dbAddress), zap.Error(err))
			return err
		}
		if sw.db != nil {
			sw.db.Close()
			sw.db = db
			sw.address = dbAddress
		}
		return nil
	}

	if forceNewConn || sw.db == nil {
		if sw.db != nil {
			err := sw.db.Ping()
			if err == nil {
				return sw.db, nil
			}
		}
		logutil.Info("sqlWriter db will init", zap.Bool("forceNewConn", forceNewConn))
		err := initFunc()
		if err != nil {
			return nil, err
		}
	}

	return sw.db, nil
}
