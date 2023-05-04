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

// MAX_CHUNK_SIZE is the maximum size of a chunk of records to be inserted in a single insert.
const MAX_CHUNK_SIZE = 1024 * 1024 * 4

const PACKET_TOO_LARGE = "packet for query is too large"

var _ SqlWriter = (*BaseSqlWriter)(nil)

// BaseSqlWriter SqlWriter is a writer that writes data to a SQL database.
type BaseSqlWriter struct {
	db        *sql.DB
	address   string
	ctx       context.Context
	dbMux     sync.Mutex
	semaphore chan struct{}
}

type SqlWriter interface {
	table.RowWriter
	WriteRows(rows string, tbl *table.Table) (int, error)
	WriteRowRecords(records [][]string, tbl *table.Table, is_merge bool) (int, error)
}

func (sw *BaseSqlWriter) GetContent() string {
	return ""
}
func (sw *BaseSqlWriter) WriteRow(row *table.Row) error {
	return nil
}

func generateInsertStatement(records [][]string, tbl *table.Table) (string, int, error) {

	sb := strings.Builder{}
	defer sb.Reset()
	sb.WriteString("INSERT INTO")
	sb.WriteString(" `" + tbl.Database + "`." + tbl.Table + " ")
	sb.WriteString("VALUES ")

	// write values
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

func bulkInsert(db *sql.DB, records [][]string, tbl *table.Table, maxLen int) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	baseStr := fmt.Sprintf("INSERT INTO `%s`.`%s` ", tbl.Database, tbl.Table)

	sb := strings.Builder{}
	defer sb.Reset()
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
				_ = json.Unmarshal([]byte(field), &js)
				escapedJSON, _ := json.Marshal(js)
				sb.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(strings.ReplaceAll(string(escapedJSON), "\\", "\\\\"), "'", "\\'")))
			} else {
				escapedStr := strings.ReplaceAll(strings.ReplaceAll(field, "\\", "\\\\'"), "'", "\\'")
				if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr[:tbl.Columns[i].Scale-1]))
				} else {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr))
				}
			}
		}
		sb.WriteString(")")

		if sb.Len() >= maxLen || idx == len(records)-1 {
			stmt := baseStr + sb.String() + ";"
			_, err := db.Exec(stmt)
			if err != nil {
				return 0, err
			}
			sb.Reset()
			sb.WriteString("VALUES ")
		} else {
			sb.WriteString(",")
		}
	}

	return len(records), nil
}

func (sw *BaseSqlWriter) WriteRows(rows string, tbl *table.Table) (int, error) {

	//sw.semaphore <- struct{}{}
	//defer func() {
	//	// Release the semaphore
	//	<-sw.semaphore
	//}()

	r := csv.NewReader(strings.NewReader(rows))
	records, err := r.ReadAll()
	if err != nil {
		return 0, err
	}
	return sw.WriteRowRecords(records, tbl, false)
}

func (sw *BaseSqlWriter) WriteRowRecords(records [][]string, tbl *table.Table, is_merge bool) (int, error) {

	if is_merge {
		return 0, nil
	}
	var err error
	var cnt int
	var stmt string
	db, err := sw.initOrRefreshDBConn(false)
	if err != nil {
		logutil.Error("sqlWriter db init failed", zap.String("address", sw.address), zap.Error(err))
		return 0, err
	}
	stmt, cnt, err = generateInsertStatement(records, tbl)
	if err != nil {
		return 0, err

	}

	if len(stmt) < 16*1024*1024 {
		_, err = db.Exec(stmt)
	} else {
		if tbl.Table == "statement_info" || is_merge {
			cnt, err = bulkInsert(db, records, tbl, MAX_CHUNK_SIZE)
			if err != nil {
				return 0, err
			}
			return cnt, nil
		}
		return cnt, nil
	}

	return cnt, err
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
			fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=300s&writeTimeout=30m&timeout=3000s&maxAllowedPacket=0",
				dbUser.UserName,
				dbUser.Password,
				dbAddress)
		db, err := sql.Open("mysql", dsn)
		logutil.Info("sqlWriter db initialized", zap.String("address", dbAddress))
		if err != nil {
			logutil.Info("sqlWriter db initialized failed", zap.String("address", dbAddress), zap.Error(err))
			return err
		}
		if sw.db != nil {
			sw.db.Close()
		}
		sw.db = db
		//sw.db.SetMaxOpenConns(3)
		//sw.db.SetMaxIdleConns(3)
		sw.address = dbAddress
		return nil
	}

	if forceNewConn || sw.db == nil {
		if sw.db != nil {
			err := sw.db.Ping()
			if err == nil {
				return sw.db, nil
			}
		}
		logutil.Info("sqlWriter db init", zap.Bool("forceNewConn", forceNewConn))
		err := initFunc()
		if err != nil {
			return nil, err
		}
	}

	return sw.db, nil
}
