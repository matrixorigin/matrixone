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

// MAX_CHUNK_SIZE is the maximum size of a chunk of records to be inserted in a single query.
// Default packet size limit for MySQL is 16MB, but we set it to 15MB to be safe.
const MAX_CHUNK_SIZE = 1024 * 1024 * 15

//18331736

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
				_ = json.Unmarshal([]byte(field), &js)
				escapedJSON, _ := json.Marshal(js)
				sb.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(strings.ReplaceAll(string(escapedJSON), "\\", "\\\\"), "'", "\\'")))
			} else {
				// escape single quote and backslash
				escapedStr := strings.ReplaceAll(strings.ReplaceAll(field, "\\", "\\\\'"), "'", "\\'")
				// truncate string if it's too long caused by escape for varchar
				if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr[:tbl.Columns[i].Scale-1]))
				} else {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr))
				}
			}
		}
		if sb.Len() >= maxLen || idx == len(records)-1 {
			sb.WriteString(");")
			stmt := baseStr + sb.String()
			_, err := db.Exec(stmt)
			if err != nil {
				// logutil.Info("bulk insert failed", zap.String("table", tbl.Table), zap.String("len", strconv.Itoa(len(stmt))), zap.Error(err))
				return 0, err
			}
			sb.Reset()
			sb.WriteString("VALUES ")
		} else {
			sb.WriteString("),")
		}
	}
	return len(records), nil
}

func (sw *BaseSqlWriter) WriteRows(rows string, tbl *table.Table) (int, error) {
	r := csv.NewReader(strings.NewReader(rows))
	records, err := r.ReadAll()
	if err != nil {
		return 0, err
	}
	db, dbErr := sw.initOrRefreshDBConn(false)
	if dbErr != nil {
		// logutil.Error("sqlWriter db init failed", zap.String("address", sw.address), zap.Error(dbErr))
		return 0, dbErr
	}

	bulkCnt, bulkErr := bulkInsert(db, records, tbl, MAX_CHUNK_SIZE)
	if bulkErr != nil {
		// logutil.Info("sqlWriter db insert bulk insert failed"+bulkErr.Error(), zap.String("address", sw.address), zap.String("records lens", strconv.Itoa(len(records))))
		return 0, err
	}
	return bulkCnt, bulkErr

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
