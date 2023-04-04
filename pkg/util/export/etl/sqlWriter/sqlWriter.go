package sqlWriter

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var _ SqlWriter = (*BaseSqlWriter)(nil)

// SqlWriter is a writer that writes data to a SQL database.
type BaseSqlWriter struct {
	table        *table.Table
	db           *sql.DB
	dsn          string
	forceNewConn bool
	ctx          context.Context
}

type SqlWriter interface {
	table.RowWriter
	WriteRows(rows string) (int, error)
}

func (sw *BaseSqlWriter) GetContent() string {
	return ""
}
func (sw *BaseSqlWriter) WriteRow(row *table.Row) error {

	//TODO: convert row into insert statement
	// insert into table (col1, col2, col3) values (val1, val2, val3)
	//var columns = row.GetRawColumn()
	//var columnsStr = strings.Join(columns, ",")
	return nil
}

func (sw *BaseSqlWriter) WriteRows(rows string) (int, error) {
	db, err := sw.initOrRefreshDBConn()
	if err != nil {
		return 0, err
	}
	//todo: convert rows into insert statement
	_, err = db.Exec("INSERT INTO `system`.rawlog (raw_item, node_uuid, node_type, span_id, trace_id, logger_name, `timestamp`, `level`, caller, message, extra, err_code, error, stack, span_name, parent_span_id, start_time, end_time, duration, resource, span_kind) VALUES('', '', '', '0', '', '', '', '', '', '', ?, '0', '', '', '', '0', '', '', '0', ?, '');")
	if err != nil {
		return 0, nil
	}
	return 1, nil
}

func (sw *BaseSqlWriter) FlushAndClose() (int, error) {
	return 0, sw.db.Close()
}

func (sw *BaseSqlWriter) initOrRefreshDBConn() (*sql.DB, error) {
	if sw.db == nil {
		dbUser := GetSQLWriterDBUser()
		if dbUser == nil {
			return nil, errNotReady
		}

		addressFunc := GetSQLWriterDBAddressFunc()
		if addressFunc == nil {
			return nil, errNotReady
		}
		dbAddress, err := addressFunc(context.Background())
		if err != nil {
			return nil, err
		}
		dsn :=
			fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=15s&writeTimeout=15s&timeout=15s",
				dbUser.UserName,
				dbUser.Password,
				dbAddress)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, err
		}
		sw.db = db
		sw.dsn = dsn
	}
	if err := sw.db.Ping(); err != nil {
		if sw.forceNewConn {
			db, err := sql.Open("mysql", sw.dsn)
			if err != nil {
				return nil, err
			}
			sw.db = db
		} else {
			return nil, err
		}
	}
	return sw.db, nil
}
