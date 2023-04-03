package sqlWriter

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

// SQLWriter is a writer that writes data to a SQL database.
type SQLWriter struct {
	dsn          string
	dbname       string
	db           *sql.DB
	forceNewConn bool
	ctx          context.Context
}

func (sw *SQLWriter) InsertRow(ctx context.Context, row *table.Row) error {

	//TODO: convert row into insert statement
	// insert into table (col1, col2, col3) values (val1, val2, val3)
	//var columns = row.GetRawColumn()
	//var columnsStr = strings.Join(columns, ",")
	return nil
}

func (sw *SQLWriter) Close() error {
	return sw.db.Close()
}

func (sw *SQLWriter) initOrRefreshDBConn() (*sql.DB, error) {
	if sw.db == nil {
		db, err := sql.Open("mysql", sw.dsn)
		if err != nil {
			return nil, err
		}
		sw.db = db
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
