package sqlWriter

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var (
	errNotReady = moerr.NewInvalidStateNoCtx("SQL writer's DB conn not ready")
)

// sqlWriterDBUser holds the db user for logger
var (
	sqlWriterDBUser atomic.Value
	dbAddressFunc   atomic.Value
)

const DBLoggerUser = "mo_logger"

type DBUser struct {
	UserName string
	Password string
}

func SetSQLWriterDBUser(userName string, password string) {
	user := &DBUser{
		UserName: userName,
		Password: password,
	}
	sqlWriterDBUser.Store(user)
}
func GetSQLWriterDBUser() *DBUser {
	return sqlWriterDBUser.Load().(*DBUser)
}

func SetSQLWriterDBAddressFunc(f func(context.Context) (string, error)) {
	dbAddressFunc.Store(f)
}

func GetSQLWriterDBAddressFunc() func(context.Context) (string, error) {
	return dbAddressFunc.Load().(func(context.Context) (string, error))
}

func NewSqlWriter(tbl *table.Table, ctx context.Context) *BaseSqlWriter {
	sw := &BaseSqlWriter{
		ctx:          ctx,
		forceNewConn: true,
		table:        tbl,
	}
	return sw
}
