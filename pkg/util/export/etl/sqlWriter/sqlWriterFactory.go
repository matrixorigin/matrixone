package sqlWriter

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	errNotReady = moerr.NewInvalidStateNoCtx("SQL writer's DB conn not ready")
)

// sqlWriterDBUser holds the db user for logger
var sqlWriterDBUser atomic.Value

var dbAddressFunc atomic.Value

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

func NewSQLWriter(dbname string, ctx context.Context) (*SQLWriter, error) {
	dbUser := GetSQLWriterDBUser()
	if dbUser != nil {
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
	sw := &SQLWriter{
		ctx:          ctx,
		dbname:       dbname,
		forceNewConn: true,
		dsn:          dbUser.UserName + ":" + dbUser.Password + "@tcp(" + dbAddress + ")/" + dbname + "?timeout=15s",
	}
	_, initErr := sw.initOrRefreshDBConn()
	if initErr != nil {
		return nil, initErr
	}
	return sw, nil
}
