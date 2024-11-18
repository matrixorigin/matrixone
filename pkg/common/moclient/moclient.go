package moclient

import (
	"context"
	"database/sql"
	"fmt"
	"go.uber.org/zap"

	_ "github.com/go-sql-driver/mysql"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type moConnector struct {
	addressFactory func(context.Context, bool) (string, error)
	dsnTemplate    string

	db *sql.DB
}

type MOClient interface {
	GetOrConnect(ctx context.Context, reuse bool) (*sql.DB, error)
	Close() error
}

func (f *moConnector) GetOrConnect(ctx context.Context, reuse bool) (*sql.DB, error) {
	if reuse && f.db != nil {
		err := f.db.PingContext(ctx)
		if err != nil {
			f.db, err = f.GetOrConnect(ctx, false)
			return f.db, err
		}
	}

	if f.db != nil {
		_ = f.db.Close()
	}

	address, err := f.addressFactory(ctx, true)
	if err != nil {
		logutil.Error("failed to refresh task storage",
			zap.Error(err))
		return nil, err
	}

	logutil.Debug("trying to refresh task storage", zap.String("address", address))
	f.db, err = sql.Open("mysql", fmt.Sprintf(f.dsnTemplate, address))
	if err != nil {
		return nil, err
	}

	f.db.SetMaxOpenConns(5)
	f.db.SetMaxIdleConns(3)

	logutil.Debug("refresh task storage completed", zap.String("sql-address", address))

	return f.db, nil

}

func (f *moConnector) Close() error {
	if f.db != nil {
		return f.db.Close()
	}
	return nil
}

func NewMOConnector(addressFactory func(context.Context, bool) (string, error), dsnTemplate string) MOClient {
	return &moConnector{
		addressFactory: addressFactory,
		dsnTemplate:    dsnTemplate,
	}
}

func DBDSNTemplate(username, password, database string) string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?readTimeout=15s&writeTimeout=15s&timeout=15s&parseTime=true&loc=Local&disable_txn_trace=1",
		username,
		password,
		"%s", database)
}
