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

package embed

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	mysql "github.com/matrixorigin/mysql"
	"github.com/stretchr/testify/require"
)

// Keep the production connector and target session; inject only a real server
// rollback followed by the typed SQL failure. No product-side test hook is needed.
type cdcAbortConnector struct {
	driver.Connector
	fault func(context.Context, driver.Conn, string) error
}

func (c cdcAbortConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &cdcAbortConn{Conn: conn, fault: c.fault}, nil
}

type cdcAbortConn struct {
	driver.Conn
	fault func(context.Context, driver.Conn, string) error
}

func (c *cdcAbortConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}
func (c *cdcAbortConn) Ping(ctx context.Context) error { return c.Conn.(driver.Pinger).Ping(ctx) }
func (c *cdcAbortConn) CheckNamedValue(v *driver.NamedValue) error {
	return c.Conn.(driver.NamedValueChecker).CheckNamedValue(v)
}
func (c *cdcAbortConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	actual := q
	for _, arg := range args {
		if arg.Name == mysql.ReuseQueryBuf {
			actual = string(arg.Value.([]byte)[5:])
		}
	}
	if err := c.fault(ctx, c.Conn, actual); err != nil {
		return nil, err
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}

func TestCDCTransactionAbortReplaysAllRowsOnMO(t *testing.T) {
	RunBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := int(cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		_, err = db.ExecContext(ctx, "CREATE DATABASE cdc_transaction_retry")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			_, cleanupErr := db.ExecContext(cleanupCtx, "DROP DATABASE cdc_transaction_retry")
			require.NoError(t, cleanupErr)
		}()
		_, err = db.ExecContext(ctx, "CREATE TABLE cdc_transaction_retry.t (id INT PRIMARY KEY, v INT)")
		require.NoError(t, err)
		original := cdc.OpenDbConn
		defer func() { cdc.OpenDbConn = original }()
		for _, code := range []uint16{20701, 1213} {
			t.Run(strconv.Itoa(int(code)), func(t *testing.T) {
				_, err := db.ExecContext(ctx, "TRUNCATE TABLE cdc_transaction_retry.t")
				require.NoError(t, err)
				failed, attempts := false, 0
				cause := &mysql.MySQLError{Number: code, Message: "controlled transaction abort"}
				cdc.OpenDbConn = func(_ context.Context, user, password, ip string, port int, timeout string) (*sql.DB, error) {
					cfg := mysql.NewConfig()
					cfg.User, cfg.Passwd, cfg.Net = user, password, "tcp"
					cfg.Addr = net.JoinHostPort(ip, strconv.Itoa(port))
					cfg.MaxAllowedPacket = 64 << 20
					cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 10*time.Second, 10*time.Second, 10*time.Second
					connector, err := mysql.NewConnector(cfg)
					if err != nil {
						return nil, err
					}
					return sql.OpenDB(cdcAbortConnector{Connector: connector, fault: func(callCtx context.Context, conn driver.Conn, q string) error {
						if strings.Contains(q, "VALUES (2,20)") {
							attempts++
							if !failed {
								failed = true
								if _, err := conn.(driver.ExecerContext).ExecContext(callCtx, "ROLLBACK", nil); err != nil {
									return err
								}
								return cause
							}
						}
						return nil
					}}), nil
				}
				exec, err := cdc.NewExecutor(ctx, "dump", "111", "127.0.0.1", port, 2, time.Minute, "10s", false)
				require.NoError(t, err)
				defer func() { require.NoError(t, exec.Close()) }()
				a := []byte("     INSERT INTO cdc_transaction_retry.t VALUES (1,10)")
				b := []byte("     INSERT INTO cdc_transaction_retry.t VALUES (2,20)")
				require.NoError(t, exec.BeginTx(ctx))
				require.NoError(t, exec.ExecSQL(ctx, nil, a, true))
				err = exec.ExecSQL(ctx, nil, b, true)
				require.ErrorIs(t, err, cause)
				require.Equal(t, 1, attempts)
				retryable, classified := cdc.ClassifyRetryableError(err)
				require.True(t, classified && retryable)
				require.NoError(t, exec.RollbackTx(ctx))
				var count int
				require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cdc_transaction_retry.t").Scan(&count))
				require.Zero(t, count, "the server rolled back A; no B-only autocommit is allowed")
				require.NoError(t, exec.BeginTx(ctx))
				require.NoError(t, exec.ExecSQL(ctx, nil, a, true))
				require.NoError(t, exec.ExecSQL(ctx, nil, b, true))
				require.NoError(t, exec.CommitTx(ctx))
				rows, err := db.QueryContext(ctx, "SELECT id,v FROM cdc_transaction_retry.t ORDER BY id")
				require.NoError(t, err)
				defer rows.Close()
				var got [][2]int
				for rows.Next() {
					var row [2]int
					require.NoError(t, rows.Scan(&row[0], &row[1]))
					got = append(got, row)
				}
				require.NoError(t, rows.Err())
				require.Equal(t, [][2]int{{1, 10}, {2, 20}}, got)
			})
		}
	})
}
