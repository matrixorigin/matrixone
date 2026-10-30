// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// The existing one-CN SQL fixture is shared; each subtest owns two real client
// sessions and four input bytes. Raw upload packets let the client stop inside
// a frame without a huge file, sleeps, or a probabilistic KILL timing window.
func TestKillLoadLocal(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists kill_load_local")
		addr := fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		admin, err := sql.Open("mysql", "dump:111@tcp("+addr+")/")
		require.NoError(t, err)
		defer admin.Close()
		execSQLRequire(t, ctx, admin, "create database kill_load_local")
		execSQLRequire(t, ctx, admin, "create table kill_load_local.dst(a int)")
		execSQLRequire(t, ctx, admin, "insert into kill_load_local.dst values(99)")

		for _, phase := range []string{"before EOF", "partial payload", "before finalize", "explicit transaction", "connection kill", "completed control", "sleep control"} {
			t.Run(phase, func(t *testing.T) {
				cfg := mysql.NewConfig()
				cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = "dump", "111", "tcp", addr
				var wire net.Conn
				cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
					c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
					if err == nil {
						wire = c
					}
					return c, err
				}
				connector, err := mysql.NewConnector(cfg)
				require.NoError(t, err)
				db := sql.OpenDB(connector)
				defer db.Close()
				victim, err := db.Conn(ctx)
				require.NoError(t, err)
				defer victim.Close()
				var id uint32
				require.NoError(t, victim.QueryRowContext(ctx, "select connection_id()").Scan(&id))
				// Close the socket on every failure before releasing the cluster fixture.
				defer wire.Close()
				require.NoError(t, wire.SetDeadline(time.Now().Add(20*time.Second)))

				var barrier string
				if phase == "partial payload" {
					barrier = "load-local-packet-header"
				}
				if phase == "before finalize" || phase == "explicit transaction" {
					barrier = "load-local-before-finalize"
				}
				if barrier != "" {
					enabled := fault.Status()
					fault.Enable()
					require.NoError(t, fault.AddFaultPoint(ctx, barrier, ":::", "wait", 0, "", false))
					defer func() {
						_, err := fault.RemoveFaultPoint(context.Background(), barrier)
						require.NoError(t, err)
						_, err = fault.RemoveFaultPoint(context.Background(), barrier+"-waiters")
						require.NoError(t, err)
						if !enabled {
							fault.Disable()
						}
					}()
					require.NoError(t, fault.AddFaultPoint(ctx, barrier+"-waiters", ":::", "getwaiters", 0, barrier, false))
				}
				if phase == "sleep control" {
					done := make(chan error, 1)
					exited := make(chan struct{})
					go func() { defer close(exited); _, err := victim.ExecContext(ctx, "select sleep(60)"); done <- err }()
					defer func() {
						_ = wire.Close()
						select {
						case <-exited:
						case <-time.After(5 * time.Second):
							t.Error("sleep query did not stop")
						}
					}()
					tick := time.NewTicker(time.Millisecond)
					defer tick.Stop()
					for {
						var active int
						require.NoError(t, admin.QueryRowContext(ctx, fmt.Sprintf("select count(*) from information_schema.processlist where conn_id=%d and info='select sleep(60)'", id)).Scan(&active))
						if active == 1 {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						case <-tick.C:
						}
					}
					execSQLRequire(t, ctx, admin, fmt.Sprintf("kill query %d", id))
					select {
					case err := <-done:
						var me *mysql.MySQLError
						require.ErrorAs(t, err, &me)
						require.Equal(t, uint16(1317), me.Number)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					var after uint32
					require.NoError(t, victim.QueryRowContext(ctx, "select connection_id()").Scan(&after))
					require.Equal(t, id, after)
					return
				}
				if phase == "explicit transaction" {
					defer func() {
						if t.Failed() {
							_ = wire.Close()
							return
						}
						cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						_, err := victim.ExecContext(cleanCtx, "rollback")
						require.NoError(t, err)
					}()
				}
				// Two canceled LOADs on the same connection pin generation/reset behavior.
				rounds := 2
				if phase == "connection kill" || phase == "completed control" {
					rounds = 1
				}
				for round := 0; round < rounds; round++ {
					if barrier != "" && round > 0 {
						require.NoError(t, fault.AddFaultPoint(ctx, barrier, ":::", "wait", 0, "", false))
					}
					if phase == "explicit transaction" {
						_, err := victim.ExecContext(ctx, "begin")
						require.NoError(t, err)
						_, err = victim.ExecContext(ctx, "insert into kill_load_local.dst values(88)")
						require.NoError(t, err)
					}
					writeLocalTestPacket(t, wire, 0, append([]byte{3}, []byte("load data local infile 'kill.csv' into table kill_load_local.dst")...))
					request := readLocalTestPacket(t, wire)
					require.NotEmpty(t, request)
					require.Equal(t, byte(0xfb), request[0], "LOAD has started on this exact session")
					if phase == "partial payload" {
						// Header announces two bytes, but the payload is withheld until KILL.
						_, err = wire.Write([]byte{2, 0, 0, 2})
						require.NoError(t, err)
					} else {
						writeLocalTestPacket(t, wire, 2, []byte("7\n"))
						if phase == "before finalize" || phase == "explicit transaction" || phase == "completed control" {
							writeLocalTestPacket(t, wire, 3, nil)
						}
					}
					if phase == "completed control" {
						response := readLocalTestPacket(t, wire)
						require.NotEmpty(t, response)
						require.Equal(t, byte(0), response[0])
						execSQLRequire(t, ctx, admin, fmt.Sprintf("kill query %d", id))
						var after uint32
						require.NoError(t, victim.QueryRowContext(ctx, "select connection_id()").Scan(&after))
						require.Equal(t, id, after)
						var count int
						require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from kill_load_local.dst where a=7").Scan(&count))
						require.Equal(t, 1, count, "completed LOAD commits before a later idle KILL")
						execSQLRequire(t, ctx, admin, "delete from kill_load_local.dst where a=7")
						return
					}
					if barrier != "" {
						waitLocalTestBarrier(t, ctx, barrier)
					}
					kill := "kill query "
					if phase == "connection kill" {
						kill = "kill connection "
					}
					execSQLRequire(t, ctx, admin, fmt.Sprintf("%s%d", kill, id))
					if barrier != "" {
						_, err = fault.RemoveFaultPoint(ctx, barrier)
						require.NoError(t, err)
					}
					if phase == "partial payload" {
						_, err = wire.Write([]byte("7\n"))
						require.NoError(t, err)
					}
					if phase != "before finalize" && phase != "explicit transaction" {
						writeLocalTestPacket(t, wire, 3, nil)
					}
					response := readLocalTestPacket(t, wire)
					require.GreaterOrEqual(t, len(response), 9)
					require.Equal(t, byte(0xff), response[0], "KILL must never silently return LOAD success")
					require.Equal(t, uint16(1317), binary.LittleEndian.Uint16(response[1:3]))
					require.Equal(t, "70100", string(response[4:9]))
					if phase == "connection kill" {
						_, err = victim.ExecContext(ctx, "select 1")
						require.Error(t, err, "KILL CONNECTION must retire the session")
					} else {
						var after uint32
						require.NoError(t, victim.QueryRowContext(ctx, "select connection_id()").Scan(&after))
						require.Equal(t, id, after, "no reconnect may conceal a lost connection")
					}
					if phase == "explicit transaction" {
						var localCount int
						require.NoError(t, victim.QueryRowContext(ctx, "select count(*) from kill_load_local.dst where a=88").Scan(&localCount))
						require.Equal(t, 1, localCount, "KILL QUERY rolls back only the LOAD statement in an explicit transaction")
						_, err := victim.ExecContext(ctx, "commit")
						require.NoError(t, err)
						execSQLRequire(t, ctx, admin, "delete from kill_load_local.dst where a=88")
					}
					var rows, sum int
					require.NoError(t, admin.QueryRowContext(ctx, "select count(*),sum(a) from kill_load_local.dst").Scan(&rows, &sum))
					require.Equal(t, 1, rows, "canceled LOAD must not commit its row")
					require.Equal(t, 99, sum, "preexisting committed data survives")
				}
			})
		}
	})
}

func writeLocalTestPacket(t *testing.T, wire net.Conn, seq byte, payload []byte) {
	t.Helper()
	packet := append([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), seq}, payload...)
	n, err := wire.Write(packet)
	require.NoError(t, err)
	require.Equal(t, len(packet), n)
}

func readLocalTestPacket(t *testing.T, wire net.Conn) []byte {
	t.Helper()
	var header [4]byte
	_, err := io.ReadFull(wire, header[:])
	require.NoError(t, err)
	n := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	require.Less(t, n, 1024*1024, "test response must be bounded")
	payload := make([]byte, n)
	_, err = io.ReadFull(wire, payload)
	require.NoError(t, err)
	return payload
}

func waitLocalTestBarrier(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		n, _, ok := fault.TriggerFault(name + "-waiters")
		if ok && n == 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}
