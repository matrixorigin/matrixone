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

package issues

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

func TestBinaryPreparedTableDescription(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		address := fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		adminDB, err := sql.Open("mysql", "dump:111@tcp("+address+")/?interpolateParams=false")
		require.NoError(t, err)
		defer adminDB.Close()
		admin, err := adminDB.Conn(ctx)
		require.NoError(t, err)
		defer admin.Close()
		const database = "binary_prepared_description"
		mustExec(t, ctx, admin, "create database "+database)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			mustExec(t, cleanup, admin, "drop database "+database)
		}()
		mustExec(t, ctx, admin, "create table "+database+".src (x int)")
		mustExec(t, ctx, admin, "create view "+database+".v as select x from "+database+".src")

		// Reuse the existing wire capture, without modifying packets. Only the
		// pinned reader uses this dialer; DDL is issued on a separate connection.
		var trace *issue25753TraceConn
		mysqlDriver.RegisterDialContext("description", func(ctx context.Context, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			trace = &issue25753TraceConn{Conn: conn}
			return trace, nil
		})
		defer mysqlDriver.DeregisterDialContext("description")
		db, err := sql.Open("mysql", "dump:111@description("+address+")/?interpolateParams=false")
		require.NoError(t, err)
		defer db.Close()
		reader, err := db.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()
		require.NotNil(t, trace)

		for _, keyword := range []string{"desc", "describe"} {
			for _, object := range []string{"src", "v"} {
				t.Run(keyword+"/"+object, func(t *testing.T) {
					query := keyword + " " + database + "." + object
					checkText := func(query, typ string, full bool) {
						t.Helper()
						rows, err := reader.QueryContext(ctx, query)
						require.NoError(t, err, query)
						checkDescriptionRows(t, rows, typ, full)
					}
					checkText(query, "int", false)
					mustExec(t, ctx, reader, "prepare description_control from '"+query+"'")
					defer mustExec(t, ctx, reader, "deallocate prepare description_control")
					checkText("execute description_control", "int", false)
					checkText("show columns from "+database+"."+object, "int", false)
					checkText("show full columns from "+database+"."+object, "int", true)

					descriptionPackets(t, trace) // discard handshake and text controls
					stmt, err := reader.PrepareContext(ctx, query)
					require.NoError(t, err, query)
					defer stmt.Close()
					packets := descriptionPackets(t, trace)
					require.NotEmpty(t, packets)
					require.Len(t, packets[0], 12, "COM_STMT_PREPARE_OK")
					require.Zero(t, packets[0][0])
					require.EqualValues(t, 7, binary.LittleEndian.Uint16(packets[0][5:7]))
					require.Zero(t, binary.LittleEndian.Uint16(packets[0][7:9]), "no parameters")
					preparedMetadata := checkDescriptionWireMetadata(t, packets[1:])
					checkBinary := func(typ string) {
						t.Helper()
						descriptionPackets(t, trace)
						rows, err := stmt.QueryContext(ctx)
						require.NoError(t, err)
						checkDescriptionRows(t, rows, typ, false)
						packets := descriptionPackets(t, trace)
						require.NotEmpty(t, packets)
						require.Equal(t, []byte{7}, packets[0], "binary EXECUTE result header")
						require.Equal(t, preparedMetadata, checkDescriptionWireMetadata(t, packets[1:]))
					}
					checkBinary("int")
					checkBinary("int")
					// A second connection changes the source without changing the
					// reader's local DDL generation. Both table and View must refresh.
					mustExec(t, ctx, admin, "alter table "+database+".src modify column x varchar(8)")
					defer mustExec(t, ctx, admin, "alter table "+database+".src modify column x int")
					checkText(query, "varchar(8)", false)
					checkBinary("varchar(8)")
				})
			}
		}

		for _, query := range []string{
			"show columns from " + database + ".src",
			"show columns from " + database + ".v",
			"show full columns from " + database + ".src",
			"show full columns from " + database + ".v",
		} {
			t.Run(query, func(t *testing.T) {
				stmt, err := reader.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer stmt.Close()
				rows, err := stmt.QueryContext(ctx)
				require.NoError(t, err)
				checkDescriptionRows(t, rows, "int", strings.HasPrefix(query, "show full"))
			})
		}

		t.Run("missing object", func(t *testing.T) {
			stmt, err := reader.PrepareContext(ctx, "describe "+database+".missing")
			if err == nil {
				defer stmt.Close()
				rows, queryErr := stmt.QueryContext(ctx)
				if rows != nil {
					defer rows.Close()
					require.NoError(t, rows.Err())
				}
				err = queryErr
			}
			var mysqlErr *mysqlDriver.MySQLError
			require.ErrorAs(t, err, &mysqlErr)
			require.EqualValues(t, 1146, mysqlErr.Number, "missing table, not a syntax error")
			var one int
			require.NoError(t, reader.QueryRowContext(ctx, "select 1").Scan(&one))
			require.Equal(t, 1, one)
		})

		t.Run("permission rejection", func(t *testing.T) {
			const username = "binary_description_reader"
			mustExec(t, ctx, admin, "create user "+username+" identified by '111'")
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
				defer stop()
				mustExec(t, cleanup, admin, "drop user "+username)
			}()
			restrictedDB, err := sql.Open("mysql", username+":111@tcp("+address+")/?interpolateParams=false")
			require.NoError(t, err)
			defer restrictedDB.Close()
			restricted, err := restrictedDB.Conn(ctx)
			require.NoError(t, err)
			defer restricted.Close()
			for _, object := range []string{"src", "v"} {
				func() {
					stmt, err := restricted.PrepareContext(ctx, "desc "+database+"."+object)
					if err == nil {
						defer stmt.Close()
						rows, queryErr := stmt.QueryContext(ctx)
						if rows != nil {
							defer rows.Close()
							require.NoError(t, rows.Err())
						}
						err = queryErr
					}
					var mysqlErr *mysqlDriver.MySQLError
					require.ErrorAs(t, err, &mysqlErr)
					require.NotEqualValues(t, 1064, mysqlErr.Number, "description syntax must be accepted")
					require.Contains(t, mysqlErr.Message, "privilege")
					var one int
					require.NoError(t, restricted.QueryRowContext(ctx, "select 1").Scan(&one))
					require.Equal(t, 1, one)
				}()
			}
		})
	})
}

func checkDescriptionRows(t *testing.T, rows *sql.Rows, typ string, full bool) {
	t.Helper()
	defer rows.Close()
	columns := []string{"Field", "Type", "Null", "Key", "Default", "Extra", "Comment"}
	types := []string{"VARCHAR", "TEXT", "VARCHAR", "ENUM", "TEXT", "VARCHAR", "TEXT"}
	value := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	want := []sql.NullString{value("x"), value(typ), value("YES"), value(""), {}, value(""), value("")}
	if full {
		columns = []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
		types = []string{"VARCHAR", "TEXT", "VARCHAR", "VARCHAR", "ENUM", "TEXT", "VARCHAR", "VARCHAR", "TEXT"}
		want = []sql.NullString{value("x"), value(typ), {}, value("YES"), value(""), {}, value(""), value("select,insert,update,references"), value("")}
	}
	actualColumns, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, columns, actualColumns)
	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, columnTypes, len(types))
	for i, column := range columnTypes {
		require.Equal(t, types[i], column.DatabaseTypeName(), columns[i])
	}
	require.True(t, rows.Next())
	actual := make([]sql.NullString, len(columns))
	dest := make([]any, len(columns))
	for i := range actual {
		dest[i] = &actual[i]
	}
	require.NoError(t, rows.Scan(dest...))
	require.Equal(t, want, actual)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func descriptionPackets(t *testing.T, trace *issue25753TraceConn) [][]byte {
	t.Helper()
	trace.mu.Lock()
	data := trace.reads
	trace.reads = nil
	trace.mu.Unlock()
	var packets [][]byte
	for len(data) > 0 {
		require.GreaterOrEqual(t, len(data), 4)
		size := int(data[0]) | int(data[1])<<8 | int(data[2])<<16
		require.GreaterOrEqual(t, len(data), 4+size)
		packets = append(packets, data[4:4+size])
		data = data[4+size:]
	}
	return packets
}

// Inspect PREPARE and EXECUTE metadata independently of database/sql, which
// discards PREPARE's column definitions. All six identifier fields in this
// fixture are short length-encoded strings (less than 251 bytes).
func checkDescriptionWireMetadata(t *testing.T, packets [][]byte) [][]byte {
	t.Helper()
	fields := []string{"Field", "Type", "Null", "Key", "Default", "Extra", "Comment"}
	kinds := []byte{0xfd, 0xfc, 0xfd, 0xfe, 0xfc, 0xfd, 0xfc}
	lengths := []uint32{256, 67108860, 12, 12, 262140, 1024, 24576}
	flags := []uint16{0, 4241, 1, 4481, 144, 0, 145}
	require.GreaterOrEqual(t, len(packets), len(fields)+1)
	metadata := make([][]byte, 0, len(fields))
	for i, field := range fields {
		packet := packets[i]
		for part := 0; part < 6; part++ {
			require.NotEmpty(t, packet)
			size := int(packet[0])
			require.Less(t, size, 251)
			require.GreaterOrEqual(t, len(packet), 1+size)
			if part == 4 {
				require.Equal(t, field, string(packet[1:1+size]))
			}
			packet = packet[1+size:]
		}
		require.Len(t, packet, 13)
		require.Equal(t, byte(12), packet[0])
		require.EqualValues(t, 45, binary.LittleEndian.Uint16(packet[1:3]))
		require.Equal(t, lengths[i], binary.LittleEndian.Uint32(packet[3:7]), field)
		require.Equal(t, kinds[i], packet[7], field)
		require.Equal(t, flags[i], binary.LittleEndian.Uint16(packet[8:10]), field)
		require.Zero(t, packet[10], field)
		metadata = append(metadata, packet)
	}
	require.NotEmpty(t, packets[len(fields)])
	require.Equal(t, byte(0xfe), packets[len(fields)][0], "column metadata EOF")
	return metadata
}
