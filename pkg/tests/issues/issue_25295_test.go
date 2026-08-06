// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

func TestIssue25295BinaryProtocolParameterKinds(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		lengthStmt, err := conn.PrepareContext(ctx, "select char_length(?)")
		require.NoError(t, err)
		defer lengthStmt.Close()

		lengthTests := []struct {
			name       string
			value      any
			wantLength sql.NullInt64
		}{
			// go-sql-driver encodes both []byte and string as MYSQL_TYPE_STRING,
			// so the server must apply the same text semantics to both.
			{name: "byte slice utf8", value: []byte("你好"), wantLength: sql.NullInt64{Int64: 2, Valid: true}},
			{name: "text utf8", value: "你好", wantLength: sql.NullInt64{Int64: 2, Valid: true}},
			{name: "invalid utf8 bytes", value: []byte{0xff, 0xfe}, wantLength: sql.NullInt64{Int64: 2, Valid: true}},
			{name: "empty bytes", value: []byte{}, wantLength: sql.NullInt64{Valid: true}},
			{name: "numeric bytes", value: []byte("1"), wantLength: sql.NullInt64{Int64: 1, Valid: true}},
			{name: "numeric text", value: "1", wantLength: sql.NullInt64{Int64: 1, Valid: true}},
			{name: "null", value: nil, wantLength: sql.NullInt64{}},
		}

		for _, test := range lengthTests {
			t.Run(test.name, func(t *testing.T) {
				var gotLength sql.NullInt64
				err := lengthStmt.QueryRowContext(ctx, test.value).Scan(&gotLength)
				require.NoError(t, err)
				require.Equal(t, test.wantLength, gotLength)
			})
		}

		numericStmt, err := conn.PrepareContext(ctx, "select ? + 0")
		require.NoError(t, err)
		defer numericStmt.Close()
		for _, test := range []struct {
			name       string
			value      any
			wantNumber sql.NullInt64
		}{
			{name: "numeric bytes", value: []byte("1"), wantNumber: sql.NullInt64{Int64: 1, Valid: true}},
			{name: "numeric text", value: "1", wantNumber: sql.NullInt64{Int64: 1, Valid: true}},
			{name: "null", value: nil, wantNumber: sql.NullInt64{}},
		} {
			t.Run("numeric "+test.name, func(t *testing.T) {
				var gotNumber sql.NullInt64
				err := numericStmt.QueryRowContext(ctx, test.value).Scan(&gotNumber)
				require.NoError(t, err)
				require.Equal(t, test.wantNumber, gotNumber)
			})
		}
	})
}
