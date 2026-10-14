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

package sqlintegration

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestGeometryContracts(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "geometry_contracts"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		scalar := func(q string) string {
			t.Helper()
			var s string
			require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&s), q)
			return s
		}
		exec("drop database if exists " + schema)
		exec("create database " + schema)
		exec("use " + schema)
		exec("create table contacts(id int primary key,a geometry srid 0,b geometry srid 0)")
		for _, tc := range []struct {
			id         int
			a, b, want string
		}{
			{1, "POLYGON((0 0,1 0,1 1,0 1,0 0))", "POLYGON((1 0,2 0,2 1,1 1,1 0))", "LINESTRING(1 0,1 1)"},
			{2, "POLYGON((0 0,1 0,1 1,0 1,0 0))", "POLYGON((1 1,2 1,2 2,1 2,1 1))", "POINT(1 1)"},
			{3, "MULTIPOLYGON(((0 0,2 0,2 2,0 2,0 0)),((4 0,5 0,5 1,4 1,4 0)),((8 0,9 0,9 1,8 1,8 0)))", "MULTIPOLYGON(((1 0,3 0,3 2,1 2,1 0)),((5 0,6 0,6 1,5 1,5 0)),((9 1,10 1,10 2,9 2,9 1)))", "GEOMETRYCOLLECTION(POLYGON((1 0,2 0,2 2,1 2,1 0)),LINESTRING(5 0,5 1),POINT(9 1))"},
		} {
			expression := fmt.Sprintf("st_intersection(st_geomfromtext('%s',0),st_geomfromtext('%s',0))", tc.a, tc.b)
			require.Equal(t, tc.want, scalar("select st_astext("+expression+")"))
			exec(fmt.Sprintf("insert into contacts values(%d,st_geomfromtext('%s',0),st_geomfromtext('%s',0))", tc.id, tc.a, tc.b))
			require.Equal(t, tc.want, scalar(fmt.Sprintf("select st_astext(st_intersection(a,b)) from contacts where id=%d", tc.id)))
			require.Equal(t, tc.want, scalar("select st_astext(st_intersection(cast(st_geomfromtext('"+tc.a+"',0) as geometry32),cast(st_geomfromtext('"+tc.b+"',0) as geometry32)))"))
		}
		exec("create table geometry_lines(id int primary key,g geometry srid 0)")
		exec("insert into geometry_lines values(1,st_geomfromtext('LINESTRING(0 0,0 0,1 0,1 0)',0)),(2,null),(3,st_geomfromtext('LINESTRING(0 0,0 0)',0))")
		require.Equal(t, "1", scalar("select st_isvalid(g) from geometry_lines where id=1"))
		require.Equal(t, "0", scalar("select st_isvalid(g) from geometry_lines where id=3"))
		require.Equal(t, scalar("select st_issimple(st_geomfromtext('LINESTRING(0 0,0 0,1 0,1 0)',0))"), scalar("select st_issimple(g) from geometry_lines where id=1"))
		require.Equal(t, "1", scalar("select st_isvalid(g) is null from geometry_lines where id=2"))
		for _, expression := range []string{
			"st_geomfromtext('POLYGON((0 0,1 0,1 1))',0)",
			"st_geomfromtext('POLYGON((0 0,1 0,1 1,0 0.0000000001))',0)",
			"cast('MULTIPOLYGON(((0 0,1 0,1 1)))' as geometry)",
			"cast('POLYGON((0 0,1 0,1 1))' as geometry32)",
			"st_geomfromgeojson('{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[1,0],[1,1]]]}')",
			"st_geomfromwkb(unhex('0103000000010000000300000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F'),0)",
		} {
			_, err := conn.ExecContext(ctx, "insert into geometry_lines values(98,st_geomfromtext('POINT(1 2)',0)),(99,"+expression+")")
			require.Error(t, err, expression)
			require.Equal(t, "3", scalar("select count(*) from geometry_lines"), "invalid batch must persist no prefix")
		}
		require.Equal(t, "0", scalar("select st_isvalid(st_geomfromtext('POLYGON((0 0,1 1,1 0,0 1,0 0))',0))"))
		require.Equal(t, "MULTIPOLYGON(EMPTY,((0 0,1 0,0 1,0 0)))", scalar("select st_astext(st_geomfromtext('MULTIPOLYGON(EMPTY,((0 0,1 0,0 1,0 0)))',0))"))
		exec("create table wire_values(id int,g geometry srid 4326,g32 geometry32 srid 4326)")
		exec("insert into wire_values values(1,st_geomfromtext('POINT(1 2)',4326),st_geomfromtext('POINT(1 2)',4326)),(2,null,null)")
		expected, _ := hex.DecodeString("e61000000101000000000000000000f03f0000000000000040")
		query := "select g,g32,st_astext(g),st_aswkb(g) from wire_values order by id"
		for _, prepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("prepared=%v", prepared), func(t *testing.T) {
				var rows *sql.Rows
				if prepared {
					stmt, err := conn.PrepareContext(ctx, query)
					require.NoError(t, err)
					defer stmt.Close()
					rows, err = stmt.QueryContext(ctx)
					require.NoError(t, err)
				} else {
					rows, err = conn.QueryContext(ctx, query)
					require.NoError(t, err)
				}
				defer rows.Close()
				cols, err := rows.ColumnTypes()
				require.NoError(t, err)
				require.Equal(t, "GEOMETRY", cols[0].DatabaseTypeName())
				require.Equal(t, "GEOMETRY", cols[1].DatabaseTypeName())
				require.True(t, rows.Next())
				var g, g32, wkb []byte
				var text sql.NullString
				require.NoError(t, rows.Scan(&g, &g32, &text, &wkb))
				require.Equal(t, expected, g)
				require.Equal(t, expected, g32)
				require.Equal(t, "POINT(1 2)", text.String)
				require.Equal(t, expected[4:], wkb)
				require.True(t, rows.Next())
				require.NoError(t, rows.Scan(&g, &g32, &text, &wkb))
				require.Nil(t, g)
				require.Nil(t, g32)
				require.False(t, text.Valid)
				require.Nil(t, wkb)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			})
		}
		// Current admission requires a constant SRID, including PREPARE.
		dynamic, err := conn.PrepareContext(ctx, "select st_geomfromtext(?,?)")
		if dynamic != nil {
			defer dynamic.Close()
		}
		require.ErrorContains(t, err, "constant integer")
		for _, srid := range []int{0, 4326, 0} {
			func() {
				stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("select st_geomfromtext(?,%d)", srid))
				require.NoError(t, err)
				defer stmt.Close()
				want := append([]byte(nil), expected...)
				if srid == 0 {
					want[0], want[1] = 0, 0
				}
				var wire []byte
				require.NoError(t, stmt.QueryRowContext(ctx, "POINT(1 2)").Scan(&wire))
				require.Equal(t, want, wire)
				require.Error(t, stmt.QueryRowContext(ctx, "POLYGON((0 0,1 0,1 1))").Scan(&wire))
				require.NoError(t, stmt.QueryRowContext(ctx, "POINT(1 2)").Scan(&wire))
				require.Equal(t, want, wire)
			}()
		}
		var reported []byte
		require.NoError(t, conn.QueryRowContext(ctx, "select st_geomfromtext('POINT(116.3 39.8)',0)").Scan(&reported))
		require.Equal(t, "0000000001010000003333333333135d406666666666e64340", hex.EncodeToString(reported))

		exec("select st_geomfromtext('POINT(1 2)',0) into @geometry_source")
		require.Equal(t, "POINT(1 2)", scalar("select @geometry_source"))
	})
}
