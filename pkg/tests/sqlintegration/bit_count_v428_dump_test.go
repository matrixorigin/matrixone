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
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestLoadV428BitCountArchive(t *testing.T) {
	const checksum = "3217cdc85ac5c07d7d736754945006b6a35295303ed5dfcdbc24af994d4e033b"
	archive := t.TempDir()
	require.NoError(t, os.CopyFS(archive, os.DirFS(filepath.Join("testdata", "bit_count_v428"))))
	data, err := os.ReadFile(filepath.Join(archive, "manifest.json"))
	require.NoError(t, err)
	require.Equal(t, checksum, fmt.Sprintf("%x", sha256.Sum256(data)))
	var manifest struct {
		CreateSQL string `json:"create_sql"`
		Relations []struct {
			Objects []struct {
				FixturePath string `json:"fixture_path"`
			}
		}
	}
	require.NoError(t, json.Unmarshal(data, &manifest))
	require.Len(t, manifest.Relations, 1)
	require.Len(t, manifest.Relations[0].Objects, 1)
	objectPath := filepath.Join(archive, manifest.Relations[0].Objects[0].FixturePath)
	object, err := os.ReadFile(objectPath)
	require.NoError(t, err)
	require.NotEmpty(t, object)

	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		scalar := func(query, want string) {
			t.Helper()
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&got), query)
			require.Equal(t, want, got, query)
		}
		const schema = "bit_count_v428_dump"
		exec("create database " + schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec("use " + schema)
		exec(manifest.CreateSQL)
		load := "load table t from '" + archive + "'"
		source := " with release 'v4.2.8' checksum '" + checksum + "'"
		_, err = conn.ExecContext(ctx, load)
		require.ErrorContains(t, err, "target table schema does not match table dump")
		_, err = conn.ExecContext(ctx, load+" with release 'v4.2.8' checksum '"+strings.Repeat("0", 64)+"'")
		require.ErrorContains(t, err, "manifest checksum does not match")
		exec("create table wrong_schema like t")
		exec("alter table wrong_schema add column extra int")
		_, err = conn.ExecContext(ctx, "load table wrong_schema from '"+archive+"'"+source)
		require.ErrorContains(t, err, "target table schema does not match table dump")
		scalar("select count(*) from wrong_schema", "0")
		exec("alter table wrong_schema add column label varchar(8)")
		_, err = conn.ExecContext(ctx, "load table wrong_schema from '"+archive+"'"+source)
		require.ErrorContains(t, err, "requires numeric columns")
		exec("create table indexed_schema like t")
		exec("create index by_d on indexed_schema(d)")
		_, err = conn.ExecContext(ctx, "load table indexed_schema from '"+archive+"'"+source)
		require.ErrorContains(t, err, "requires a table without secondary index relations")

		object[0] ^= 1
		require.NoError(t, os.WriteFile(objectPath, object, 0600))
		_, err = conn.ExecContext(ctx, load+source)
		require.ErrorContains(t, err, "object")
		require.ErrorContains(t, err, "does not match manifest checksum")
		scalar("select count(*) from t", "0")
		exec("insert into t(id,d,f) values(9,3.9,3.9)")
		scalar("select concat(x, ':', vx) from t where id=9", "2:2")
		exec("truncate table t")
		object[0] ^= 1
		require.NoError(t, os.WriteFile(objectPath, object, 0600))
		exec(load + source)
		scalar("select count(*) from t", "4")
		scalar("select count(*) from t where id=3 and s is null and x is null and v is null and vx is null", "1")
		scalar("select group_concat(concat_ws(':',id,s,x,v,vx) order by id) from t where id<>3", "1:1:1:1:1,2:64:64:64:64,4:1:1:1:1")
		exec("insert into t(id,d,f) values(5,3.9,3.9)")
		exec("update t set d=3.9,f=3.9 where id=3")
		scalar("select group_concat(concat_ws(':',id,s,x,v,vx) order by id) from t where id in (3,5)", "3:1:1:1:1,5:1:1:1:1")
	})
}
