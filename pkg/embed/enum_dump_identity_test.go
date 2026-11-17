// Copyright 2026 Matrix Origin
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

package embed

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestEnumDumpDomainIdentityAndLegacyCluster(t *testing.T) {
	RunBaseClusterTests(t, func(cluster Cluster) {
		root := t.TempDir()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			for _, stmt := range []string{
				"drop table if exists mo_catalog.enum_dump_cluster_src",
				"drop table if exists mo_catalog.enum_dump_cluster_dst",
				"drop database if exists enum_dump_src",
				"drop database if exists enum_dump_dst",
				"drop stage if exists enum_dump_stage",
			} {
				_, err := db.ExecContext(cleanup, stmt)
				require.NoErrorf(t, err, "%s", stmt)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		for _, stmt := range []string{
			"create database enum_dump_src", "create database enum_dump_dst",
			fmt.Sprintf("create stage enum_dump_stage url = 'file://%s/'", root),
			"create table enum_dump_src.source (id int primary key, e enum('a\\nb') collate utf8mb4_bin, key idx_e(e) comment 'stable')",
			"insert into enum_dump_src.source values (1, 1)",
			"select mo_ctl('dn','flush','enum_dump_src.source')",
		} {
			_, err := db.ExecContext(ctx, stmt)
			require.NoErrorf(t, err, "%s", stmt)
		}
		var indexTable string
		require.NoError(t, db.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='idx_e' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_dump_src' and relname='source') limit 1").Scan(&indexTable))
		require.NotEmpty(t, indexTable)
		_, err = db.ExecContext(ctx, fmt.Sprintf("select mo_ctl('dn','flush','enum_dump_src.%s')", indexTable))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "dump table enum_dump_src.source to 'stage://enum_dump_stage/full'")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create table enum_dump_dst.bad (id int primary key, e enum('a\\\\nb') collate utf8mb4_bin, key idx_e(e) comment 'stable')")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "load table enum_dump_dst.bad from 'stage://enum_dump_stage/full'")
		require.ErrorContains(t, err, "schema")
		raw, err := os.ReadFile(filepath.Join(root, "full", "manifest.json"))
		require.NoError(t, err)
		var manifest struct {
			CreateSQL  string `json:"create_sql"`
			SchemaHash string `json:"schema_hash"`
		}
		require.NoError(t, json.Unmarshal(raw, &manifest))
		require.NotEmpty(t, manifest.CreateSQL)
		_, err = db.ExecContext(ctx, "use enum_dump_dst")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, manifest.CreateSQL)
		require.NoErrorf(t, err, "manifest DDL: %s", manifest.CreateSQL)
		_, err = db.ExecContext(ctx, "load table enum_dump_dst.source from 'stage://enum_dump_stage/full'")
		require.NoError(t, err)
		for _, name := range []string{"enum_dump_src.source", "enum_dump_dst.source"} {
			var ordinal int
			var value string
			require.NoError(t, db.QueryRowContext(ctx, "select e+0, hex(cast(e as char)) from "+name).Scan(&ordinal, &value))
			require.Equal(t, 1, ordinal)
			require.Equal(t, "610A62", value)
		}
		_, err = db.ExecContext(ctx, "create cluster table mo_catalog.enum_dump_cluster_src (id int primary key, e enum('A','a'))")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "dump table mo_catalog.enum_dump_cluster_src to 'stage://enum_dump_stage/cluster' metadata only")
		require.NoError(t, err)
		raw, err = os.ReadFile(filepath.Join(root, "cluster", "manifest.json"))
		require.NoError(t, err)
		manifest = struct {
			CreateSQL  string `json:"create_sql"`
			SchemaHash string `json:"schema_hash"`
		}{}
		require.NoError(t, json.Unmarshal(raw, &manifest))
		require.Empty(t, manifest.CreateSQL)
		require.NotEmpty(t, manifest.SchemaHash)
		_, err = db.ExecContext(ctx, "create cluster table mo_catalog.enum_dump_cluster_dst (id int primary key, e enum('A','a'))")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "load table mo_catalog.enum_dump_cluster_dst from 'stage://enum_dump_stage/cluster'")
		require.NoError(t, err)
	})
}
