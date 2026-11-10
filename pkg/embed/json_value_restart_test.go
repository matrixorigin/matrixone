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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This is a real same-binary full-cluster restart, not a predecessor-release
// upgrade. The catalog and object data remain in the test's private directory.
func TestJSONValuePersistedExpressionsSurviveRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	t.Cleanup(cancel)
	// Darwin's Unix socket path limit is shorter than testing.TempDir's
	// generated name for this test. Own a short directory and release it only
	// after this cluster has closed.
	dir, err := os.MkdirTemp("/tmp", "jv-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	c, err := NewCluster(WithTesting(), WithCNCount(1), WithTestAdmissionContext(ctx),
		WithPreStart(adjustBasicClusterService), func(c *cluster) { c.options.dataPath = dir })
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)
	start, cleanup := newRecoveryClusterStarterWithBudget(ctx, c.Start, time.Now, 3*time.Minute)
	t.Cleanup(cleanup)
	ctx, err = start()
	require.NoError(t, err)
	open := func() *sql.DB {
		t.Helper()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		return recoverySQLClient(t, cn)
	}
	exec := func(db *sql.DB, statement string) {
		t.Helper()
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	db := open()
	for _, statement := range []string{
		`create database json_value_restart`,
		`create table json_value_restart.docs (id int primary key, doc json, n bigint generated always as (json_value(doc, '$.keep' returning signed)) stored, key n_idx(n))`,
		`insert into json_value_restart.docs(id, doc) values (1, '{"keep":2}')`,
		`create view json_value_restart.legacy_view as select json_value(doc, '$.keep') as v from json_value_restart.docs`,
		`create view json_value_restart.typed_view as select json_value(doc, '$.keep' returning signed default 9 on error) as v from json_value_restart.docs`,
		`create table json_value_restart.defaults (n bigint default (json_value('5', '$' returning signed)))`,
		`insert into json_value_restart.defaults values (default)`,
		`select mo_ctl('dn', 'flush', 'json_value_restart.docs')`,
		`select mo_ctl('dn', 'flush', 'json_value_restart.defaults')`,
		`select mo_ctl('dn', 'globalcheckpoint', '')`,
	} {
		exec(db, statement)
	}
	require.NoError(t, db.Close())
	require.NoError(t, c.Stop())
	ctx, err = start()
	require.NoError(t, err)
	db = open()
	defer db.Close()
	var legacy string
	require.NoError(t, db.QueryRowContext(ctx, `select v from json_value_restart.legacy_view`).Scan(&legacy))
	require.Equal(t, "2", legacy)
	var typed, generated, defaultValue, indexed int64
	require.NoError(t, db.QueryRowContext(ctx, `select v from json_value_restart.typed_view`).Scan(&typed))
	require.NoError(t, db.QueryRowContext(ctx, `select n from json_value_restart.docs where id=1`).Scan(&generated))
	require.NoError(t, db.QueryRowContext(ctx, `select n from json_value_restart.defaults`).Scan(&defaultValue))
	require.NoError(t, db.QueryRowContext(ctx, `select count(*) from json_value_restart.docs force index(n_idx) where n=2`).Scan(&indexed))
	require.Equal(t, int64(2), typed)
	require.Equal(t, int64(2), generated)
	require.Equal(t, int64(5), defaultValue)
	require.Equal(t, int64(1), indexed)
	exec(db, `update json_value_restart.docs set doc='{"keep":3}' where id=1`)
	require.NoError(t, db.QueryRowContext(ctx, `select count(*) from json_value_restart.docs force index(n_idx) where n=3`).Scan(&indexed))
	require.Equal(t, int64(1), indexed)
}
