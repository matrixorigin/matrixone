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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestShowMetadataAdmissionIsolation(t *testing.T) {
	c, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(s embed.ServiceOperator) {
		if s.ServiceType() == metadata.ServiceType_CN {
			s.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.Frontend.SkipCheckUser = false })
		}
	}))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	testutils.CreateAccount(t, c, "show_admission", "111")
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port
	open := func(user string) *sql.DB {
		db, err := sql.Open("mysql", fmt.Sprintf("show_admission#%s:111@tcp(127.0.0.1:%d)/", user, port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	admin := open("root#accountadmin")
	for _, q := range []string{"create database d", "create table d.allowed (id int)", "create table d.denied (id int)", "create role r", "create user reader identified by '111' default role r", "grant connect on account * to r", "grant select on table d.allowed to r"} {
		_, err = admin.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	reader := open("reader#r")
	names := func(query string) []string {
		rows, err := reader.QueryContext(ctx, query)
		require.NoError(t, err, query)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			got = append(got, name)
		}
		require.NoError(t, rows.Err())
		return got
	}
	require.Equal(t, []string{"allowed"}, names("show tables from d"))
	for _, q := range []string{"show tables from d where exists (select 1 from d.denied)", "show columns from d.allowed where Field = (select cast(id as varchar) from d.denied limit 1)"} {
		_, err := reader.ExecContext(ctx, q)
		require.ErrorContains(t, err, "row-local predicate", q)
	}
	_, err = reader.ExecContext(ctx, "select * from d.denied")
	require.Error(t, err)
	_, err = admin.ExecContext(ctx, "grant show tables on database d to r")
	require.NoError(t, err)
	require.Equal(t, []string{"allowed", "denied"}, names("show tables from d"))
	_, err = reader.ExecContext(ctx, "prepare s from 'show tables from d'")
	require.NoError(t, err)
	require.Equal(t, []string{"allowed", "denied"}, names("execute s"))
	_, err = admin.ExecContext(ctx, "revoke show tables on database d from r")
	require.NoError(t, err)
	require.Equal(t, []string{"allowed"}, names("execute s"))
}
