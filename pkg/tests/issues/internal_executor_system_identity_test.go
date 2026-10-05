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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

func TestInternalExecutorSystemIdentity(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sys, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer func() { require.NoError(t, sys.Close()) }()
		const database = "internal_executor_system_identity"
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			execSQLRequire(t, cleanup, sys, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, sys, "create database "+database)
		execSQLRequire(t, ctx, sys, "create table "+database+".t(id int primary key)")
		internal := frontend.NewInternalExecutor(cn.ServiceID())
		for i, tc := range []struct {
			name string
			ctx  context.Context
		}{
			{"sys account", defines.AttachAccountId(ctx, 0)},
			{"explicit system IDs", defines.AttachAccount(ctx, 0, 0, 0)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result := internal.Query(tc.ctx, "select user_name from mo_catalog.mo_user where user_id=0", ie.SessionOverrideOptions{})
				require.NoError(t, result.Error())
				require.Equal(t, uint64(1), result.RowCount())
				user, err := result.GetString(tc.ctx, 0, 0)
				require.NoError(t, err)
				require.Equal(t, "root", user)
				require.NoError(t, internal.Exec(tc.ctx, fmt.Sprintf("insert into %s.t values(%d)", database, i+1), ie.SessionOverrideOptions{}))
				result = internal.Query(tc.ctx, fmt.Sprintf("select id from %s.t where id=%d", database, i+1), ie.SessionOverrideOptions{})
				require.NoError(t, result.Error())
				require.Equal(t, uint64(1), result.RowCount())
				id, err := result.GetUint64(tc.ctx, 0, 0)
				require.NoError(t, err)
				require.Equal(t, uint64(i+1), id)
			})
		}
		// A caller-supplied name is not the anonymous system identity, even
		// when it carries root's numeric IDs. Retain the real catalog check.
		t.Run("explicit mismatched name remains denied", func(t *testing.T) {
			sysCtx := defines.AttachAccount(ctx, 0, 0, 0)
			opts := ie.NewOptsBuilder().Username("internal").AccountId(0).UserId(0).DefaultRoleId(0).Finish()
			result := internal.Query(sysCtx, "select id from "+database+".t", opts)
			require.ErrorContains(t, result.Error(), "authenticated user no longer matches current catalog")
			require.ErrorContains(t, internal.Exec(sysCtx, "insert into "+database+".t values(99)", opts), "authenticated user no longer matches current catalog")
			result = internal.Query(sysCtx, "select count(*) from "+database+".t", ie.SessionOverrideOptions{})
			require.NoError(t, result.Error(), "default identity must recover after an explicit identity fails")
			count, err := result.GetUint64(sysCtx, 0, 0)
			require.NoError(t, err)
			require.Equal(t, uint64(2), count, "denied writes must have no side effects")
		})
		t.Run("non-administrator role cannot borrow default administrator identity", func(t *testing.T) {
			publicCtx := defines.AttachAccount(ctx, 0, 0, 1)
			result := internal.Query(publicCtx, "show accounts", ie.SessionOverrideOptions{})
			require.ErrorContains(t, result.Error(), "do not have privilege")
			require.ErrorContains(t, internal.Exec(publicCtx, "insert into "+database+".t values(99)", ie.SessionOverrideOptions{}), "do not have privilege")
		})
	})
}
