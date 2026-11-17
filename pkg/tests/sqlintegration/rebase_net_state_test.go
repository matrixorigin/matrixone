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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestNativeRebaseWriteBackHasNoNetChanges(t *testing.T) {
	count := captureRebasePhases(t)
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_net_bu", "drop snapshot if exists nr_net_bf",
			"drop snapshot if exists nr_net_fh", "drop snapshot if exists nr_net_uh",
			"drop database if exists nr_net_result", "drop database if exists nr_net_f",
			"drop database if exists nr_net_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_net_"}
		f.exec("create database nr_net_u")
		f.exec("create table nr_net_u.t(id int primary key,v int)")
		f.exec("insert into nr_net_u.t values(1,10),(2,20)")
		bu := f.snapshot("nr_net_u", "bu")
		f.exec("data branch create database nr_net_f from " + bu)
		bf := f.snapshot("nr_net_f", "bf")
		for _, query := range []string{
			"update nr_net_f.t set v=11 where id=1", "update nr_net_f.t set v=10 where id=1",
			"insert into nr_net_f.t values(3,30)", "delete from nr_net_f.t where id=3",
			"update nr_net_u.t set v=21 where id=2", "update nr_net_u.t set v=20 where id=2",
			"insert into nr_net_u.t values(4,40)", "delete from nr_net_u.t where id=4",
		} {
			f.exec(query) // Separate commits leave physical history, not net changes.
		}
		fh, uh := f.snapshot("nr_net_f", "fh"), f.snapshot("nr_net_u", "uh")
		_, counts, err := f.rebaseTables("nr_net_result", fh, uh, bf, bu, "", 1)
		require.NoError(t, err)
		require.Equal(t, [6]uint64{}, counts)
		for _, database := range []string{"nr_net_result", "nr_net_f", "nr_net_u"} {
			require.Equal(t, []string{"1:10", "2:20"}, f.rows(database, "t"))
		}
	})
	require.Positive(t, count("delta_keys"))
}
