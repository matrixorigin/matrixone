//go:build mo_m0

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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestM0DefaultFalseCopySQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer func() { _ = db.Close() }()
		const schema = "m0_default_copy"
		const table = schema + ".customer"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+table+m0RollbackCustomerSchema)
		m0Exec(t, ctx, db, "insert into "+table+" values (1,1,1,50000,100,10),(2,1,1,50000,100,NULL)")
		for step := range 2 {
			m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
			beforeID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
			beforeObjects := m0ObjectFingerprints(t, ctx, db, table)
			require.NotEmpty(t, beforeObjects)
			column := fmt.Sprintf("c_cleaned_t0_s%d", step)
			m0Exec(t, ctx, db, "alter table "+table+" add column "+column+" boolean default false")
			afterID, afterVersion := m0TableGeneration(t, ctx, db, schema, "customer")
			require.NotEqual(t, beforeID, afterID, "control must replace the physical relation through COPY")
			require.Zero(t, afterVersion)
			m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
			afterObjects := m0ObjectFingerprints(t, ctx, db, table)
			require.NotEmpty(t, afterObjects)
			require.NotEqual(t, beforeObjects, afterObjects)
			require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where "+column+" = false"))
			m0Exec(t, ctx, db, "update "+table+" set c_balance=0 where c_balance is null")
		}
		_, err = db.ExecContext(ctx, "alter table "+table+" add column rejected boolean default false, algorithm=inplace")
		require.Error(t, err)
		require.Equal(t, 0, m0Count(t, ctx, db, "select count(*) from "+table+" where c_balance is null"))
	})
}
