//go:build mo_m0

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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

// Omit authority at CREATE to model a pre-contract relation. No successful
// authorization is injected, and no catalog DML protection is bypassed.
func TestM0LegacyRootCopyEstablishesContract(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_legacy_copy"
		injector := openM0SQL(t, cn)
		defer injector.Close()
		const omission = "fj/m0/evolution/omit-create"
		defer m0RemoveFaultPointQuiet(context.Background(), injector, omission)
		m0AddFaultPoint(t, ctx, injector, omission, "echo", "")
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		m0RemoveFaultPoint(t, ctx, injector, omission)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		id, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		var raw []byte
		require.NoError(t, db.QueryRowContext(ctx, "select extra_info from mo_catalog.mo_tables where rel_id=?", id).Scan(&raw))
		var extra api.SchemaExtra
		require.NoError(t, extra.Unmarshal(raw))
		require.Nil(t, extra.M0Evolution)
		m0Exec(t, ctx, db, "alter table "+table+" add column copied int comment 'kept'")
		copiedID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		require.NotEqual(t, id, copiedID, "uncontracted ADD must perform COPY")
		require.NoError(t, db.QueryRowContext(ctx, "select extra_info from mo_catalog.mo_tables where rel_id=?", copiedID).Scan(&raw))
		require.NoError(t, extra.Unmarshal(raw))
		require.NotNil(t, extra.M0Evolution)
		require.Equal(t, uint32(0), extra.M0Evolution.LayoutGeneration)
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+table+" where copied is null"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from information_schema.columns where table_schema='"+schema+"' and table_name='customer' and column_name='copied' and column_comment='kept'"))
		m0Exec(t, ctx, db, "alter table "+table+" add column metadata_only int")
		finalID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, copiedID, finalID, "new root must admit subsequent M0 ADD")
	})
}

func TestM0ContractCNWorkspaceWriterSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_contract_cn_writer"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "alter table "+table+" add column tail int")
		const flush = "fj/cn/workspace_force_flush"
		defer m0RemoveFaultPointQuiet(context.Background(), db, flush)
		m0AddFaultPoint(t, ctx, db, flush, "echo", "")
		m0Exec(t, ctx, db, "insert into "+table+" values(4,4,4,80000,2000,50,9)")
		m0RemoveFaultPoint(t, ctx, db, flush)
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+table))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where tail=9"))
		found := false
		for _, proof := range m0ObjectHeaderProofs(t, ctx, db, cn, table) {
			found = found || strings.Contains(proof, ":1:[7]")
		}
		require.True(t, found, "forced CN output must declare contract layout 1 and its complete physical prefix")
	})
}

func TestM0CommentPreservesLayoutGenerationSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_contract_comment"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "alter table "+table+" add column tail int")
		before := m0ReadEvolution(t, ctx, db, schema)
		id, version := m0TableGeneration(t, ctx, db, schema, "customer")
		objects := m0ObjectFingerprints(t, ctx, db, table)
		m0Exec(t, ctx, db, "alter table "+table+" comment='metadata only'")
		afterID, afterVersion := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, id, afterID)
		require.Greater(t, afterVersion, version)
		require.Equal(t, before, m0ReadEvolution(t, ctx, db, schema))
		require.Equal(t, objects, m0ObjectFingerprints(t, ctx, db, table))
		require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+table+" where tail is null"))
		m0Exec(t, ctx, db, "alter table "+table+" add column after_comment int")
		finalID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, id, finalID, "COMMENT must not force later ADD to COPY")
		require.Equal(t, uint32(2), m0ReadEvolution(t, ctx, db, schema).LayoutGeneration)
		m0Exec(t, ctx, db, "update "+table+" set after_comment=11 where c_id=1")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where after_comment=11"))
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where after_comment is null"))
	})
}
