// Copyright 2025 Matrix Origin
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

package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func Test_prepareCloneViewSnapshot(t *testing.T) {
	original := &plan.Snapshot{
		Tenant: &plan.SnapshotTenant{TenantID: 1001},
	}

	rewritten := prepareCloneViewSnapshot(original, 42)
	require.NotNil(t, rewritten)
	require.NotNil(t, rewritten.TS)
	require.Equal(t, int64(42), rewritten.TS.PhysicalTime)
	require.Equal(t, uint32(1001), rewritten.Tenant.TenantID)
	require.Nil(t, original.TS)

	valid := &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 99},
		Tenant: &plan.SnapshotTenant{TenantID: 2002},
	}
	require.Same(t, valid, prepareCloneViewSnapshot(valid, 42))

	fromNil := prepareCloneViewSnapshot(nil, 24)
	require.NotNil(t, fromNil)
	require.NotNil(t, fromNil.TS)
	require.Equal(t, int64(24), fromNil.TS.PhysicalTime)
	require.Nil(t, fromNil.Tenant)

	require.Nil(t, prepareCloneViewSnapshot(nil, 0))
}

func Test_rewriteCloneViewInfos(t *testing.T) {
	fallbackKey := "pub_db#"
	viewMap := map[string]*tableInfo{
		genKey("pub_db", "v1"): {
			dbName:    "pub_db",
			tblName:   "v1",
			typ:       view,
			createSql: "create view `pub_db`.`v1` as select 'pub_db' as marker, a from `pub_db`.`t1`",
		},
		fallbackKey: {
			dbName:    "pub_db",
			tblName:   "legacy_v",
			typ:       view,
			createSql: "create view `pub_db`.`legacy_v` as select 1",
		},
	}
	sortedViews := []string{
		genKey("other_db", "dep_v"),
		fallbackKey,
		genKey("pub_db", "v1"),
	}

	rewrittenViewMap, rewrittenViews, err := rewriteCloneViewInfos(viewMap, sortedViews, "pub_db", "clone_db")
	require.NoError(t, err)
	require.Equal(t, []string{
		genKey("other_db", "dep_v"),
		"clone_db#",
		genKey("clone_db", "v1"),
	}, rewrittenViews)

	info, ok := rewrittenViewMap[genKey("clone_db", "v1")]
	require.True(t, ok)
	require.Equal(t, "clone_db", info.dbName)
	require.Equal(t, "create view clone_db.v1 as select 'pub_db' as marker, a from clone_db.t1;", info.createSql)

	fallbackInfo, ok := rewrittenViewMap["clone_db#"]
	require.True(t, ok)
	require.Equal(t, "clone_db", fallbackInfo.dbName)
	require.Equal(t, "create view clone_db.legacy_v as select 1;", fallbackInfo.createSql)

	require.Equal(t, "pub_db", viewMap[genKey("pub_db", "v1")].dbName)
	require.Equal(t, "create view `pub_db`.`v1` as select 'pub_db' as marker, a from `pub_db`.`t1`", viewMap[genKey("pub_db", "v1")].createSql)
	require.Equal(t, "pub_db", viewMap[fallbackKey].dbName)
	require.Equal(t, "create view `pub_db`.`legacy_v` as select 1", viewMap[fallbackKey].createSql)
}

func Test_rewriteCloneCreateSQL_RewritesOnlyTableNames(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		`create view pub_db.v as
			with c as (select * from pub_db.cte_t)
			select pub_db as pub_db,
			       (select max(id) from pub_db.proj_t) as m,
			       case when exists (select 1 from pub_db.case_t) then 1 else 0 end as c,
			       ((select max(id) from pub_db.null_t) is null) as n
			  from c
			  join pub_db.join_t as j on j.id in (select id from pub_db.on_t)
			 where exists (select 1 from pub_db.where_t)
			 group by pub_db
			having count(*) > (select count(*) from pub_db.having_t)`,
		"pub_db",
		"clone_db",
		false,
	)
	require.NoError(t, err)
	require.Contains(t, got, "create view clone_db.v")
	require.Contains(t, got, "from clone_db.cte_t")
	require.Contains(t, got, "from clone_db.proj_t")
	require.Contains(t, got, "from clone_db.case_t")
	require.Contains(t, got, "from clone_db.null_t")
	require.Contains(t, got, "join clone_db.join_t")
	require.Contains(t, got, "from clone_db.on_t")
	require.Contains(t, got, "from clone_db.where_t")
	require.Contains(t, got, "from clone_db.having_t")
	require.NotContains(t, got, "pub_db.cte_t")
	require.NotContains(t, got, "pub_db.proj_t")
	require.NotContains(t, got, "pub_db.case_t")
	require.NotContains(t, got, "pub_db.null_t")
	require.NotContains(t, got, "pub_db.join_t")
	require.NotContains(t, got, "pub_db.on_t")
	require.NotContains(t, got, "pub_db.where_t")
	require.NotContains(t, got, "pub_db.having_t")
	require.NotContains(t, got, "select clone_db as")
	require.Contains(t, got, "as pub_db")
}

func Test_rewriteCloneCreateSQL_PreservesUnqualifiedViewFormat(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		"create view v1 as select * from t1;",
		"pub_db",
		"clone_db",
		false,
	)
	require.NoError(t, err)
	require.Equal(t, "create view v1 as select * from t1;", got)
}

func Test_rewriteCloneCreateSQL_QuotesSystemViewIdentifiers(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		"create view information_schema.v as select mt.`constraint` from mo_catalog.mo_tables as mt",
		"information_schema",
		"information_schema_new",
		true,
	)
	require.NoError(t, err)
	require.Contains(t, got, "`mt`.`constraint`")
	require.NotContains(t, got, "mt.constraint")

	_, err = rewriteCloneCreateSQL(got, "information_schema_new", "information_schema_next", true)
	require.NoError(t, err)
}
