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
}

func Test_rewriteCloneViewInfos(t *testing.T) {
	viewMap := map[string]*tableInfo{
		genKey("pub_db", "v1"): {
			dbName:    "pub_db",
			tblName:   "v1",
			typ:       view,
			createSql: "create view `pub_db`.`v1` as select * from `pub_db`.`t1`",
		},
	}
	sortedViews := []string{
		genKey("other_db", "dep_v"),
		genKey("pub_db", "v1"),
	}

	rewrittenViewMap, rewrittenViews := rewriteCloneViewInfos(viewMap, sortedViews, "pub_db", "clone_db")
	require.Equal(t, []string{
		genKey("other_db", "dep_v"),
		genKey("clone_db", "v1"),
	}, rewrittenViews)

	info, ok := rewrittenViewMap[genKey("clone_db", "v1")]
	require.True(t, ok)
	require.Equal(t, "clone_db", info.dbName)
	require.Equal(t, "create view `clone_db`.`v1` as select * from `clone_db`.`t1`", info.createSql)

	require.Equal(t, "pub_db", viewMap[genKey("pub_db", "v1")].dbName)
	require.Equal(t, "create view `pub_db`.`v1` as select * from `pub_db`.`t1`", viewMap[genKey("pub_db", "v1")].createSql)
}
