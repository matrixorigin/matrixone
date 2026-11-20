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

package dml

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func TestDataBranchMergeWithCheckpointCatalog(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 3*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		db.SetMaxOpenConns(1)
		execSQLDB(t, ctx, db, "set role moadmin")

		beforeCreate := types.BuildTS(time.Now().UnixNano(), 0)
		dbName := testutils.GetDatabaseName(t)
		execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
		execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			execSQLDB(t, ctx, db, "use mo_catalog")
			execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		execSQLDB(t, ctx, db, "create table customers (id int primary key, name varchar(32))")
		execSQLDB(t, ctx, db, "insert into customers values (1,'汉德'),(2,'two'),(3,'three'),(4,'four'),(5,'five'),(6,'six')")
		execSQLDB(t, ctx, db, "data branch create table preview from customers")
		execSQLDB(t, ctx, db, "update preview set name='汉得' where id=1")
		// Persist unrelated catalog insertions and deletions alongside the branch.
		execSQLDB(t, ctx, db, "create table a_unrelated (id int primary key)")
		execSQLDB(t, ctx, db, "create table z_unrelated (id int primary key)")
		execSQLDB(t, ctx, db, "drop table a_unrelated")
		gcTS := types.BuildTS(time.Now().UnixNano(), 0)
		queryStringRows(t, ctx, db, "select mo_ctl('dn','checkpoint','')")

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		service := cn.RawService().(cnservice.Service)
		eng := service.GetEngine().(*disttae.Engine)
		// Exercise the catalog's history boundary without truncating unrelated
		// system tables or advancing beyond the completed checkpoint.
		part := eng.GetOrCreateLatestPart(ctx, uint64(catalog.System_Account), catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID)
		require.NoError(t, part.Truncate(ctx, [2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}, gcTS, nil))
		read, err := service.GetTxnClient().New(ctx, types.TS{}.ToTimestamp())
		require.NoError(t, err)
		require.NoError(t, eng.New(ctx, read))
		_, _, rel, err := eng.GetRelationById(ctx, read, catalog.MO_TABLES_ID)
		require.NoError(t, err)
		stateStart, err := disttae.GetPartitionStateStart(ctx, rel)
		require.NoError(t, err)
		require.NoError(t, read.Rollback(ctx))
		require.True(t, stateStart.GT(&beforeCreate), "the catalog history must require snapshot recovery")

		start := time.Now()
		execSQLDB(t, ctx, db, "data branch merge preview into customers when conflict skip")
		t.Logf("six-row customer rename with checkpoint catalog: %s", time.Since(start))
		want := [][]string{{"1", "汉得"}, {"2", "two"}, {"3", "three"}, {"4", "four"}, {"5", "five"}, {"6", "six"}}
		require.Equal(t, want, queryStringRows(t, ctx, db, "select id,name from customers order by id"))
		// Repeating the merge must preserve every row and the already applied name.
		execSQLDB(t, ctx, db, "data branch merge preview into customers when conflict skip")
		require.Equal(t, want, queryStringRows(t, ctx, db, "select id,name from customers order by id"))

		// Catalog pruning must not change row-level conflict, insert or delete semantics.
		execSQLDB(t, ctx, db, "data branch create table edits from customers")
		execSQLDB(t, ctx, db, "update edits set name='branch' where id=2")
		execSQLDB(t, ctx, db, "update customers set name='main' where id=2")
		execSQLDB(t, ctx, db, "delete from edits where id=3")
		execSQLDB(t, ctx, db, "insert into edits values (7,'seven')")
		execSQLDB(t, ctx, db, "data branch merge edits into customers when conflict skip")
		require.Equal(t, [][]string{{"1", "汉得"}, {"2", "main"}, {"4", "four"}, {"5", "five"}, {"6", "six"}, {"7", "seven"}},
			queryStringRows(t, ctx, db, "select id,name from customers order by id"))
	})
}
