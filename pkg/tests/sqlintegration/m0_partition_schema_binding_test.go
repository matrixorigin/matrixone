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
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// The parent and its independently created members must use a compatible
// physical column mapping. Compare definitions from one real catalog snapshot,
// including the distinct index relations used by partition index scans.
func TestM0PartitionMemberColumnMappingSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "m0_partition_column_mapping"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(statement string) {
			t.Helper()
			_, err := db.ExecContext(ctx, statement)
			require.NoError(t, err)
		}
		exec("create database " + schema)
		exec("create table " + schema + ".t(a int not null,b varchar(16) default 'x',c decimal(12,3),g int generated always as (a+1) stored,primary key(a),key idx_b(b)) partition by list(a) (partition p0 values in (1,2),partition p1 values in (3,4))")
		exec("insert into " + schema + ".t(a,b,c) values (1,'x',1.234),(3,'y',2.345)")

		type column struct {
			name                      string
			id                        uint64
			seq                       uint32
			typ, width, scale         int32
			charset, collation        uint32
			hidden, primary, nullable bool
		}
		project := func(def *plan.TableDef) []column {
			out := make([]column, 0, len(def.Cols))
			for _, col := range def.Cols {
				out = append(out, column{col.Name, col.ColId, col.Seqnum, col.Typ.Id,
					col.Typ.Width, col.Typ.Scale, col.Typ.Charset, col.Typ.CollationVersion,
					col.Hidden, col.Primary, col.Default != nil && col.Default.NullAbility})
			}
			return out
		}
		nullDefault := func(d *plan.Default) bool {
			if d == nil || !d.NullAbility {
				return false
			}
			origin := strings.TrimSpace(d.OriginString)
			if d.Expr == nil {
				return origin == ""
			}
			lit := d.Expr.GetLit()
			return lit != nil && lit.Isnull && (origin == "" || strings.EqualFold(origin, "null"))
		}
		service := cn.RawService().(cnservice.Service)
		catalogCtx := defines.AttachAccountId(ctx, 0)
		check := func(names ...string) {
			t.Helper()
			require.NoError(t, service.GetSQLExecutor().ExecTxn(catalogCtx, func(txn executor.TxnExecutor) error {
				database, err := service.GetEngine().Database(catalogCtx, schema, txn.Txn())
				if err != nil {
					return err
				}
				parent, err := database.Relation(catalogCtx, "t", nil)
				if err != nil {
					return err
				}
				parentColumns := project(parent.GetTableDef(catalogCtx))
				storage := partitionservice.NewStorage(service.ID(), service.GetSQLExecutor(), service.GetEngine())
				metadata, found, err := storage.GetMetadata(catalogCtx, parent.GetTableID(catalogCtx), txn.Txn())
				if err != nil {
					return err
				}
				require.True(t, found)
				memberNames := make(map[string]string, len(metadata.Partitions))
				for _, member := range metadata.Partitions {
					memberNames[member.Name] = member.PartitionTableName
				}
				parentIndex := parent.GetExtraInfo().IndexTables
				require.Len(t, parentIndex, 1)
				_, _, index, err := service.GetEngine().GetRelationById(catalogCtx, txn.Txn(), parentIndex[0])
				if err != nil {
					return err
				}
				indexColumns := project(index.GetTableDef(catalogCtx))
				for _, name := range names {
					memberName, ok := memberNames[name]
					require.True(t, ok, "missing partition %s", name)
					member, err := database.Relation(catalogCtx, memberName, nil)
					if err != nil {
						return err
					}
					require.NotNil(t, member.GetExtraInfo())
					require.True(t, features.IsPartition(member.GetExtraInfo().FeatureFlag), "partition feature missing for %s", name)
					require.Equal(t, parent.GetTableID(catalogCtx), member.GetExtraInfo().ParentTableID, "partition parent for %s", name)
					memberDef := member.GetTableDef(catalogCtx)
					require.Equal(t, parentColumns, project(memberDef), "base member %s", name)
					for i, col := range parent.GetTableDef(catalogCtx).Cols {
						memberDefault := memberDef.Cols[i].Default
						require.True(t, proto.Equal(col.Default, memberDefault) || (nullDefault(col.Default) && nullDefault(memberDefault)), "default for %s column %s", name, col.Name)
						require.True(t, proto.Equal(col.GeneratedCol, memberDef.Cols[i].GeneratedCol), "generated expression for %s column %s", name, col.Name)
					}
					memberIndex := member.GetExtraInfo().IndexTables
					require.Len(t, memberIndex, 1)
					_, _, childIndex, err := service.GetEngine().GetRelationById(catalogCtx, txn.Txn(), memberIndex[0])
					if err != nil {
						return err
					}
					require.NotNil(t, childIndex.GetExtraInfo())
					require.True(t, features.IsIndexTable(childIndex.GetExtraInfo().FeatureFlag), "index feature missing for %s", name)
					require.Equal(t, member.GetTableID(catalogCtx), childIndex.GetExtraInfo().ParentTableID, "index parent for %s", name)
					require.Equal(t, indexColumns, project(childIndex.GetTableDef(catalogCtx)), "index member %s", name)
				}
				// Run the receiver's local guard through the real relation and
				// ranges owner in this same catalog transaction.
				ranges, err := parent.Ranges(m0read.WithRemoteMemberValidation(catalogCtx), engine.DefaultRangesParam)
				if err != nil {
					return err
				}
				require.NotNil(t, ranges)
				indexRanges, err := index.Ranges(m0read.WithRemoteMemberValidation(catalogCtx), engine.DefaultRangesParam)
				if err != nil {
					return err
				}
				require.NotNil(t, indexRanges)
				return nil
			}, executor.Options{}.WithWaitCommittedLogApplied()))
		}
		check("p0", "p1")
		exec("alter table " + schema + ".t add partition (partition p2 values in (5,6))")
		exec("insert into " + schema + ".t(a,b,c) values (5,'z',3.456)")
		check("p0", "p1", "p2")
		exec("alter table " + schema + ".t add column d bigint")
		check("p0", "p1", "p2")
		exec("alter table " + schema + ".t truncate partition p1")
		check("p0", "p1", "p2")
		rows, err := db.QueryContext(ctx, "select a from "+schema+".t order by a")
		require.NoError(t, err)
		defer rows.Close()
		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{1, 5}, ids)
		require.NoError(t, rows.Close())
	})
}
