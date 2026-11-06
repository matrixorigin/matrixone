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

package shard

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbshard "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// The partitioned carrier selects a real remote service. The request names a
// regular data table so this exercises RPC plus the registered engine handlers,
// without implying that combined partitioned relations support statement views.
func TestCandidateReaderThroughRemoteEngineHandlers(t *testing.T) {
	savedCommit := version.CommitID
	version.CommitID = "candidate-reader-integration"
	t.Cleanup(func() { version.CommitID = savedCommit })
	require.NoError(t, runShardClusterTestWithReuse(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := strings.ToLower(testutils.GetDatabaseName(t))
		carrier := mustCreatePartitionBasedTable(t, c, db, 3)
		service := shardservice.GetService(cn.ServiceID())
		clusterInfo := clusterservice.GetMOCluster(cn.ServiceID())
		readyCtx, stopReady := context.WithTimeout(t.Context(), 30*time.Second)
		defer stopReady()
		var remote uint64
		require.Eventually(t, func() bool {
			_, shards, err := service.GetTableShards(carrier)
			if err != nil {
				return false
			}
			for _, shard := range shards {
				for _, replica := range shard.Replicas {
					if replica.CN == cn.ServiceID() || replica.State != pbshard.ReplicaState_Running {
						continue
					}
					// Allocation can precede this CN's metadata-cache refresh.
					// Wait for the same readiness required by the sender gate.
					compatible := false
					err := clusterservice.GetCNServiceWithoutWorkingStateWithContext(readyCtx, clusterInfo,
						clusterservice.NewServiceIDSelector(replica.CN), func(info metadata.CNService) bool {
							compatible = info.CommitID == version.CommitID
							return false
						})
					if err == nil && compatible {
						remote = shard.ShardID
						return true
					}
				}
			}
			return false
		}, 30*time.Second, 100*time.Millisecond, "carrier must have an allocated remote replica with published commit metadata")
		// Two separately persisted one-row objects are the minimum that proves
		// the range predicate prunes a block rather than merely filtering rows.
		testutils.ExecSQL(t, db, cn,
			"create table candidate_data (pk float(8,2) primary key, payload int)",
			"insert into candidate_data values (1.23,7)")
		testutils.ExecSQL(t, db, cn, "select mo_ctl('dn','flush','"+db+".candidate_data')")
		testutils.ExecSQL(t, db, cn, "insert into candidate_data values (9.87,9)")
		committed := testutils.ExecSQL(t, db, cn, "select mo_ctl('dn','flush','"+db+".candidate_data')")
		testutils.WaitClusterAppliedTo(t, c, committed)

		ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), 0), time.Minute)
		defer cancel()
		svc := cn.RawService().(cnservice.Service)
		err = svc.GetSQLExecutor().ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			// Establish the transaction/process through the normal execution owner.
			res, err := tx.Exec("select pk from candidate_data", executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			proc := tx.Txn().GetWorkspace().(*disttae.Transaction).GetProc()
			database, err := svc.GetEngine().Database(ctx, db, tx.Txn())
			require.NoError(t, err)
			relation, err := database.Relation(ctx, "candidate_data", nil)
			require.NoError(t, err)
			full, err := relation.Ranges(ctx, engine.RangesParam{Policy: engine.Policy_CollectCommittedData})
			require.NoError(t, err)
			typ := plan.Type{Id: int32(types.T_float32), Width: 8, Scale: 2}
			lower, upper := float32(1.2), float32(1.3)
			bound := func(value float32) *plan.Expr {
				return &plan.Expr{Typ: typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: append([]byte(nil), types.EncodeFloat32(&value)...)}}}
			}
			ge, err := plan2.BindFuncExprImplByPlanExpr(ctx, ">=", []*plan.Expr{readutil.NewColumnExpr(0, typ, "pk"), bound(lower)})
			require.NoError(t, err)
			le, err := plan2.BindFuncExprImplByPlanExpr(ctx, "<=", []*plan.Expr{readutil.NewColumnExpr(0, typ, "pk"), bound(upper)})
			require.NoError(t, err)
			filter, err := plan2.BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{ge, le})
			require.NoError(t, err)
			columns := []string{"pk", "payload", catalog.Row_ID}
			makeBatch := func() *batch.Batch {
				b := batch.NewWithSize(3)
				b.Vecs[0] = vector.NewVec(types.New(types.T_float32, 8, 2))
				b.Vecs[1] = vector.NewVec(types.T_int32.ToType())
				b.Vecs[2] = vector.NewVec(types.T_Rowid.ToType())
				return b
			}
			readers, err := relation.BuildReaders(ctx, proc, nil, full, 1, 0, false, engine.Policy_CheckCommittedOnly, engine.FilterHint{})
			require.NoError(t, err)
			require.Len(t, readers, 1)
			defer func() { require.NoError(t, readers[0].Close()) }()
			reference := make(map[uint32]types.Rowid)
			func() {
				b := makeBatch()
				defer b.Clean(proc.Mp())
				for {
					b.CleanOnlyData()
					end, err := readers[0].Read(ctx, columns, nil, proc.Mp(), b)
					require.NoError(t, err)
					if end {
						break
					}
					for i := 0; i < b.RowCount(); i++ {
						reference[math.Float32bits(vector.GetFixedAtNoTypeCheck[float32](b.Vecs[0], i))] = vector.GetFixedAtNoTypeCheck[types.Rowid](b.Vecs[2], i)
					}
				}
			}()
			require.Len(t, reference, 2)
			info, err := proc.BuildProcessInfo("candidate reader handler integration")
			require.NoError(t, err)
			param := pbshard.ReadParam{Process: info, TxnTable: pbshard.TxnTable{DatabaseID: relation.GetDBID(ctx), DatabaseName: db, TableName: "candidate_data"}}
			read := func(readCtx context.Context, method int, param pbshard.ReadParam, apply func([]byte) error) error {
				return service.Read(readCtx, shardservice.ReadRequest{TableID: carrier, Method: method, Param: param, RequireSameCommit: true, Apply: apply},
					shardservice.DefaultOptions.Shard(remote).ReadAt(tx.Txn().SnapshotTS()))
			}
			param.RangesParam.Exprs = []*plan.Expr{filter}
			param.RangesParam.DataCollectPolicy = engine.Policy_CollectCommittedData
			var selected []byte
			require.NoError(t, read(ctx, shardservice.ReadRanges, param, func(response []byte) error {
				selected = append([]byte(nil), response...)
				return nil
			}))
			ranges, err := readutil.UnmarshalRelationData(selected)
			require.NoError(t, err)
			require.Less(t, ranges.DataCnt(), full.DataCnt(), "remote range handler prunes the unrelated persisted block")
			param.ReaderBuildParam.RelData = selected
			param.ReaderBuildParam.Expr = filter
			param.ReaderBuildParam.TombstoneApplyPolicy = int32(engine.Policy_CheckCommittedOnly)
			var stream []byte
			require.NoError(t, read(ctx, shardservice.ReadBuildReader, param, func(response []byte) error {
				require.Len(t, response, len(types.Uuid{}))
				stream = append([]byte(nil), response...)
				return nil
			}))
			// Register cleanup before any assertion on the stream's contents.
			defer func() {
				cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer stop()
				param.ReadCloseParam.Uuid = stream
				require.NoError(t, read(cleanupCtx, shardservice.ReadClose, param, func([]byte) error { return nil }))
			}()
			param.ReadNextParam.Uuid = stream
			param.ReadNextParam.Columns = columns
			var keys []uint32
			for end := false; !end; {
				require.NoError(t, read(ctx, shardservice.ReadNext, param, func(response []byte) error {
					require.NotEmpty(t, response)
					end = types.DecodeBool(response)
					if end {
						return nil
					}
					require.GreaterOrEqual(t, len(response), 5)
					size := int(types.DecodeUint32(response[1:5]))
					require.Equal(t, len(response)-5, size)
					b := &batch.Batch{}
					defer b.Clean(proc.Mp())
					if err := b.UnmarshalBinaryWithAnyMp(response[5:], proc.Mp()); err != nil {
						return err
					}
					for i := 0; i < b.RowCount(); i++ {
						key := math.Float32bits(vector.GetFixedAtNoTypeCheck[float32](b.Vecs[0], i))
						keys = append(keys, key)
						id, exists := reference[key]
						require.True(t, exists)
						require.Equal(t, id, vector.GetFixedAtNoTypeCheck[types.Rowid](b.Vecs[2], i))
						require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](b.Vecs[1], i))
					}
					return nil
				}))
			}
			require.Equal(t, []uint32{math.Float32bits(1.23)}, keys)
			return nil
		}, executor.Options{}.WithDatabase(db).WithMinCommittedTS(committed))
		require.NoError(t, err)
	}, false))
}
