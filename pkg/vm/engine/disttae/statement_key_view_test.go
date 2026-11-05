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

package disttae

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_shard"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestStatementKeyProbeUsesPhysicalBitsAndLogicalScale(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	typ := types.New(types.T_float32, 0, 2)
	def := &plan.TableDef{Name: "keys", Name2ColIndex: map[string]int32{"k": 0}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "k"},
		Cols: []*plan.ColDef{{Name: "k", Typ: plan.Type{Id: int32(types.T_float32), Scale: 2}, Primary: true}}}
	for _, physical := range []bool{false, true} {
		for _, value := range []float32{1.234, 0, math.Float32frombits(1 << 31), float32(math.Inf(1)), math.Float32frombits(0x7fa00001)} {
			func() {
				key := vector.NewVec(typ)
				defer key.Free(proc.Mp())
				require.NoError(t, vector.AppendFixed(key, value, false, proc.Mp()))
				expr, err := statementKeyProbe(t.Context(), &statementKeyTable{pk: 0, physical: physical, def: def}, key)
				require.NoError(t, err)
				require.Equal(t, math.Float32bits(value), math.Float32bits(vector.GetFixedAtNoTypeCheck[float32](key, 0)), "constructing a candidate filter preserves the residual key")
				if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
					require.Nil(t, expr)
					return
				}
				filter, err := readutil.ConstructBasePKFilter(expr, def, proc.Mp())
				require.NoError(t, err)
				defer filter.Cleanup()
				require.True(t, filter.Valid)
				require.Equal(t, types.T_float32, filter.Oid)
				if !physical {
					require.Equal(t, function.BETWEEN, filter.Op)
					lower, upper := types.DecodeFloat32(filter.LB), types.DecodeFloat32(filter.UB)
					normalize := types.NewFloat32ScaleNormalizer(2).Normalize
					require.Equal(t, normalize(value), normalize(lower))
					require.Equal(t, normalize(value), normalize(upper))
				} else if value == 0 {
					require.Equal(t, function.BETWEEN, filter.Op)
					require.Equal(t, uint32(1<<31), math.Float32bits(types.DecodeFloat32(filter.LB)))
					require.Zero(t, math.Float32bits(types.DecodeFloat32(filter.UB)))
				} else {
					require.Equal(t, function.IN, filter.Op)
					require.Equal(t, 1, filter.Vec.Length())
				}
			}()
		}
	}
}

func TestStatementKeyViewRemoteRangesHaveReaderLifetime(t *testing.T) {
	colexec.NewServer("")
	for _, tc := range []struct {
		physical bool
		blocks   int
		denyWire bool
	}{{true, 1, false}, {false, 1, false}, {true, 1339, false}, {true, 1339, true}} {
		t.Run(fmt.Sprintf("physical=%t/blocks=%d/deny-wire=%t", tc.physical, tc.blocks, tc.denyWire), func(t *testing.T) {
			physical := tc.physical
			txn, tbl := newDumpInsertTxnForTest(t)
			t.Cleanup(func() { cleanTxnWritesForTest(txn) })
			op := txn.op.(*mock_frontend.MockTxnOperator)
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
			op.EXPECT().IsSnapOp().Return(false).AnyTimes()
			op.EXPECT().Snapshot().Return(pbtxn.CNTxnSnapshot{}, nil).AnyTimes()
			txn.proc.Base.TxnOperator = op
			txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)
			packer := types.NewPacker()
			defer packer.Close()
			txn.engine.packerPool = fileservice.NewPool(1, func() *types.Packer { return packer }, func(p *types.Packer) { p.Reset() }, func(*types.Packer) {})
			fs, err := colexec.GetSharedFSFromProc(txn.proc)
			require.NoError(t, err)
			txn.engine.fs = fs
			tbl.eng = txn.engine
			tbl.remoteWorkspace = true
			tbl.createdInTxn = true
			tbl.primaryIdx = 0
			tbl.proc.Store(txn.proc)
			typ := types.New(types.T_float32, 0, 2)
			tbl.tableDef = &plan.TableDef{TblId: 42, Name: "tbl", DbName: "db", Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk", Names: []string{"pk"}}, Name2ColIndex: map[string]int32{"pk": 0, catalog.Row_ID: 1},
				Cols: []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(types.T_float32), Scale: 2}, Seqnum: 0}, {Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}, Seqnum: objectio.SEQNUM_ROWID}}}
			delegate := &txnTableDelegate{origin: tbl, isLocal: func() (bool, error) { return false, nil }}
			relations := []engine.Relation{delegate}
			if !physical {
				// The provider's first relation is the base table; subsequent relation
				// keys use SQL unique-key equality, including FLOAT32 scale normalization.
				base := &txnTable{accountId: 1, tableId: 43, tableName: "base", db: tbl.db, eng: txn.engine, createdInTxn: true, remoteWorkspace: true}
				base.proc.Store(txn.proc)
				base.tableDef = &plan.TableDef{TblId: 43, Name: "base", DbName: "db", Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk", Names: []string{"pk"}}, Name2ColIndex: map[string]int32{"pk": 0, catalog.Row_ID: 1},
					Cols: []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0}, {Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}, Seqnum: objectio.SEQNUM_ROWID}}}
				txn.tableOps.addCreateTable(genTableKey(1, "base", 7, "db"), 0, base)
				relations = []engine.Relation{&txnTableDelegate{origin: base, isLocal: func() (bool, error) { return true, nil }}, delegate}
			}
			service := mock_shard.NewMockShardService(gomock.NewController(t))
			delegate.shard.service = service
			failure := moerr.NewTxnNeedRetryNoCtx()
			failRanges := false
			ranges, builds := 0, 0
			service.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req shardservice.ReadRequest, _ shardservice.ReadOptions) error {
				switch req.Method {
				case shardservice.ReadRanges:
					ranges++
					require.True(t, req.RequireSameCommit)
					require.Len(t, req.Param.RangesParam.Exprs, 1)
					data := readutil.NewBlockListRelationData(0)
					for range tc.blocks {
						data.AppendBlockInfo(&objectio.BlockInfo{BlockID: types.Blockid{0: 93}})
					}
					encoded, err := data.MarshalBinary()
					require.NoError(t, err)
					err = req.Apply(encoded)
					clear(encoded)
					if err != nil {
						return err
					}
					if failRanges {
						return failure
					}
					return nil
				case shardservice.ReadBuildReader:
					builds++
					require.True(t, req.RequireSameCommit)
					require.NotNil(t, req.Param.ReaderBuildParam.Expr)
					data, err := readutil.UnmarshalRelationData(req.Param.ReaderBuildParam.RelData)
					require.NoError(t, err)
					require.Equal(t, types.Blockid{0: 93}, data.GetBlockInfo(0).BlockID)
					require.Nil(t, data.GetTombstones())
					require.Equal(t, tc.blocks, data.DataCnt())
					wireSize, err := readutil.BlockListEncodedSize(tc.blocks * objectio.BlockInfoSize)
					require.NoError(t, err)
					require.LessOrEqual(t, cap(req.Param.ReaderBuildParam.RelData), 2*wireSize+128)
					id := types.Uuid{1}
					return req.Apply(types.EncodeUuid(&id))
				case shardservice.ReadNext:
					data := batch.NewWithSize(2)
					defer data.Clean(txn.proc.Mp())
					data.Vecs[0] = vector.NewVec(typ)
					data.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
					values := []float32{math.Float32frombits(1 << 31), 0, 1.231}
					if !physical {
						values = []float32{1.231}
					}
					for i, value := range values {
						require.NoError(t, vector.AppendFixed(data.Vecs[0], value, false, txn.proc.Mp()))
						require.NoError(t, vector.AppendFixed(data.Vecs[1], types.Rowid{0: byte(10 + i)}, false, txn.proc.Mp()))
					}
					data.SetRowCount(len(values))
					encoded, err := data.MarshalBinary()
					require.NoError(t, err)
					end, size := false, uint32(len(encoded))
					frame := append(types.EncodeBool(&end), types.EncodeUint32(&size)...)
					frame = append(frame, encoded...)
					err = req.Apply(frame)
					clear(frame)
					return err
				case shardservice.ReadClose:
					return nil
				default:
					return moerr.NewInternalErrorNoCtx("unexpected float view read")
				}
			}).AnyTimes()
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.Open(16 << 20)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.Zero(t, account.Seal().Used)
				_, err := registry.Finalize(account)
				require.NoError(t, err)
				require.Zero(t, registry.LiveAllocationMetadata())
			})
			generation, err := txn.proc.GetHashBuildBudget()
			require.NoError(t, err)
			wireSize, err := readutil.BlockListEncodedSize(tc.blocks * objectio.BlockInfoSize)
			require.NoError(t, err)
			wireDenied := 0
			view, err := delegate.NewStatementKeyView(t.Context(), relations, txn.proc, account, func(n uint64) (engine.StatementViewReservation, error) {
				if tc.denyWire && n == uint64(wireSize)*2+128 {
					wireDenied++
					return nil, failure
				}
				return generation.ReserveMemory(t.Context(), n)
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			if tc.denyWire {
				initial := generation.Snapshot().Used
				key, err := vector.NewConstFixed(typ, float32(1.231), 1, txn.proc.Mp())
				require.NoError(t, err)
				defer key.Free(txn.proc.Mp())
				row, err := view.Lookup(t.Context(), 42, key, 0)
				require.Same(t, failure, err)
				require.Nil(t, row)
				require.Equal(t, 1, wireDenied)
				require.Equal(t, 1, ranges)
				require.Zero(t, builds, "wire denial precedes serialization and remote Build")
				require.Equal(t, initial, generation.Snapshot().Used, "both collectors released on denied wire admission")
				require.NoError(t, view.Close())
				require.Zero(t, generation.Snapshot().Used)
				return
			}
			values := []float32{0, math.Float32frombits(1 << 31), 1.231}
			wantIDs := []byte{11, 10, 12}
			if !physical {
				values = []float32{1.234, 1.232}
				wantIDs = []byte{10, 10}
			}
			var stableUsage uint64
			for i, value := range values {
				func() {
					key := vector.NewVec(typ)
					defer key.Free(txn.proc.Mp())
					require.NoError(t, vector.AppendFixed(key, value, false, txn.proc.Mp()))
					row, err := view.Lookup(t.Context(), 42, key, 0)
					require.NoError(t, err)
					require.NotNil(t, row)
					defer row.Clean(txn.proc.Mp())
					require.Equal(t, types.Rowid{0: wantIDs[i]}, vector.GetFixedAtNoTypeCheck[types.Rowid](row.Vecs[1], 0))
				}()
				if i == 0 {
					stableUsage = generation.Snapshot().Used
				}
				require.Equal(t, stableUsage, generation.Snapshot().Used, "finished probes retain no committed range copies")
			}
			require.Equal(t, len(values), ranges)
			require.Equal(t, len(values), builds)
			failRanges = true
			key := vector.NewVec(typ)
			defer key.Free(txn.proc.Mp())
			require.NoError(t, vector.AppendFixed(key, float32(1.234), false, txn.proc.Mp()))
			row, err := view.Lookup(t.Context(), 42, key, 0)
			require.Same(t, failure, err)
			require.Nil(t, row)
			require.Equal(t, stableUsage, generation.Snapshot().Used, "a later shard failure releases partially collected ranges")
			require.NoError(t, view.Close())
			require.Zero(t, generation.Snapshot().Used)
		})
	}
}

func TestStatementKeyViewOwnsPriorWorkspaceUntilClose(t *testing.T) {
	colexec.NewServer("")
	for _, terminal := range []string{"capture cancelled", "capture budget", "new selection", "statement rollback", "transaction rollback", "LOAD deleted", "LOAD delete error", "remote read error", "remote read cancelled", "remote view close", "remote close error", "remote data owned", "GC pool closed", "GC pool saturated", "GC delete retry", "GC quota"} {
		t.Run(terminal, func(t *testing.T) {
			txn, tbl := newDumpInsertTxnForTest(t)
			quota := int64(1 << 20)
			if terminal == "GC pool closed" {
				quota = statementObjectPinQuota
			}
			txn.engine.config.memThrottler = rscthrottler.NewMemThrottler("statement-pins", 1, rscthrottler.WithConstLimit(quota))
			t.Cleanup(func() { cleanTxnWritesForTest(txn) })
			op := txn.op.(*mock_frontend.MockTxnOperator)
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
			op.EXPECT().IsSnapOp().Return(false).AnyTimes()
			op.EXPECT().EnterRollbackStmt().AnyTimes()
			op.EXPECT().ExitRollbackStmt().AnyTimes()
			txn.tableCache = &sync.Map{}
			txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)
			packer := types.NewPacker()
			t.Cleanup(packer.Close)
			txn.engine.packerPool = fileservice.NewPool(1, func() *types.Packer { return packer }, func(p *types.Packer) { p.Reset() }, func(*types.Packer) {})
			fs, err := colexec.GetSharedFSFromProc(txn.proc)
			require.NoError(t, err)
			txn.engine.fs = fs
			tbl.eng = txn.engine
			tbl.remoteWorkspace = true
			tbl.createdInTxn = true
			tbl.primaryIdx = 0
			tbl.primarySeqnum = 0
			tbl.proc.Store(txn.proc)
			tbl.tableDef = &plan.TableDef{TblId: 42, Name: "tbl", DbName: "db", Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk", Names: []string{"pk"}},
				Name2ColIndex: map[string]int32{"pk": 0, catalog.Row_ID: 1},
				Cols:          []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0}, {Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}, Seqnum: objectio.SEQNUM_ROWID}}}
			// Three rows are sufficient to retain a prior deletion while later changes
			// challenge snapshot visibility and physical RowID lifetime independently.
			raw := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{1, 2, 3})
			txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, accountId: 1, databaseId: 7, tableId: 42, databaseName: "db", tableName: "tbl", bat: raw})
			txn.addBatchSelectionsLocked(raw, []int64{1})
			var loadFS *loadCleanupFileService
			var gcFS *loadCleanupFileService
			var loadName string
			loadErr := moerr.NewInternalErrorNoCtx("LOAD deletion failed")
			gcCase := strings.HasPrefix(terminal, "GC ")
			if terminal == "LOAD deleted" || terminal == "LOAD delete error" || gcCase {
				if !gcCase {
					loadFS = &loadCleanupFileService{FileService: fs, entered: make(chan struct{}), release: make(chan struct{})}
					if terminal == "LOAD delete error" {
						loadFS.err = loadErr
					}
					txn.engine.fs = loadFS
					txn.engine.cloneTxnCache = newCloneTxnCache()
					txn.SetCloneTxn(1)
				}
				stats := mockStatsList(t, 1)[0]
				loadName = stats.ObjectName().String()
				require.NoError(t, writeObjectToFS(t.Context(), fs, loadName))
				t.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					require.NoError(t, fs.Delete(cleanupCtx, loadName))
				})
				statsBatch := cloneObjectStatsBatchForTest(t, txn.proc.Mp(), stats)
				txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, databaseId: 7, tableId: 42, fileName: loadName, bat: statsBatch})
				if !gcCase {
					txn.TrackLoadFiles(loadName)
				}
				if terminal == "GC delete retry" {
					gcFS = &loadCleanupFileService{FileService: fs, err: loadErr}
					txn.engine.fs = gcFS
				}
				if terminal == "GC quota" {
					txn.engine.config.memThrottler = rscthrottler.NewMemThrottler("statement-pins-denied", 1, rscthrottler.WithConstLimit(1))
				}
				if terminal == "GC pool closed" || terminal == "GC pool saturated" {
					pool, e := ants.NewPool(1)
					require.NoError(t, e)
					txn.engine.gcPool = pool
					t.Cleanup(pool.Release)
					if terminal == "GC pool closed" {
						pool.Release()
					} else {
						entered, release := make(chan struct{}), make(chan struct{})
						t.Cleanup(func() { close(release) })
						require.NoError(t, pool.Submit(func() { close(entered); <-release }))
						<-entered
					}
				}
			}
			originalID := vector.GetFixedAtNoTypeCheck[types.Rowid](raw.Vecs[0], 0)
			delegate := &txnTableDelegate{origin: tbl, isLocal: func() (bool, error) { return true, nil }}
			remote := strings.HasPrefix(terminal, "remote ")
			readEntered := make(chan struct{})
			markReadEntered := sync.OnceFunc(func() { close(readEntered) })
			readErr := moerr.NewInternalErrorNoCtx("remote statement read failed")
			if remote {
				// A captured, uncommitted tombstone must never become a remote
				// file dependency, including when ReadClose is not acknowledged.
				stats := mockStatsList(t, 1)[0]
				tombstone := batch.NewWithSize(1)
				tombstone.Attrs = []string{catalog.ObjectMeta_ObjectStats}
				tombstone.Vecs[0] = vector.NewVec(types.T_varbinary.ToType())
				require.NoError(t, vector.AppendBytes(tombstone.Vecs[0], stats.Marshal(), false, txn.proc.Mp()))
				tombstone.SetRowCount(1)
				if terminal == "remote data owned" {
					// The raw selection above already supplies a real captured
					// tombstone. Do not install a synthetic persisted object.
					tombstone.Clean(txn.proc.Mp())
				} else {
					txn.appendWorkspaceEntryLocked(Entry{typ: DELETE, databaseId: 7, tableId: 42, fileName: stats.ObjectName().String(), bat: tombstone})
				}
				txn.proc.Base.TxnOperator = op
				op.EXPECT().Snapshot().Return(pbtxn.CNTxnSnapshot{}, nil).AnyTimes()
				delegate.isLocal = func() (bool, error) { return false, nil }
				service := mock_shard.NewMockShardService(gomock.NewController(t))
				delegate.shard.service = service
				expectedCalls, nextRead := 5, 0
				if terminal == "remote data owned" {
					expectedCalls = 9
				}
				service.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request shardservice.ReadRequest, _ shardservice.ReadOptions) error {
					switch request.Method {
					case shardservice.ReadRanges:
						require.Equal(t, uint32(engine.Policy_CollectCommittedData), request.Param.RangesParam.DataCollectPolicy)
						data := readutil.NewBlockListRelationData(1)
						encoded, e := data.MarshalBinary()
						require.NoError(t, e)
						return request.Apply(encoded)
					case shardservice.ReadBuildReader:
						nextRead = 0
						data, e := readutil.UnmarshalRelationData(request.Param.ReaderBuildParam.RelData)
						require.NoError(t, e)
						require.Nil(t, data.GetTombstones(), "remote reader has no uncommitted file references")
						id := types.Uuid{1}
						return request.Apply(types.EncodeUuid(&id))
					case shardservice.ReadNext:
						markReadEntered()
						if terminal == "remote data owned" {
							nextRead++
							if nextRead > 1 {
								end := true
								return request.Apply(types.EncodeBool(&end))
							}
							data := batch.NewWithSize(2)
							defer data.Clean(txn.proc.Mp())
							data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
							data.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
							for i, pk := range []int64{2, 4} {
								id := types.Rowid{0: 42}
								if i == 0 {
									id = vector.GetFixedAtNoTypeCheck[types.Rowid](raw.Vecs[0], 1)
								}
								require.NoError(t, vector.AppendFixed(data.Vecs[0], pk, false, txn.proc.Mp()))
								require.NoError(t, vector.AppendFixed(data.Vecs[1], id, false, txn.proc.Mp()))
							}
							data.SetRowCount(2)
							encoded, e := data.MarshalBinary()
							require.NoError(t, e)
							end, size := false, uint32(len(encoded))
							frame := append(types.EncodeBool(&end), types.EncodeUint32(&size)...)
							frame = append(frame, encoded...)
							e = request.Apply(frame)
							clear(frame) // The transport may immediately recycle it.
							return e
						}
						if terminal == "remote close error" {
							end := true
							return request.Apply(types.EncodeBool(&end))
						}
						if terminal == "remote read cancelled" || terminal == "remote view close" {
							<-ctx.Done()
							return errors.Join(context.Cause(ctx))
						}
						return errors.Join(readErr)
					case shardservice.ReadClose:
						if terminal == "remote data owned" {
							return nil
						}
						return context.DeadlineExceeded
					default:
						t.Errorf("unexpected remote method %d", request.Method)
						return readErr
					}
				}).Times(expectedCalls) // Error drains both closes; success also checks a deleted row then a live row.
			}
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.Open(16 << 20)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.Zero(t, account.Seal().Used)
				_, e := registry.Finalize(account)
				require.NoError(t, e)
				require.Zero(t, registry.LiveAllocationMetadata())
			})
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			cause := moerr.NewInternalErrorNoCtx("cancel statement view capture")
			if terminal == "capture cancelled" {
				cancel(cause)
			}
			reserve := func(n uint64) (engine.StatementViewReservation, error) {
				if terminal == "capture budget" {
					return nil, mpool.ErrAllocationAccountCapacity
				}
				g, e := txn.proc.GetHashBuildBudget()
				if e != nil {
					return nil, e
				}
				return g.ReserveMemory(ctx, n)
			}
			view, err := delegate.NewStatementKeyView(ctx, []engine.Relation{delegate}, txn.proc, account, reserve)
			if terminal == "capture cancelled" || terminal == "capture budget" || terminal == "GC quota" {
				if terminal == "capture cancelled" {
					require.ErrorIs(t, err, cause)
				} else {
					require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				}
				require.Nil(t, view)
				require.Nil(t, txn.statementKeyPins.Load())
				require.Equal(t, 3, raw.RowCount())
				require.Equal(t, originalID, vector.GetFixedAtNoTypeCheck[types.Rowid](raw.Vecs[0], 0))
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, view.Close()) })

			key := vector.NewVec(types.T_int64.ToType())
			defer key.Free(txn.proc.Mp())
			require.NoError(t, vector.AppendFixedList(key, []int64{1, 2}, nil, txn.proc.Mp()))
			owned, err := view.Lookup(t.Context(), 42, key, 0)
			require.NoError(t, err)
			require.NotNil(t, owned)
			defer owned.Clean(txn.proc.Mp())
			require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](owned.Vecs[0], 0))
			require.Equal(t, originalID, vector.GetFixedAtNoTypeCheck[types.Rowid](owned.Vecs[1], 0))
			if gcCase {
				other, e := delegate.NewStatementKeyView(ctx, []engine.Relation{delegate}, txn.proc, account, reserve)
				require.NoError(t, e)
				t.Cleanup(func() { require.NoError(t, other.Close()) })
				require.Equal(t, quota-statementObjectPinQuota, txn.engine.config.memThrottler.Available(), "two readers share one engine-owned record even at the exact quota")
				require.NoError(t, txn.Rollback(t.Context()))
				require.True(t, moerr.IsMoErrCode(view.Validate(t.Context()), moerr.ErrTxnNeedRetry))
				require.NoError(t, view.Close())
				require.True(t, objectExistsInFS(t.Context(), fs, loadName), "another reader still owns the object")
				require.NoError(t, other.Close(), "cancelled statement close hands cleanup to the engine")
				require.True(t, objectExistsInFS(t.Context(), fs, loadName))
				require.NotNil(t, txn.engine.statementGC.head)
				require.Equal(t, quota-statementObjectPinQuota, txn.engine.config.memThrottler.Available())
				if gcFS != nil {
					require.False(t, gcFS.deleteCalled, "a cancelled statement performs no opportunistic engine GC IO")
					require.Same(t, loadErr, txn.engine.drainStatementObjects(false))
					require.True(t, objectExistsInFS(t.Context(), fs, loadName))
					require.NotNil(t, txn.engine.statementGC.head, "failed deletion keeps its retry owner and quota")
					require.Equal(t, quota-statementObjectPinQuota, txn.engine.config.memThrottler.Available())
					// A later live statement can attempt engine cleanup, but a
					// failed old obligation cannot turn that close into an error.
					nextTxn, nextTable := newDumpInsertTxnForTest(t)
					t.Cleanup(func() { cleanTxnWritesForTest(nextTxn) })
					nextTxn.engine = txn.engine
					nextOp := nextTxn.op.(*mock_frontend.MockTxnOperator)
					nextOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
					nextOp.EXPECT().IsSnapOp().Return(false).AnyTimes()
					nextTable.eng = txn.engine
					nextTable.remoteWorkspace = true
					nextTable.createdInTxn = true
					nextTable.primaryIdx = 0
					nextTable.proc.Store(nextTxn.proc)
					nextTable.tableDef = tbl.tableDef
					nextDelegate := &txnTableDelegate{origin: nextTable, isLocal: func() (bool, error) { return true, nil }}
					nextView, e := nextDelegate.NewStatementKeyView(t.Context(), []engine.Relation{nextDelegate}, nextTxn.proc, account, reserve)
					require.NoError(t, e)
					require.NoError(t, nextView.Close(), "engine cleanup is diagnostic after ownership transfer")
					require.NotNil(t, txn.engine.statementGC.head)
					require.True(t, objectExistsInFS(t.Context(), fs, loadName))
					require.Equal(t, quota-statementObjectPinQuota, txn.engine.config.memThrottler.Available())
					gcFS.err = nil
				}
				require.NoError(t, txn.engine.drainStatementObjects(false), "the surviving engine owner retries outside the cancelled statement")
				require.Nil(t, txn.statementKeyPins.Load())
				require.Nil(t, txn.engine.statementGC.head)
				require.Equal(t, quota, txn.engine.config.memThrottler.Available())
				require.False(t, objectExistsInFS(t.Context(), fs, loadName))
				return
			}
			if loadFS == nil && !remote {
				absent, e := view.Lookup(t.Context(), 42, key, 1)
				require.NoError(t, e)
				require.Nil(t, absent, "a prior workspace deletion remains absent")
			}
			func() {
				txn.Lock()
				defer txn.Unlock()
				require.NoError(t, txn.mergeTxnWorkspaceLocked(t.Context()))
				var count int
				require.NoError(t, txn.dumpInsertBatchLocked(t.Context(), fs, 0, &count))
			}()
			require.Equal(t, 3, raw.RowCount(), "merge/dump cannot rewrite captured RowIDs")
			require.NoError(t, view.Validate(t.Context()))
			if remote {
				type lookupResult struct {
					row *batch.Batch
					err error
				}
				done := make(chan lookupResult, 1)
				go func() {
					row, e := view.Lookup(t.Context(), 42, key, 1)
					done <- lookupResult{row, e}
				}()
				<-readEntered
				if terminal == "remote read cancelled" {
					txn.statementID = 1
					// This failed statement owns the read, while both captured
					// entries belong to the preceding successful transaction work.
					txn.offsets = []int{len(txn.writes)}
					txn.transfer.statements = []statementTransferState{{}}
					require.NoError(t, txn.RollbackLastStatement(t.Context()))
				}
				var closeDone chan error
				if terminal == "remote view close" {
					closeDone = make(chan error, 1)
					go func() { closeDone <- view.Close() }()
				}
				result := <-done
				if closeDone != nil {
					require.NoError(t, <-closeDone)
				}
				require.Nil(t, result.row)
				if terminal == "remote data owned" {
					require.NoError(t, result.err, "captured deletion removes the remote pk=2")
					vector.SetFixedAtNoTypeCheck(key, 1, int64(4))
					remoteRow, e := view.Lookup(t.Context(), 42, key, 1)
					require.NoError(t, e)
					require.NotNil(t, remoteRow)
					defer remoteRow.Clean(txn.proc.Mp())
					require.Same(t, view.(*statementKeyView).vectorAllocation, remoteRow.AllocationAccountSelection())
					require.NoError(t, view.Close())
					require.NoError(t, txn.Rollback(t.Context()))
					require.Nil(t, txn.statementKeyPins.Load())
					require.Equal(t, int64(4), vector.GetFixedAtNoTypeCheck[int64](remoteRow.Vecs[0], 0))
					require.Equal(t, types.Rowid{0: 42}, vector.GetFixedAtNoTypeCheck[types.Rowid](remoteRow.Vecs[1], 0))
					require.Equal(t, originalID, vector.GetFixedAtNoTypeCheck[types.Rowid](owned.Vecs[1], 0))
					return
				}
				if terminal == "remote read cancelled" {
					require.True(t, moerr.IsMoErrCode(result.err, moerr.ErrTxnNeedRetry), "%v", result.err)
				} else if terminal == "remote view close" {
					require.ErrorIs(t, result.err, context.Canceled, "Close cancels a blocked lookup before waiting for its mutex")
				} else if terminal == "remote close error" {
					require.ErrorIs(t, result.err, context.DeadlineExceeded)
				} else {
					require.Same(t, readErr, result.err)
				}
				require.NoError(t, view.Close())
				require.Nil(t, txn.statementKeyPins.Load())
				require.Equal(t, 3, raw.RowCount(), "read cancellation preserves prior transaction writes")
				require.Equal(t, originalID, vector.GetFixedAtNoTypeCheck[types.Rowid](owned.Vecs[1], 0))
				return
			}
			if loadFS != nil {
				// Hold the lookup owner across cancellation. Physical cleanup
				// must wait, with txn.Lock available and protection installed.
				v := view.(*statementKeyView)
				v.Lock()
				unlock := sync.OnceFunc(v.Unlock)
				defer unlock()
				releaseDelete := sync.OnceFunc(func() { close(loadFS.release) })
				defer releaseDelete()
				type cleanupResult struct {
					names []string
					err   error
				}
				done := make(chan cleanupResult, 1)
				go func() {
					names, e := txn.deleteLoadFiles(t.Context(), nil)
					done <- cleanupResult{names, e}
				}()
				<-v.ctx.Done()
				// Cancellation is issued under txn.Lock before the closer drops
				// that short critical section. Prove lock availability while the
				// lookup is still held, rather than racing a single TryLock.
				lockAvailable := make(chan struct{})
				go func() { txn.Lock(); txn.Unlock(); close(lockAvailable) }()
				select {
				case <-lockAvailable:
				case <-time.After(5 * time.Second):
					t.Fatal("quiescence waited under the transaction mutex")
				}
				select {
				case <-loadFS.entered:
					t.Fatal("LOAD deleted while a lookup still owned the object")
				default:
				}
				require.True(t, objectExistsInFS(t.Context(), fs, loadName))
				unlock()
				<-loadFS.entered
				require.True(t, txn.engine.cloneTxnCache.IsSharedFile(txn.op.Txn().ID, loadName))
				releaseDelete()
				result := <-done
				if loadFS.err != nil {
					require.ErrorIs(t, result.err, loadErr)
					require.Empty(t, result.names)
					require.Contains(t, txn.loadFiles[txn.statementID], loadName)
				} else {
					require.NoError(t, result.err)
					require.Equal(t, []string{loadName}, result.names)
					require.False(t, objectExistsInFS(t.Context(), fs, loadName))
					txn.Lock()
					txn.removeLoadFileProtectionsLocked(result.names)
					txn.Unlock()
					// Reuse after synchronous cleanup is safe: no deferred GC
					// owns this name after the view's last Close.
					require.NoError(t, writeObjectToFS(t.Context(), fs, loadName))
				}
				require.NoError(t, view.Close())
				require.Nil(t, txn.statementKeyPins.Load())
				require.True(t, objectExistsInFS(t.Context(), fs, loadName))
				return
			}
			switch terminal {
			case "new selection":
				txn.Lock()
				txn.addBatchSelectionsLocked(raw, []int64{0})
				txn.Unlock()
				again, e := view.Lookup(t.Context(), 42, key, 0)
				require.NoError(t, e)
				require.NotNil(t, again)
				require.Equal(t, originalID, vector.GetFixedAtNoTypeCheck[types.Rowid](again.Vecs[1], 0))
				again.Clean(txn.proc.Mp())
			case "statement rollback":
				txn.statementID = 1
				txn.offsets = []int{0}
				txn.transfer.statements = []statementTransferState{{}}
				require.NoError(t, txn.RollbackLastStatement(t.Context()))
			case "transaction rollback":
				require.NoError(t, txn.Rollback(t.Context()))
			}
			require.True(t, moerr.IsMoErrCode(view.Validate(t.Context()), moerr.ErrTxnNeedRetry))
			if terminal != "new selection" {
				failed, e := view.Lookup(t.Context(), 42, key, 0)
				require.True(t, moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry), "%v", e)
				require.Nil(t, failed)
				require.Equal(t, 3, raw.RowCount(), "rollback detaches rather than frees a pinned batch")
				require.Zero(t, txn.workspaceSize)
			}
			require.NoError(t, view.Close())
			require.Nil(t, txn.statementKeyPins.Load(), "the last close releases pin-map capacity")
			require.Nil(t, view.(*statementKeyView).tables, "closed prepared handles retain no capture metadata")
			if terminal != "new selection" {
				require.Zero(t, raw.RowCount())
			}
			// A returned image has independent vector ownership, even after rollback
			// and the captured resources' last Close.
			require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](owned.Vecs[0], 0))
			require.Equal(t, originalID, vector.GetFixedAtNoTypeCheck[types.Rowid](owned.Vecs[1], 0))
			failed, e := view.Lookup(t.Context(), 42, key, 0)
			require.Error(t, e)
			require.Nil(t, failed)
		})
	}
}

type statementGCFileService struct {
	fileservice.FileService
	delete func(context.Context, ...string) error
}

func (f *statementGCFileService) Delete(ctx context.Context, names ...string) error {
	return f.delete(ctx, names...)
}

func TestStatementObjectGCPartiallyDeletedChunkRemainsOwned(t *testing.T) {
	// One more than the production chunk limit is the minimum that proves a
	// successful chunk is released while a later partially deleted chunk retries.
	const count = GCBatchOfFileCount + 1
	quota := int64(count) * statementObjectPinQuota
	e := &Engine{}
	e.config.memThrottler = rscthrottler.NewMemThrottler("statement-gc-chunks", 1, rscthrottler.WithConstLimit(quota))
	_, ok := e.AcquireQuota(quota)
	require.True(t, ok)
	files := make(map[string]bool, count)
	for i := range count {
		name := fmt.Sprint(i)
		files[name] = true
		e.deferStatementObject(&statementObjectPin{name: name})
	}
	deleteErr := moerr.NewInternalErrorNoCtx("partial object delete")
	calls := 0
	e.fs = &statementGCFileService{delete: func(ctx context.Context, names ...string) error {
		require.NoError(t, context.Cause(ctx))
		_, bounded := ctx.Deadline()
		require.True(t, bounded)
		calls++
		for _, name := range names {
			delete(files, name)
		}
		if calls == 2 {
			return deleteErr // The file is gone; ownership still awaits success.
		}
		return nil
	}}
	require.Same(t, deleteErr, e.drainStatementObjects(false))
	require.Empty(t, files)
	require.NotNil(t, e.statementGC.head)
	require.Equal(t, quota-statementObjectPinQuota, e.config.memThrottler.Available())
	require.NoError(t, e.drainStatementObjects(true))
	require.Equal(t, 3, calls)
	require.Nil(t, e.statementGC.head)
	require.Nil(t, e.statementGC.active)
	require.Equal(t, quota, e.config.memThrottler.Available())
}

func TestStatementKeyProbeZeroRangeReachesBothPackedRows(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	for _, oid := range []types.T{types.T_float32, types.T_float64} {
		t.Run(oid.String(), func(t *testing.T) {
			pk := vector.NewVec(types.New(oid, 0, 0))
			ids, times := vector.NewVec(types.T_Rowid.ToType()), vector.NewVec(types.T_TS.ToType())
			defer pk.Free(proc.Mp())
			defer ids.Free(proc.Mp())
			defer times.Free(proc.Mp())
			for row, v := range []float64{-1, math.Copysign(0, -1), 0, 1} {
				if oid == types.T_float32 {
					require.NoError(t, vector.AppendFixed(pk, float32(v), false, proc.Mp()))
				} else {
					require.NoError(t, vector.AppendFixed(pk, v, false, proc.Mp()))
				}
				require.NoError(t, vector.AppendFixed(ids, types.Rowid{0: 17, 23: byte(row)}, false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(times, types.BuildTS(1, 0), false, proc.Mp()))
			}
			def := &plan.TableDef{Name: "zeros", Name2ColIndex: map[string]int32{"k": 0}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "k"}, Cols: []*plan.ColDef{{Name: "k", Typ: plan.Type{Id: int32(oid)}, Primary: true}}}
			var input api.Batch
			input.Attrs = []string{"rowid", "time", "k"}
			for _, v := range []*vector.Vector{ids, times, pk} {
				encoded, err := vector.VectorToProtoVector(v)
				require.NoError(t, err)
				input.Vecs = append(input.Vecs, encoded)
			}
			packer := types.NewPacker()
			defer packer.Close()
			pool := fileservice.NewPool(1, func() *types.Packer { return packer }, func(p *types.Packer) { p.Reset() }, func(*types.Packer) {})
			state := logtailreplay.NewPartitionState("", false, 42, false)
			state.HandleRowsInsert(t.Context(), &input, 0, packer, proc.Mp())
			for _, row := range []int{1, 2} {
				key, err := pk.CloneWindow(row, row+1, proc.Mp())
				require.NoError(t, err)
				expr, err := statementKeyProbe(t.Context(), &statementKeyTable{pk: 0, physical: true, def: def}, key)
				require.NoError(t, err)
				base, err := readutil.ConstructBasePKFilter(expr, def, proc.Mp())
				require.NoError(t, err)
				mem, err := readutil.NewMemPKFilter(def, timestamp.Timestamp{}, pool, base, engine.FilterHint{})
				require.NoError(t, err)
				require.True(t, mem.Valid())
				require.Equal(t, function.BETWEEN, mem.Op())
				require.Len(t, mem.Keys(), 2)
				iter := state.NewPrimaryKeyIterWithFilters(types.BuildTS(2, 0), mem.Specs())
				var got []types.Rowid
				for iter.Next() {
					got = append(got, iter.Entry().RowID)
				}
				require.NoError(t, iter.Close())
				require.Equal(t, []types.Rowid{{0: 17, 23: 1}, {0: 17, 23: 2}}, got, "the actual packed iterator keeps both zero encodings")
				require.True(t, sameStatementKey(true, key, 0, pk, row))
				require.False(t, sameStatementKey(true, key, 0, pk, 3-row), "physical residual selects its original zero")
				require.True(t, sameStatementKey(false, key, 0, pk, 3-row), "logical equality merges signed zero")
				base.Cleanup()
				key.Free(proc.Mp())
			}
		})
	}
}

func TestStatementKeyViewFiniteFloatProbeDoesNotRetainCommittedImages(t *testing.T) {
	colexec.NewServer("")
	var peaks []uint64
	for _, count := range []int{1000, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			txn, tbl := newDumpInsertTxnForTest(t)
			t.Cleanup(func() { cleanTxnWritesForTest(txn) })
			snapshot := timestamp.Timestamp{PhysicalTime: 2}
			op := txn.op.(*mock_frontend.MockTxnOperator)
			op.EXPECT().SnapshotTS().Return(snapshot).AnyTimes()
			op.EXPECT().IsSnapOp().Return(false).AnyTimes()
			txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)
			packer := types.NewPacker()
			defer packer.Close()
			txn.engine.packerPool = fileservice.NewPool(1, func() *types.Packer { return packer }, func(p *types.Packer) { p.Reset() }, func(*types.Packer) {})
			fs, err := colexec.GetSharedFSFromProc(txn.proc)
			require.NoError(t, err)
			txn.engine.fs = fs
			tbl.eng = txn.engine
			tbl.createdInTxn = true
			tbl.remoteWorkspace = true
			tbl.primaryIdx = 0
			tbl.primarySeqnum = 0
			tbl.proc.Store(txn.proc)
			typ := types.New(types.T_float32, 0, 2)
			tbl.tableDef = &plan.TableDef{TblId: 42, Name: "tbl", DbName: "db", Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk", Names: []string{"pk"}}, Name2ColIndex: map[string]int32{"pk": 0, "payload": 1, catalog.Row_ID: 2}, Cols: []*plan.ColDef{
				{Name: "pk", Typ: plan.Type{Id: int32(types.T_float32), Scale: 2}, Seqnum: 0},
				{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 1},
				{Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}, Seqnum: objectio.SEQNUM_ROWID},
			}}
			stored := batch.NewWithSize(4)
			stored.Attrs = []string{"rowid", "time", "pk", "payload"}
			defer stored.Clean(txn.proc.Mp())
			for col, typ := range []types.Type{types.T_Rowid.ToType(), types.T_TS.ToType(), typ, types.New(types.T_varchar, 0, 0)} {
				stored.Vecs[col] = vector.NewVec(typ)
			}
			payload := []byte(strings.Repeat("p", 1200))
			block := types.Blockid{0: 17}
			for row := range count {
				require.NoError(t, vector.AppendFixed(stored.Vecs[0], types.NewRowid(&block, uint32(row)), false, txn.proc.Mp()))
				require.NoError(t, vector.AppendFixed(stored.Vecs[1], types.BuildTS(1, 0), false, txn.proc.Mp()))
				require.NoError(t, vector.AppendFixed(stored.Vecs[2], float32(row+1)+0.001, false, txn.proc.Mp()))
				require.NoError(t, vector.AppendBytes(stored.Vecs[3], payload, false, txn.proc.Mp()))
			}
			stored.SetRowCount(count)
			var input api.Batch
			input.Attrs = stored.Attrs
			for _, v := range stored.Vecs {
				encoded, e := vector.VectorToProtoVector(v)
				require.NoError(t, e)
				input.Vecs = append(input.Vecs, encoded)
			}
			partition := txn.engine.GetOrCreateLatestPart(t.Context(), 1, tbl.db.databaseId, 42)
			state, publish := partition.MutateState()
			state.HandleRowsInsert(t.Context(), &input, 0, packer, txn.proc.Mp())
			publish()
			delegate := &txnTableDelegate{origin: tbl, isLocal: func() (bool, error) { return true, nil }}
			base := &txnTable{accountId: 1, tableId: 43, tableName: "base", db: tbl.db, eng: txn.engine, createdInTxn: true, remoteWorkspace: true}
			base.proc.Store(txn.proc)
			base.tableDef = &plan.TableDef{TblId: 43, Name: "base", DbName: "db", Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk", Names: []string{"pk"}}, Name2ColIndex: map[string]int32{"pk": 0, catalog.Row_ID: 1}, Cols: []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0}, {Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}, Seqnum: objectio.SEQNUM_ROWID}}}
			txn.tableOps.addCreateTable(genTableKey(1, "base", 7, "db"), 0, base)
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.Open(256 << 10)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.Zero(t, account.Seal().Used)
				_, err := registry.Finalize(account)
				require.NoError(t, err)
				require.Zero(t, registry.LiveAllocationMetadata())
			})
			generation, err := txn.proc.GetHashBuildBudget()
			require.NoError(t, err)
			view, err := delegate.NewStatementKeyView(t.Context(), []engine.Relation{&txnTableDelegate{origin: base, isLocal: func() (bool, error) { return true, nil }}, delegate}, txn.proc, account, func(n uint64) (engine.StatementViewReservation, error) {
				return generation.ReserveMemory(t.Context(), n)
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, view.Close()) }()
			var usage uint64
			for i, wanted := range []int{count / 2, count, count + 1} {
				key := vector.NewVec(typ)
				require.NoError(t, vector.AppendFixed(key, float32(wanted)+0.003, false, txn.proc.Mp()))
				row, err := view.Lookup(t.Context(), 42, key, 0)
				key.Free(txn.proc.Mp())
				require.NoError(t, err)
				if wanted > count {
					require.Nil(t, row)
				} else {
					require.NotNil(t, row)
					require.Equal(t, types.NewRowid(&block, uint32(wanted-1)), vector.GetFixedAtNoTypeCheck[types.Rowid](row.Vecs[2], 0))
					require.Equal(t, payload, row.Vecs[1].GetBytesAt(0))
					row.Clean(txn.proc.Mp())
				}
				if i == 0 {
					usage = generation.Snapshot().Used
				}
				require.Equal(t, usage, generation.Snapshot().Used, "completed probes retain no committed row images")
			}
			peaks = append(peaks, account.Snapshot().Peak)
			t.Logf("committed_rows=%d committed_payload_bytes=%d probe_native_peak=%d retained_query_metadata=%d", count, count*len(payload), account.Snapshot().Peak, usage)
			require.NoError(t, view.Close())
			require.Zero(t, generation.Snapshot().Used)
		})
	}
	require.Len(t, peaks, 2)
	require.Equal(t, peaks[0], peaks[1], "ten times more committed data does not grow retained probe images")
}
