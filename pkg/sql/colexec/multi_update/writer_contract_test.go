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

package multi_update

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func committedDeleteInput(t *testing.T, proc *process.Process, first, rows int) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(2)
	b.Attrs = DeleteBatchAttrs
	b.Vecs[0] = vector.NewOffHeapVecWithType(types.T_Rowid.ToType())
	b.Vecs[1] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	t.Cleanup(func() { b.Clean(proc.Mp()) })
	for id := first; id < first+rows; id++ {
		block := types.BuildTestBlockid(1, int64(id))
		require.NoError(t, vector.AppendFixed(b.Vecs[0], types.NewRowid(&block, 0), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], int64(id), false, proc.Mp()))
	}
	b.SetRowCount(rows)
	return b
}

func deleteContractWriter(credits *admissionThrottler) *s3WriterDelegate {
	w := &s3WriterDelegate{
		updateCtxs:    []*MultiUpdateCtx{{TableDef: &plan.TableDef{Name: "delete_contract", TblId: 99}, DeleteCols: []int{0, 1}}},
		insertSinkers: make([]*colexec.CNS3Writer, 1), deleteBatches: make([]*batch.BatchSet, 1),
		deleteBlockMap: make([]map[types.Blockid]*deleteBlockData, 1), segmentMap: make(map[string]int32),
		flushThreshold: 64 << 20,
	}
	w.memController.throttler = credits
	return w
}

func TestCommittedDeleteFutureGrowthRemainsAdmitted(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
	credits := &admissionThrottler{capacity: 1 << 30}
	w := deleteContractWriter(credits)
	t.Cleanup(func() { require.NoError(t, w.free(proc)) })
	analyzer := process.NewAnalyzer(0, false, false, "delete-contract")
	for first := 1; first <= 5; first += 2 {
		require.NoError(t, w.append(proc, analyzer, committedDeleteInput(t, proc, first, 2)))
		require.Greater(t, w.chargedBytes(), w.retainedBytes(), "deferred materialization is an owner, not unused credit")
		require.Equal(t, w.chargedBytes(), credits.reserved)
	}
	var observed int
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().DoAndReturn(func() txn.TxnMeta {
		observed++
		require.LessOrEqual(t, w.retainedBytes(), credits.reserved, "observe real buffers and per-block bitmap growth before each next mutation")
		return txn.TxnMeta{ID: []byte{1}}
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp
	acquires := credits.acquires
	result, err := w.prepareDeleteBatches(proc, 0, w.deleteBatches[0].TakeBatches(), true)
	defer func() {
		for _, b := range result {
			b.Clean(w.memoryPool(proc))
		}
	}()
	require.NoError(t, err)
	require.Equal(t, 6, observed)
	require.Equal(t, acquires, credits.acquires, "materialization cannot reacquire D while holding its own source")
	var rows int
	for _, b := range result {
		rows += b.RowCount()
	}
	require.Equal(t, 6, rows)
	require.LessOrEqual(t, w.retainedBytes(), credits.reserved)
	for _, b := range result {
		b.Clean(w.memoryPool(proc))
	}
	result = nil
	w.cleanDeleteBatches(w.memoryPool(proc))
	w.reconcileMemory()
	require.Zero(t, credits.reserved)
	require.NoError(t, w.reset(proc))
	require.NoError(t, w.append(proc, analyzer, committedDeleteInput(t, proc, 10, 2)))
	require.Positive(t, credits.reserved)
}

func TestCommittedDeletePreparationFailureReleasesOwners(t *testing.T) {
	for _, failure := range []string{"cancel", "allocation"} {
		t.Run(failure, func(t *testing.T) {
			_, _, proc := prepareTestCtx(t, true)
			t.Cleanup(proc.Free)
			t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
			credits := &admissionThrottler{capacity: 1 << 30}
			w := deleteContractWriter(credits)
			t.Cleanup(func() { require.NoError(t, w.free(proc)) })
			if failure == "allocation" {
				var err error
				w.ownedMP, err = mpool.NewMPool("delete-partial-failure", 1<<20, mpool.NoFixed)
				require.NoError(t, err)
			}
			input := committedDeleteInput(t, proc, 1, 2)
			if failure == "allocation" {
				input.Vecs[1].Free(proc.Mp())
				input.Vecs[1] = testutil.MakeVarcharVector([]string{strings.Repeat("x", 320<<10), strings.Repeat("y", 320<<10)}, nil, proc.Mp())
			}
			require.NoError(t, w.append(proc, process.NewAnalyzer(0, false, false, "delete-failure"), input))
			if failure == "cancel" {
				ctx, cancel := context.WithCancel(proc.Ctx)
				proc.Ctx = ctx
				cancel()
			}
			result, err := w.prepareDeleteBatches(proc, 0, w.deleteBatches[0].TakeBatches(), true)
			for _, b := range result {
				b.Clean(w.memoryPool(proc))
			}
			require.Error(t, err)
			if failure == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.NoError(t, w.reset(proc))
			require.Zero(t, credits.reserved)
			require.Zero(t, w.retainedBytes())
			require.Empty(t, w.deleteBlockMap[0])
			require.Zero(t, w.chargedBytes())
			require.False(t, credits.invalidRelease)
		})
	}
}

func TestPartitionWriterDrainsAllMetadataFrames(t *testing.T) {
	for _, count := range []int{0, 1, 3, 5} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			proc, w := metadataProtocolWriter(t, []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}}, {TableDef: &plan.TableDef{TblId: 100}}}, false)
			src := batch.NewWithSize(1)
			t.Cleanup(func() { src.Clean(proc.Mp()) })
			src.Vecs[0] = testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
			src.SetRowCount(1)
			var want []uint64
			for i := 0; i < count; i++ {
				// Alternate table and action; metadata must still emit deletes
				// before inserts, with each table's frame preserved exactly once.
				action := actionInsert
				if i%2 == 0 {
					action = actionDelete
				}
				require.NoError(t, w.appendMetadata(proc, action, i%2, uint64(i+1), "", src))
			}
			for _, parity := range []int{0, 1} {
				for i := parity; i < count; i += 2 {
					want = append(want, uint64(i+1))
				}
			}
			w.tailFlushed = true
			raw := NewArgument()
			t.Cleanup(raw.Release)
			raw.Action = UpdateWriteS3
			raw.OpAnalyzer = process.NewAnalyzer(0, false, false, "partition-contract")
			raw.ctr.state = vm.Build
			once := false
			raw.getFlushableS3WriterFunc = func() *s3WriterDelegate {
				if once {
					return nil
				}
				once = true
				return w
			}
			op := &PartitionMultiUpdate{raw: raw}
			child := colexec.NewMockOperator().WithBatchs(nil)
			t.Cleanup(func() { child.Free(proc, false, nil) })
			op.AppendChild(child)
			var got []uint64
			for calls := 0; ; calls++ {
				require.Less(t, calls, count+2)
				result, err := op.Call(proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				got = append(got, vector.MustFixedColNoTypeCheck[uint64](result.Batch.Vecs[2])...)
			}
			require.Equal(t, want, got)
		})
	}
}

func TestPartitionWriterCleanupCoversBuildAndPartialEval(t *testing.T) {
	for _, reset := range []bool{false, true} {
		_, _, proc := prepareTestCtx(t, true)
		t.Cleanup(proc.Free)
		t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
		credits := &admissionThrottler{capacity: 1 << 30}
		build, eval := deleteContractWriter(credits), deleteContractWriter(credits)
		for _, w := range []*s3WriterDelegate{build, eval} {
			require.NoError(t, w.append(proc, process.NewAnalyzer(0, false, false, "partition-cleanup"), committedDeleteInput(t, proc, 1, 2)))
		}
		raw := NewArgument()
		t.Cleanup(raw.Release)
		op := &PartitionMultiUpdate{raw: raw, writers: map[uint64]*s3WriterDelegate{99: build}, freeWriters: []*s3WriterDelegate{eval}, affectedRows: 3}
		if reset {
			op.Reset(proc, true, context.Canceled)
			require.Equal(t, uint64(3), op.affectedRows, "Compile reads the result after Scope.Run resets writers")
			require.Equal(t, vm.Build, raw.ctr.state)
		} else {
			op.Free(proc, true, context.Canceled)
		}
		require.Empty(t, op.writers)
		require.Empty(t, op.freeWriters)
		require.Zero(t, credits.reserved)
		require.Nil(t, build.ownedMP)
		require.Nil(t, eval.ownedMP)
		op.Free(proc, false, nil)
		require.False(t, credits.invalidRelease)
	}
}

func TestPartitionReplayRetiresAllSourceCredits(t *testing.T) {
	for _, mode := range []string{"eof", "cancel", "write_error", "late_write_error", "late_cancel", "late_error_reset", "partial_eval", "decode_error"} {
		t.Run(mode, func(t *testing.T) {
			_, ctrl, proc := prepareTestCtx(t, true)
			t.Cleanup(proc.Free)
			t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
			ctx, cancel := context.WithTimeout(proc.Ctx, 5*time.Second)
			t.Cleanup(cancel)
			proc.Ctx = ctx
			opTxn := mock_frontend.NewMockTxnOperator(ctrl)
			opTxn.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte{1}}).AnyTimes()
			opTxn.EXPECT().TxnOptions().Return(txn.TxnOptions{Autocommit: true}).AnyTimes()
			proc.Base.TxnOperator = opTxn
			const total = int64(1 << 20)
			owner := rscthrottler.NewMemThrottler(t.Name(), 1, rscthrottler.WithConstLimit(total), rscthrottler.WithMemorySampleForTesting(16<<20, 0))
			raw := NewArgument()
			t.Cleanup(raw.Release)
			raw.Action = UpdateWriteS3
			raw.OpAnalyzer = process.NewTempAnalyzer()
			raw.ctr.state = vm.Build
			op := &PartitionMultiUpdate{raw: raw, writers: make(map[uint64]*s3WriterDelegate)}
			t.Cleanup(func() { op.Free(proc, false, nil) })
			raw.getFlushableS3WriterFunc = op.getFlushableS3Writer
			child := colexec.NewMockOperator().WithBatchs(nil)
			op.AppendChild(child)
			t.Cleanup(func() { child.Free(proc, false, nil) })
			var writers []*s3WriterDelegate
			keys := make(map[uint64]string)
			for p := range 2 {
				w := deleteContractWriter(nil)
				w.memController.throttler = owner
				w.updateCtxs = prepareTestDeleteMultiUpdateCtx(false, false)
				w.updateCtxs[0].TableDef.TblId = uint64(99 + p)
				w.updateCtxs[0].TableDef.Cols[0].Typ.Id = int32(types.T_varchar)
				w.updateCtxs[0].TableDef.Cols[0].Typ.Width = 1 << 20
				w.deleteBlockInfo = make([]*deleteBlockInfo, 1)
				w.insertBlockInfo = make([]*batch.Batch, 1)
				w.insertBlockRowCount = make([]uint64, 1)
				w.deleteFutureBytes = make([]int64, 1)
				w.deleteKeys = make([]*deleteKeySet, 1)
				w.action = actionDelete
				op.writers[uint64(99+p)] = w
				writers = append(writers, w)
				key := strings.Repeat(string(rune('a'+p)), 200<<10)
				keys[uint64(99+p)] = key
				source := batch.NewWithSchema(false, DeleteBatchAttrs, []types.Type{types.T_Rowid.ToType(), types.T_varchar.ToType()})
				func() {
					defer source.Clean(proc.Mp())
					var block types.Blockid
					copy(block[:], colexec.TxnWorkspaceSegment[:])
					require.NoError(t, vector.AppendFixed(source.Vecs[0], types.NewRowid(&block, uint32(p)), false, proc.Mp()))
					require.NoError(t, vector.AppendBytes(source.Vecs[1], []byte(key), false, proc.Mp()))
					source.SetRowCount(1)
					prepared, err := w.prepareDeleteBatches(proc, 0, []*batch.Batch{source}, false)
					require.NoError(t, err)
					require.Empty(t, prepared)
				}()
			}
			for p, w := range writers {
				source := committedDeleteInput(t, proc, 10+p*192, 192)
				source.Vecs[1].Free(proc.Mp())
				source.Vecs[1] = testutil.MakeVarcharVector(func() []string {
					values := make([]string, 192)
					for i := range values {
						values[i] = strconv.Itoa(p*192 + i)
					}
					return values
				}(), nil, proc.Mp())
				require.NoError(t, w.append(proc, raw.OpAnalyzer, source))
				require.Positive(t, w.memController.grantedSize)
			}
			for _, w := range writers {
				var head [9]byte
				_, err := w.metadataSpool.ReadAt(head[:], 0)
				require.NoError(t, err)
				frame, err := colexec.MetadataOutputCapacity(0, int64(binary.LittleEndian.Uint64(head[:8])))
				require.NoError(t, err)
				require.Less(t, frame, total)
				require.Greater(t, frame, total-writers[0].memController.grantedSize, "even after its own sources retire, either first partition would wait on its sibling")
			}
			files := &partitionWriteFailureFS{FileService: proc.GetFileService()}
			proc.Base.FileService = files
			if strings.HasPrefix(mode, "late_") {
				files.failAt = 2
				if mode == "late_cancel" {
					files.cancel = cancel
				}
			}
			if mode != "eof" && mode != "partial_eval" && mode != "decode_error" {
				if mode == "cancel" {
					cancel()
				} else if mode == "write_error" {
					proc.Base.FileService = &writeErrorFS{FileService: proc.GetFileService(), err: errors.New("injected partition flush failure")}
				}
				_, err := op.Call(proc)
				require.Error(t, err)
				if mode == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				}
				require.Equal(t, 2, len(op.writers)+len(op.freeWriters), "failed sealing keeps every writer owned for cleanup")
				if mode == "late_error_reset" {
					op.Reset(proc, true, err)
				} else {
					op.Free(proc, true, err)
				}
				if strings.HasPrefix(mode, "late_") {
					require.Len(t, files.written, 1, "one object was persisted before the next failure")
					for _, path := range files.written {
						_, statErr := files.StatFile(context.Background(), path)
						require.True(t, moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound), "unpublished object must be cleaned: %s, err %v", path, statErr)
					}
					require.False(t, files.cancelledDelete, "cleanup outlives query cancellation")
				}
				require.Equal(t, total, owner.Available())
				for _, w := range writers {
					require.Nil(t, w.metadataSpool)
					require.Nil(t, w.ownedMP)
				}
				return
			}
			rows := make(map[uint64]uint64)
			seen := make(map[uint64]bool)
			corrupted := false
			for calls := 0; ; calls++ {
				require.Less(t, calls, 8)
				result, err := op.Call(proc)
				if mode == "decode_error" && err != nil {
					require.ErrorContains(t, err, "row")
					op.Free(proc, true, err)
					require.Len(t, files.deleteContexts, 2)
					require.Same(t, files.deleteContexts[0], files.deleteContexts[1], "all partitions must share one teardown deadline")
					require.Len(t, files.written, 2)
					for _, path := range files.written {
						_, err := files.StatFile(context.Background(), path)
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "a failed decode must not retire cleanup ownership")
					}
					require.Equal(t, total, owner.Available())
					return
				}
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				for _, w := range writers {
					require.True(t, w.tailFlushed)
					require.Zero(t, w.memController.grantedSize)
					require.Zero(t, w.retainedBytes())
				}
				if mode == "decode_error" && !corrupted {
					for _, w := range writers {
						for offset := int64(0); offset < w.metadataWritten; {
							var head [9]byte
							_, err := w.metadataSpool.ReadAt(head[:], offset)
							require.NoError(t, err)
							if head[8]&metadataObjectNamesFlag != 0 {
								var negative [8]byte
								for i := range negative {
									negative[i] = 0xff
								}
								_, err = w.metadataSpool.WriteAt(negative[:], offset+9)
								require.NoError(t, err)
							}
							offset += 9 + int64(binary.LittleEndian.Uint64(head[:8]))
						}
					}
					corrupted = true
				}
				bat := result.Batch
				id := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[1])[0]
				rows[id] += vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[2])[0]
				if mode == "partial_eval" && string(bat.Vecs[3].GetBytesAt(0)) != objectio.PhysicalAddr_Attr {
					relation := mock_frontend.NewMockRelation(ctrl)
					relation.EXPECT().GetTableName().Return("partition_cleanup").AnyTimes()
					var transferred string
					relation.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, data *batch.Batch, _ string) error {
						stats := objectio.ObjectStats(data.Vecs[0].GetBytesAt(0))
						transferred = stats.ObjectName().String()
						_, err := files.StatFile(context.Background(), transferred)
						return err
					})
					copy, err := bat.Dup(proc.Mp())
					require.NoError(t, err)
					consumerChild := colexec.NewMockOperator().WithBatchs([]*batch.Batch{copy})
					func() {
						defer consumerChild.Free(proc, false, nil)
						consumer := NewArgument()
						defer consumer.Release()
						defer consumer.Free(proc, false, nil)
						consumer.Action = UpdateFlushS3Info
						consumer.OpAnalyzer = process.NewTempAnalyzer()
						consumer.ctr.sources = map[uint64]engine.Relation{id: relation}
						consumer.AppendChild(consumerChild)
						_, err = consumer.Call(proc)
						require.NoError(t, err)
					}()
					require.NotEmpty(t, transferred)
					cancel()
					op.Free(proc, true, context.Canceled)
					require.Len(t, files.written, 2)
					for _, path := range files.written {
						_, err := files.StatFile(context.Background(), path)
						if path == transferred {
							require.NoError(t, err, "the consumer now owns this object")
						} else {
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "unread partition output must be cleaned")
						}
					}
					require.Equal(t, total, owner.Available())
					return
				}
				if string(bat.Vecs[3].GetBytesAt(0)) == objectio.PhysicalAddr_Attr {
					decoded := batch.NewOffHeapEmpty()
					func() {
						defer decoded.Clean(proc.Mp())
						require.NoError(t, decoded.UnmarshalBinaryWithAnyMp(bat.Vecs[4].GetBytesAt(0), proc.Mp()))
						require.Equal(t, keys[id], string(decoded.Vecs[1].GetBytesAt(0)))
						require.False(t, seen[id])
						seen[id] = true
					}()
				}
			}
			require.NotEqual(t, "decode_error", mode, "injected decode failure must be observed")
			require.Equal(t, map[uint64]uint64{99: 193, 100: 193}, rows)
			require.Len(t, seen, 2)
			require.Equal(t, total, owner.Available())
			op.Free(proc, false, nil)
			for _, w := range writers {
				require.Nil(t, w.metadataSpool)
				require.Nil(t, w.ownedMP)
			}

		})
	}
}

// The failure occurs after a prior real file write, independently of map order.
type partitionWriteFailureFS struct {
	fileservice.FileService
	calls           int
	failAt          int
	cancel          context.CancelFunc
	written         []string
	cancelledDelete bool
	deleteContexts  []context.Context
}

func (fs *partitionWriteFailureFS) Write(ctx context.Context, v fileservice.IOVector) error {
	fs.calls++
	if fs.failAt > 0 && fs.calls == fs.failAt {
		if fs.cancel != nil {
			fs.cancel()
			return ctx.Err()
		}
		return errors.New("injected later partition write failure")
	}
	if err := fs.FileService.Write(ctx, v); err != nil {
		return err
	}
	fs.written = append(fs.written, v.FilePath)
	return nil
}

func (fs *partitionWriteFailureFS) Delete(ctx context.Context, paths ...string) error {
	fs.cancelledDelete = fs.cancelledDelete || ctx.Err() != nil
	fs.deleteContexts = append(fs.deleteContexts, ctx)
	return fs.FileService.Delete(ctx, paths...)
}

func TestPreflushedInsertObjectOwnership(t *testing.T) {
	for _, handoff := range []bool{false, true} {
		t.Run(strconv.FormatBool(handoff), func(t *testing.T) {
			_, ctrl, proc := prepareTestCtx(t, true)
			t.Cleanup(proc.Free)
			t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
			shared, err := colexec.GetSharedFSFromProc(proc)
			require.NoError(t, err)
			local, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.LocalFileServiceName)
			require.NoError(t, err)
			files := &partitionWriteFailureFS{FileService: shared}
			proc.Base.FileService, err = fileservice.NewFileServices(defines.SharedFileServiceName, files, local)
			require.NoError(t, err)
			tc := buildTestCase(prepareTestInsertMultiUpdateCtx(false, false), prepareTestEng(ctrl, false), nil, 0, UpdateWriteS3, false)
			t.Cleanup(func() { tc.op.Free(proc, false, nil) })
			require.NoError(t, tc.op.Prepare(proc))
			w := tc.op.ctr.s3Writer
			w.cleanupUnpublished = true // The partition EOF entry activates this scope.
			bat := batch.NewWithSize(4)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Vecs[0] = testutil.MakeInt64Vector([]int64{2, 1}, nil, proc.Mp())
			bat.Vecs[1] = testutil.MakeVarcharVector([]string{"b", "a"}, nil, proc.Mp())
			bat.Vecs[2] = testutil.MakeInt32Vector([]int32{20, 10}, nil, proc.Mp())
			bat.Vecs[3] = testutil.MakeInt32Vector([]int32{20, 10}, nil, proc.Mp())
			bat.SetRowCount(2)
			require.NoError(t, w.append(proc, tc.op.OpAnalyzer, bat))
			require.NoError(t, w.flushTail(proc, tc.op.OpAnalyzer))
			require.Len(t, files.written, 1)
			if handoff {
				require.NoError(t, w.readMetadataFrame(proc))
				require.Equal(t, uint8(actionInsert), vector.MustFixedColNoTypeCheck[uint8](w.outputBat.Vecs[0])[0])
				inner := batch.NewOffHeapEmpty()
				defer inner.Clean(proc.Mp())
				require.NoError(t, inner.UnmarshalBinaryWithAnyMp(w.outputBat.Vecs[4].GetBytesAt(0), proc.Mp()))
				stats := objectio.ObjectStats(inner.Vecs[1].GetBytesAt(0))
				require.Equal(t, files.written[0], stats.ObjectName().String())
				require.Equal(t, uint32(2), stats.Rows())
			}
			require.NoError(t, w.free(proc))
			_, err = files.StatFile(context.Background(), files.written[0])
			if handoff {
				require.NoError(t, err, "insert output belongs to the metadata consumer")
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
			}
		})
	}
}
