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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

type partitionCountingReader struct {
	readutil.EmptyReader
	reads, closes int
	filter        docfilter.MembershipFilter
}

func (r *partitionCountingReader) Read(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error) {
	r.reads++
	return true, nil
}
func (r *partitionCountingReader) Close() error {
	r.closes++
	if r.filter != nil {
		r.filter.Free()
		r.filter = nil
	}
	return nil
}

func partitionTestRanges(ids ...byte) engine.RelData {
	data := readutil.NewBlockListRelationData(0)
	for _, id := range ids {
		data.AppendBlockInfo(&objectio.BlockInfo{BlockID: types.Blockid{id}})
	}
	return data
}

func TestPartitionBlockReadersCoverageAndCleanup(t *testing.T) {
	for _, count := range []int{0, 1, 2} {
		for _, num := range []int{1, 3} {
			t.Run(fmt.Sprintf("parts%d-dop%d", count, num), func(t *testing.T) {
				parts := make([]engine.RelData, count)
				for i := range parts {
					parts[i] = partitionTestRanges(byte(i + 1))
				}
				var owned []*partitionCountingReader
				calls := 0
				readers, err := buildPartitionBlockReaders(context.Background(), parts, num, func(data engine.RelData) ([]engine.Reader, error) {
					require.Same(t, parts[calls], data)
					calls++
					result := make([]engine.Reader, num)
					for i := range result {
						rd := &partitionCountingReader{}
						owned = append(owned, rd)
						result[i] = rd
					}
					return result, nil
				})
				t.Cleanup(func() { closeReaders(readers) })
				require.NoError(t, err)
				require.Len(t, readers, num)
				require.Equal(t, count, calls)
				for _, reader := range readers {
					end, err := reader.Read(context.Background(), nil, nil, nil, nil)
					require.NoError(t, err)
					require.True(t, end)
					require.NoError(t, reader.Close())
				}
				for _, reader := range owned {
					require.Equal(t, 1, reader.reads)
					require.Equal(t, 1, reader.closes)
				}
			})
		}
	}
}

func TestPartitionBlockReadersFailureOwnership(t *testing.T) {
	failure := errors.New("second partition construction failed")
	for _, kind := range []string{"error", "cancel", "wrong count", "nil reader", "already canceled"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			admission := new(remoteMembershipFilterAdmission)
			root, err := docfilter.NewWithMemoryAdmission(append([]byte{docfilter.TagSorted64}, make([]byte, 8)...), admission)
			require.NoError(t, err)
			t.Cleanup(func() {
				if root != nil {
					root.Free()
				}
			})
			var owned []*partitionCountingReader
			calls := 0
			if kind == "already canceled" {
				cancel()
			}
			result, err := buildPartitionBlockReaders(ctx, []engine.RelData{partitionTestRanges(1), partitionTestRanges(2)}, 1, func(engine.RelData) ([]engine.Reader, error) {
				calls++
				rd := &partitionCountingReader{filter: root.Share()}
				owned = append(owned, rd)
				if calls == 1 {
					return []engine.Reader{rd}, nil
				}
				switch kind {
				case "error":
					return []engine.Reader{rd}, failure
				case "cancel":
					cancel()
					return []engine.Reader{rd}, nil
				case "wrong count":
					return []engine.Reader{rd, new(readutil.EmptyReader)}, nil
				case "nil reader":
					rd.Close()
					owned = owned[:len(owned)-1]
					return []engine.Reader{nil}, nil
				}
				return []engine.Reader{rd}, nil
			})
			require.Error(t, err)
			require.Nil(t, result)
			if kind == "error" {
				require.ErrorIs(t, err, failure)
			}
			if kind == "cancel" || kind == "already canceled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			if kind == "already canceled" {
				require.Zero(t, calls)
			}
			for _, reader := range owned {
				require.Equal(t, 1, reader.closes)
			}
			require.True(t, root.Valid(), "factory shares must not consume caller's root")
			require.Zero(t, admission.released.Load())
			root.Free()
			root = nil
			require.Equal(t, admission.acquired.Load(), admission.released.Load())
		})
	}
	for _, num := range []int{0, -1} {
		_, err := buildPartitionBlockReaders(context.Background(), nil, num, nil)
		require.Error(t, err)
	}
}

type unshareablePartitionFilter struct{ engine.MembershipFilter }

type partitionSnapshotTombstones struct {
	engine.Tombstoner
	seen []types.TS
}

func (t *partitionSnapshotTombstones) HasAnyTombstoneFile() bool { return true }
func (t *partitionSnapshotTombstones) ApplyPersistedTombstones(_ context.Context, _ fileservice.FileService, ts *types.TS, _ *objectio.Blockid, rows []int64, _ *objectio.Bitmap) ([]int64, error) {
	t.seen = append(t.seen, *ts)
	return rows, nil
}

func TestPartitionRemoteTombstoneAndSnapshotContract(t *testing.T) {
	ctx := context.Background()
	ts := timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 7}
	parts := []engine.RelData{partitionTestRanges(1), partitionTestRanges(2)}
	data := readutil.NewEmptyTombstoneData()
	tomb := &partitionSnapshotTombstones{Tombstoner: data}
	for i, part := range parts {
		block := part.GetBlockInfo(0)
		require.NoError(t, data.AppendInMemory(types.NewRowid(&block.BlockID, uint32(i+2))))
	}
	tomb.SortInMemory()
	combined := &CombinedRelData{tables: parts, cnt: 2}
	require.NoError(t, combined.AttachTombstones(tomb))
	calls := 0
	readers, err := buildPartitionBlockReaders(ctx, combined.tables, 1, func(data engine.RelData) ([]engine.Reader, error) {
		require.Same(t, parts[calls], data)
		block := data.GetBlockInfo(0)
		source := readutil.NewRemoteDataSource(ctx, nil, ts, data)
		defer source.Close()
		left, err := source.ApplyTombstones(ctx, &block.BlockID, []int64{2, 3}, 0)
		require.NoError(t, err)
		require.Equal(t, []int64{int64(3 - calls)}, left)
		mask, err := source.GetTombstones(ctx, &block.BlockID)
		require.NoError(t, err)
		defer mask.Release()
		require.True(t, mask.Contains(uint64(calls+2)))
		require.False(t, mask.Contains(uint64(3-calls)))
		calls++
		return []engine.Reader{new(readutil.EmptyReader)}, nil
	})
	t.Cleanup(func() { closeReaders(readers) })
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Len(t, tomb.seen, 4)
	for _, got := range tomb.seen {
		require.Equal(t, types.TimestampToTS(ts), got)
	}
}

func TestCombinedRemoteMembershipFilterOwnership(t *testing.T) {
	for _, mode := range []string{"transported", "caller-owned", "unshareable"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			admission := new(remoteMembershipFilterAdmission)
			rt := moruntime.ServiceRuntime(proc.GetService())
			previous, hadPrevious := rt.GetGlobalVariables(moruntime.CNMemoryThrottler)
			rt.SetGlobalVariables(moruntime.CNMemoryThrottler, admission)
			t.Cleanup(func() {
				if hadPrevious {
					rt.SetGlobalVariables(moruntime.CNMemoryThrottler, previous)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.CNMemoryThrottler, admission)
				}
			})
			payload := append([]byte{docfilter.TagSorted64}, make([]byte, 8)...)
			hint := engine.FilterHint{MembershipFilterBytes: payload}
			var root docfilter.MembershipFilter
			if mode != "transported" {
				var err error
				root, err = docfilter.NewWithMemoryAdmission(payload, admission)
				require.NoError(t, err)
				t.Cleanup(func() {
					if root != nil {
						root.Free()
					}
				})
				hint = engine.FilterHint{BF: root}
				if mode == "unshareable" {
					hint.BF = &unshareablePartitionFilter{root}
				}
			}
			e := &Engine{service: proc.GetService(), fs: proc.GetFileService()}
			data := &CombinedRelData{tables: []engine.RelData{partitionTestRanges(1, 2), partitionTestRanges(3, 4)}, cnt: 4}
			readers, err := e.BuildBlockReaders(context.Background(), proc, timestamp.Timestamp{PhysicalTime: 42}, nil,
				&plan.TableDef{Name: "partitioned", Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk"}}, data, 2, hint)
			t.Cleanup(func() {
				for _, reader := range readers {
					if reader != nil {
						require.NoError(t, reader.Close())
					}
				}
			})
			if mode == "unshareable" {
				require.ErrorContains(t, err, "shareable")
				require.Empty(t, readers)
				require.True(t, root.Valid())
			} else {
				require.NoError(t, err)
				require.Len(t, readers, 2)
				require.Equal(t, int64(8), admission.acquired.Load())
				require.NoError(t, readers[0].Close())
				readers[0] = nil
				require.Zero(t, admission.released.Load())
				require.NoError(t, readers[1].Close())
				readers[1] = nil
			}
			if root != nil {
				require.Zero(t, admission.released.Load())
				require.True(t, root.Valid())
				root.Free()
				root = nil
			}
			require.Equal(t, admission.acquired.Load(), admission.released.Load())
		})
	}
}
