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

package mergeblock

import (
	"math"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func metadataMemoryFrame(t *testing.T, mp *mpool.MPool, rows int, distinct bool) *batch.Batch {
	t.Helper()
	b := batch.NewOffHeapWithSize(2)
	b.Attrs = []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
	b.Vecs[0] = vector.NewOffHeapVecWithType(types.T_text.ToType())
	b.Vecs[1] = vector.NewOffHeapVecWithType(types.T_binary.ToType())
	t.Cleanup(func() { b.Clean(mp) })
	segment := objectio.NewSegmentid()
	for row := 0; row < rows; row++ {
		number := uint16(0)
		if distinct {
			number = uint16(row)
		}
		name := objectio.BuildObjectName(segment, number)
		loc := blockio.EncodeLocation(name, objectio.Extent{}, 7, uint16(row))
		sid := loc.Name().SegmentId()
		info := objectio.BlockInfo{BlockID: *objectio.NewBlockid(&sid, name.Num(), loc.ID())}
		info.SetMetaLocation(loc)
		require.NoError(t, vector.AppendBytes(b.Vecs[0], objectio.EncodeBlockInfo(&info), false, mp))
		if distinct || row == 0 {
			require.NoError(t, vector.AppendBytes(b.Vecs[1], objectio.ZeroObjectStats[:], false, mp))
		}
	}
	b.SetRowCount(rows)
	return b
}

func TestMetadataOutputBoundCoversRealSplitHighwater(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	sourceMP := mpool.MustNew("merge-metadata-source")
	t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
	op := &MergeBlock{container: Container{mp: make(map[int]*batch.Batch), mp2: make(map[int][]*batch.Batch)}, AddAffectedRows: true}
	t.Cleanup(func() { op.Free(proc, false, nil) })
	analyzer := process.NewAnalyzer(0, false, false, "metadata-bound")
	frames := []*batch.Batch{
		metadataMemoryFrame(t, sourceMP, 65, true),
		metadataMemoryFrame(t, sourceMP, 129, false),
		metadataMemoryFrame(t, sourceMP, 1, true),
		metadataMemoryFrame(t, sourceMP, 129, true),
	}
	var frameLimit int64
	for _, frame := range frames {
		frameLimit = max(frameLimit, frame.MarshalBinarySize())
	}
	bound, ok := MetadataOutputBound(frameLimit)
	require.True(t, ok)
	var total uint64
	for _, frame := range frames {
		require.LessOrEqual(t, frame.RowCount(), bound.Rows)
		var logical int64
		for col, vec := range frame.Vecs {
			payload := vec.WindowSize(0, vec.Length())
			logical += payload
			require.LessOrEqual(t, payload, bound.ColumnBytes[col])
		}
		require.LessOrEqual(t, logical, bound.BatchBytes)
		require.NoError(t, op.Split(proc, frame, analyzer))
		private := op.container.mp[0]
		require.Equal(t, frame.RowCount(), private.RowCount())
		require.Equal(t, frame.Vecs[1].Length(), private.Vecs[1].Length())
		require.Empty(t, op.container.mp2, "modern metadata never enters the legacy raw-batch decoder")
		// The independent MP highwater includes real old/new AppendBytes
		// replacements. Temporary object metadata coexists with these copies.
		meta := int64(len(objectio.BuildObjectMeta(uint16(frame.RowCount()))))
		objects := int64(unsafe.Sizeof(batch.Batch{}) + 2*unsafe.Sizeof(vector.Vector{}))
		require.LessOrEqual(t, proc.Mp().Stats().HighWaterMark.Load()+2*meta+objects, bound.ScratchBytes)
		total += uint64(frame.RowCount() * 7)
	}
	require.Equal(t, total, op.GetAffectedRows())
	retained := proc.Mp().AllocatedBytes()
	require.Positive(t, retained)
	op.Reset(proc, false, nil)
	require.Equal(t, retained, proc.Mp().AllocatedBytes(), "Reset clears row contents while the root I promise still owns highwater copies")
	op.Free(proc, false, nil)
	require.Zero(t, proc.Mp().AllocatedBytes())
}

func TestMetadataOutputBoundUsesFinalFrameCapacity(t *testing.T) {
	small, ok := MetadataOutputBound(4 << 10)
	require.True(t, ok)
	large, ok := MetadataOutputBound(1 << 20)
	require.True(t, ok)
	require.Greater(t, large.Rows, small.Rows)
	require.Greater(t, large.ScratchBytes, small.ScratchBytes)
	require.Equal(t, int64(1<<20), large.BatchBytes)
	require.Equal(t, int64(objectio.BlockInfoSize+objectio.ObjectStatsLen+2*types.VarlenaSize), large.RowBytes)
	for _, invalid := range []int64{-1, 0, int64(objectio.BlockInfoSize + types.VarlenaSize - 1), math.MaxInt64} {
		_, ok := MetadataOutputBound(invalid)
		require.False(t, ok)
	}
}

func TestMergeBlockSplitPropagatesAllocationFailure(t *testing.T) {
	for _, stage := range []string{"block-info", "object-stats"} {
		t.Run(stage, func(t *testing.T) {
			limited, err := mpool.NewMPool("merge-split-failure", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			t.Cleanup(func() { mpool.DeleteMPool(limited) })
			proc := testutil.NewProcessWithMPool(t, "", limited)
			t.Cleanup(proc.Free)
			sourceMP := mpool.MustNew("merge-failure-input")
			t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
			frame := metadataMemoryFrame(t, sourceMP, 1, true)
			op := &MergeBlock{container: Container{mp: make(map[int]*batch.Batch), mp2: make(map[int][]*batch.Batch)}}
			t.Cleanup(func() { op.Free(proc, false, nil) })
			if stage == "object-stats" {
				// Prewarm only the block-info column. Its copy can reuse
				// capacity, while the first stats allocation must fail.
				op.GetMetaLocBat(frame, proc)
				require.NoError(t, vector.AppendBytes(op.container.mp[0].Vecs[0], frame.Vecs[0].GetBytesAt(0), false, limited))
			}
			ballast, err := limited.Alloc((1<<20)-int(limited.CurrNB()), true)
			require.NoError(t, err)
			t.Cleanup(func() {
				if ballast != nil {
					limited.Free(ballast)
				}
			})
			baseline := int64(len(ballast))
			err = op.Split(proc, frame, process.NewAnalyzer(0, false, false, "split-failure"))
			require.ErrorContains(t, err, "mpool out of space", "an unsuccessful metadata copy must not be published as successful Split")
			if stage == "block-info" {
				require.Zero(t, op.container.mp[0].Vecs[0].Length())
			} else {
				require.Equal(t, 1, op.container.mp[0].Vecs[0].Length())
				require.Zero(t, op.container.mp[0].Vecs[1].Length())
			}
			op.Free(proc, true, err)
			require.Equal(t, baseline, limited.AllocatedBytes(), "Free destroys all partial metadata owners after the real native-MPool rejection")
			limited.Free(ballast)
			ballast = nil
			op.container.mp, op.container.mp2 = make(map[int]*batch.Batch), make(map[int][]*batch.Batch)
			require.NoError(t, op.Split(proc, frame, process.NewAnalyzer(0, false, false, "split-reuse")))
			require.Equal(t, 1, op.container.mp[0].RowCount())
			require.Equal(t, 1, op.container.mp[0].Vecs[1].Length())
			op.Free(proc, false, nil)
			require.Zero(t, limited.AllocatedBytes())
		})
	}
}

func TestMergeMetadataBoundUsesInsertProtocolInsteadOfRawSlot(t *testing.T) {
	protocol, err := colexec.InsertMetadataFrameLimit(1, 1)
	require.NoError(t, err)
	bound, ok := MetadataOutputBound(protocol)
	require.True(t, ok)
	raw, ok := MetadataOutputBound(512 << 20)
	require.True(t, ok)
	require.Less(t, bound.ScratchBytes, raw.ScratchBytes)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	sourceMP := mpool.MustNew("dedicated-metadata-source")
	t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
	frame := metadataMemoryFrame(t, sourceMP, 129, true)
	require.LessOrEqual(t, frame.MarshalBinarySize(), protocol)
	op := &MergeBlock{container: Container{mp: make(map[int]*batch.Batch), mp2: make(map[int][]*batch.Batch)}}
	t.Cleanup(func() { op.Free(proc, false, nil) })
	require.NoError(t, op.Split(proc, frame, process.NewAnalyzer(0, false, false, "dedicated-metadata")))
	require.Equal(t, frame.RowCount(), op.container.mp[0].RowCount())
	require.Equal(t, frame.Vecs[1].Length(), op.container.mp[0].Vecs[1].Length())
	require.LessOrEqual(t, proc.Mp().Stats().HighWaterMark.Load(), bound.ScratchBytes)
	op.Free(proc, false, nil)
	require.Zero(t, proc.Mp().AllocatedBytes())
}
