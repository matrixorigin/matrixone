// Copyright 2026 Matrix Origin
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

package mergesort

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Use the real ordinary merger, writer, object encoding and checked readback.
// The first generation makes a multi-row raw-sorted run which intentionally
// disagrees with the writer's decoded relation. Feed it into another merge.
func TestJSONMergeObjsRetainsRawRunThroughWriteAndRead(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	encode := func(text string) []byte {
		value, err := bytejson.ParseFromString(text)
		require.NoError(t, err)
		raw, err := value.Marshal()
		require.NoError(t, err)
		return raw
	}
	merge := func(runs ...[][]byte) [][]byte {
		writer := ioutil.ConstructWriter(0, []uint16{0}, 0, false, false, fs)
		host := &jsonReadbackMergeHost{mp: mp, writer: writer, loaded: make([]bool, len(runs))}
		for _, rows := range runs {
			bat := batch.NewWithSize(1)
			bat.Attrs = []string{"j"}
			bat.Vecs[0] = vector.NewVec(types.T_json.ToType())
			for _, raw := range rows {
				require.NoError(t, vector.AppendBytes(bat.Vecs[0], raw, false, mp))
			}
			bat.SetRowCount(len(rows))
			host.runs = append(host.runs, bat)
			defer bat.Clean(mp)
		}
		require.NoError(t, mergeObjs(ctx, host, 0))
		require.Len(t, host.GetCommitEntry().CreatedObjs, 1, "merge must persist its output")
		reader, err := ioutil.NewFileReaderNoCache(fs, writer.GetName().String())
		require.NoError(t, err)
		bats, release, err := reader.LoadAllColumns(ctx, []uint16{0}, mp)
		require.NoError(t, err)
		defer release()
		var got [][]byte
		for _, bat := range bats {
			for row := 0; row < bat.Vecs[0].Length(); row++ {
				got = append(got, bytes.Clone(bat.Vecs[0].GetBytesAt(row)))
			}
		}
		return got
	}
	a, b, c := encode(`[100]`), encode(`[0,0]`), encode(`[0,0,0]`)
	require.Less(t, bytes.Compare(a, b), 0)
	require.Greater(t, bytejson.CompareByteJsonPhysical(types.DecodeJson(a), types.DecodeJson(b)), 0)
	retained := merge([][]byte{b}, [][]byte{a})
	require.Equal(t, [][]byte{a, b}, retained)
	require.Equal(t, [][]byte{a, b, c}, merge(retained, [][]byte{c}))
}

type jsonReadbackMergeHost struct {
	transferSlabFailureHost
	mp     *mpool.MPool
	writer *ioutil.BlockWriter
	runs   []*batch.Batch
	loaded []bool
}

func (h *jsonReadbackMergeHost) GetVector(typ *types.Type) (*vector.Vector, func()) {
	v := vector.NewVec(*typ)
	return v, func() { v.Free(h.mp) }
}
func (h *jsonReadbackMergeHost) GetMPool() *mpool.MPool                { return h.mp }
func (h *jsonReadbackMergeHost) PrepareNewWriter() *ioutil.BlockWriter { return h.writer }
func (h *jsonReadbackMergeHost) DoTransfer() bool                      { return false }
func (h *jsonReadbackMergeHost) GetObjectCnt() int                     { return len(h.runs) }
func (h *jsonReadbackMergeHost) GetBlkCnts() []int {
	counts := make([]int, len(h.runs))
	for i := range counts {
		counts[i] = 1
	}
	return counts
}
func (h *jsonReadbackMergeHost) GetAccBlkCnts() []int {
	counts := make([]int, len(h.runs))
	for i := range counts {
		counts[i] = i
	}
	return counts
}
func (h *jsonReadbackMergeHost) GetSortKeyType() types.Type { return types.T_json.ToType() }
func (h *jsonReadbackMergeHost) GetBlockMaxRows() uint32    { return 3 }
func (h *jsonReadbackMergeHost) LoadNextBatch(_ context.Context, idx uint32, _ *batch.Batch) (*batch.Batch, *nulls.Nulls, func(), error) {
	if h.loaded[idx] {
		return nil, nil, nil, ErrNoMoreBlocks
	}
	h.loaded[idx] = true
	return h.runs[idx], &nulls.Nulls{}, func() {}, nil
}
