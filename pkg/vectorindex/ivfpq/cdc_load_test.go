//go:build gpu

// Copyright 2022 Matrix Origin
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

package ivfpq

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func makeCdcChunkBatch(proc *process.Process, chunks []cuvscdc.EventChunk) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_blob, 65536, 0))
	for _, ch := range chunks {
		vector.AppendFixed[int64](bat.Vecs[0], ch.ChunkId, false, proc.Mp())
		vector.AppendBytes(bat.Vecs[1], ch.Data, false, proc.Mp())
	}
	bat.SetRowCount(len(chunks))
	return bat
}

func encodeChunk(t *testing.T, dim, includeBytesPerRow int, ops []cuvscdc.CdcOp, pkids []int64, vecs [][]float32, includes [][]byte) []byte {
	t.Helper()
	var buf []byte
	insIdx := 0
	for i, op := range ops {
		var v []float32
		var inc []byte
		if op == cuvscdc.CdcOpInsert {
			v = vecs[insIdx]
			if includeBytesPerRow > 0 {
				inc = includes[insIdx]
			}
			insIdx++
		}
		var vb []byte
		if v != nil {
			vb = util.UnsafeSliceToBytes(v)
		}
		out, err := cuvscdc.EncodeEventRecord(buf, op, pkids[i], vb, inc, 4*dim, includeBytesPerRow)
		require.NoError(t, err)
		buf = out
	}
	return cuvscdc.FrameCdcChunk(buf, nil, 0, 0, 0)
}

func TestLoadCdcEventsFromDB_RoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	tblcfg := testTblcfg()
	dim := 4
	ops := []cuvscdc.CdcOp{cuvscdc.CdcOpDelete, cuvscdc.CdcOpInsert, cuvscdc.CdcOpDelete}
	pkids := []int64{42, 7, 9}
	vecs := [][]float32{{1, 2, 3, 4}}
	chunkBytes := encodeChunk(t, dim, 0, ops, pkids, vecs, nil)
	chunks := []cuvscdc.EventChunk{{ChunkId: 0, Data: chunkBytes}}

	orig := runSql
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeCdcChunkBatch(proc, chunks)}}, nil
	}
	defer func() { runSql = orig }()

	idx := &IvfpqModel[float32, float32]{Id: "idx-1"}
	got, err := idx.loadCdcEventsFromDB(sqlproc, tblcfg)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, int64(0), got[0].ChunkId)
	require.Equal(t, chunkBytes, got[0].Data)
}

func TestLoadCdcEventsFromDB_Empty(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	orig := runSql
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: proc.Mp()}, nil
	}
	defer func() { runSql = orig }()

	idx := &IvfpqModel[float32, float32]{Id: "idx-1"}
	got, err := idx.loadCdcEventsFromDB(sqlproc, testTblcfg())
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestReplayEventChunks_DeleteInsertDelete(t *testing.T) {
	dim := 4
	chunkBytes := encodeChunk(t, dim, 0,
		[]cuvscdc.CdcOp{cuvscdc.CdcOpDelete, cuvscdc.CdcOpInsert, cuvscdc.CdcOpDelete},
		[]int64{1, 1, 1},
		[][]float32{{1, 2, 3, 4}},
		nil,
	)
	chunks := []cuvscdc.EventChunk{{ChunkId: 0, Data: chunkBytes}}

	delPkids, ovPkids, ovVecs, ovInc, err := replayEventChunks[float32](chunks, dim, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, delPkids)
	require.Empty(t, ovPkids)
	require.Empty(t, ovVecs)
	require.Empty(t, ovInc)
}

func TestReplayEventChunks_FlattenOverflow(t *testing.T) {
	dim := 3
	chunkBytes := encodeChunk(t, dim, 0,
		[]cuvscdc.CdcOp{cuvscdc.CdcOpInsert, cuvscdc.CdcOpInsert},
		[]int64{10, 20},
		[][]float32{{1, 2, 3}, {4, 5, 6}},
		nil,
	)
	chunks := []cuvscdc.EventChunk{{ChunkId: 0, Data: chunkBytes}}

	delPkids, ovPkids, ovVecs, _, err := replayEventChunks[float32](chunks, dim, 0)
	require.NoError(t, err)
	require.Empty(t, delPkids)
	require.Equal(t, []int64{10, 20}, ovPkids)
	require.Equal(t, []float32{1, 2, 3, 4, 5, 6}, ovVecs)
}

func TestReplayEventChunks_MultiChunkOrder(t *testing.T) {
	dim := 2
	chunk0 := encodeChunk(t, dim, 0,
		[]cuvscdc.CdcOp{cuvscdc.CdcOpInsert}, []int64{5},
		[][]float32{{1, 1}}, nil)
	chunk1 := encodeChunk(t, dim, 0,
		[]cuvscdc.CdcOp{cuvscdc.CdcOpDelete}, []int64{5},
		nil, nil)
	chunks := []cuvscdc.EventChunk{
		{ChunkId: 1, Data: chunk1},
		{ChunkId: 0, Data: chunk0},
	}
	delPkids, ovPkids, _, _, err := replayEventChunks[float32](chunks, dim, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{5}, delPkids)
	require.Empty(t, ovPkids)
}

// TestLoadIndex_WithCdcDeltas builds an ivfpq index, saves it, and then loads
// it with mocked tag=1 event-log chunks present. See cagra/cdc_load_test.go
// for the architectural commentary.
func TestLoadIndex_WithCdcDeltas(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg := testIdxcfg()
	tblcfg := testTblcfg()
	ids := make([]int64, testNVectors)
	for i := range ids {
		ids[i] = int64(i + 5000)
	}

	built := buildTestModel(t, "cdc-deltas", ids)
	tarPath := built.Path
	defer os.Remove(tarPath)

	deletedPkids := []int64{ids[0], 9999, ids[1]}
	overflowPkids := []int64{777, 888}
	overflowVecs := make([]float32, len(overflowPkids)*testDim)
	for i := range overflowVecs {
		overflowVecs[i] = float32(i + 1)
	}
	ops := []cuvscdc.CdcOp{
		cuvscdc.CdcOpDelete, cuvscdc.CdcOpDelete, cuvscdc.CdcOpDelete,
		cuvscdc.CdcOpInsert, cuvscdc.CdcOpInsert,
	}
	pkidsAll := append([]int64{}, deletedPkids...)
	pkidsAll = append(pkidsAll, overflowPkids...)
	chunkBytes := encodeChunk(t, testDim, 0, ops, pkidsAll,
		[][]float32{
			overflowVecs[:testDim],
			overflowVecs[testDim:],
		}, nil)
	eventChunks := []cuvscdc.EventChunk{{ChunkId: 0, Data: chunkBytes}}

	origStream := runSql_streaming
	runSql_streaming = func(ctx context.Context, sqlproc *sqlexec.SqlProcess, sql string, ch chan executor.Result, errChan chan error) (executor.Result, error) {
		ch <- executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc, tarPath)}}
		return executor.Result{}, nil
	}
	defer func() { runSql_streaming = origStream }()

	origRunSql := runSql
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "AND tag = 1"):
			return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeCdcChunkBatch(proc, eventChunks)}}, nil
		default:
			return executor.Result{
				Mp: proc.Mp(),
				Batches: []*batch.Batch{
					makeMetaBatch(proc, "cdc-deltas", built.Checksum, 0, built.FileSize),
				},
			}, nil
		}
	}
	defer func() { runSql = origRunSql }()

	models, err := LoadMetadata[float32, float32](sqlproc, tblcfg.DbName, tblcfg.MetadataTable)
	require.NoError(t, err)
	require.Equal(t, 1, len(models))

	idx := models[0]
	idx.Devices = []int{0}
	defer idx.Destroy()

	err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 1, true)
	require.NoError(t, err)
	require.NotNil(t, idx.Index)

	require.ElementsMatch(t, deletedPkids, idx.DeletedPkids)
	require.ElementsMatch(t, overflowPkids, idx.OverflowPkids)
	require.Equal(t, len(overflowPkids)*testDim, len(idx.OverflowVecs))

	// Querying the first vector — its pkid was deleted; cuvs's native bitset
	// prefilter should drop it from the result set.
	data := generateTestData(testNVectors, testDim)
	query := data[:testDim]
	keys, _, err := idx.SearchQuantize(query, 1, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(keys))
	require.NotEqual(t, ids[0], keys[0],
		"deleted pkid should not appear in search results (bitset prefilter must apply)")
}
