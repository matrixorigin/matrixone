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

package spillutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func makeTestKeyExpr() []*plan.Expr {
	return []*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_int32), Width: 32},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}}
}

func makeTestEvalKeysFn() func(*batch.Batch) ([]*vector.Vector, error) {
	return func(bat *batch.Batch) ([]*vector.Vector, error) {
		return bat.Vecs[:1], nil
	}
}

func makeInt32Batch(proc *process.Process, values []int32) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(len(values))
	return bat
}

func writeBuildFile(
	proc *process.Process,
	name string,
	bat *batch.Batch,
) *os.File {
	return writeBuildRecords(proc, name, bat)
}

func writeBuildRecords(
	proc *process.Process,
	name string,
	batches ...*batch.Batch,
) *os.File {
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		panic(err)
	}
	file, err := spillfs.CreateAndRemoveFile(context.Background(), name)
	if err != nil {
		panic(err)
	}
	for _, bat := range batches {
		if _, err := file.Write(marshalTestSpillRecord(bat)); err != nil {
			panic(err)
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}
	return file
}

func makeCorruptBatchFile(t *testing.T) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "corrupt-spill")
	require.NoError(t, err)
	rowCount, batchSize := int64(1), int64(1)
	var encoded bytes.Buffer
	encoded.Write(types.EncodeInt64(&rowCount))
	encoded.Write(types.EncodeInt64(&batchSize))
	encoded.WriteByte(0xff)
	_, err = file.Write(encoded.Bytes())
	require.NoError(t, err)
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	return file
}

func TestTakeSpillBuildPayloadRejectsWrongBudgetRef(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	_, _, err := TakeSpillBuildPayload(proc, nil)
	require.ErrorContains(t, err, message.ErrSpillBuildPayloadEmpty.Error())

	fd, err := os.CreateTemp(t.TempDir(), "wrong-budget-ref")
	require.NoError(t, err)
	_, err = fd.Write([]byte{1})
	require.NoError(t, err)
	_, err = fd.Seek(0, io.SeekStart)
	require.NoError(t, err)
	releases := 0
	file := message.NewSpillFile(fd, 1, 1, func() { releases++ })
	jm := message.NewJoinMap(
		message.GroupSels{}, nil, nil, nil, nil, proc.Mp(),
	)
	jm.SetRowCount(1)
	jm.IncRef(1)
	require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{
		Files:     []*message.SpillFile{file},
		BudgetRef: struct{}{},
	}))
	_, _, err = TakeSpillBuildPayload(proc, jm)
	require.ErrorContains(t, err, "missing its producer budget generation")
	require.Equal(t, 1, releases)
	jm.Free()
}

func TestTakeSpillBuildPayloadRejectsGlobalRowMismatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	fd, err := os.CreateTemp(t.TempDir(), "payload-row-mismatch")
	require.NoError(t, err)
	releases := 0
	jm := message.NewJoinMap(
		message.GroupSels{}, nil, nil, nil, nil, proc.Mp(),
	)
	jm.SetRowCount(2)
	jm.IncRef(1)
	require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{
		Files: []*message.SpillFile{
			message.NewSpillFile(fd, 1, 0, func() { releases++ }),
		},
		BudgetRef: generation,
	}))
	_, _, err = TakeSpillBuildPayload(proc, jm)
	require.ErrorContains(t, err, "row count")
	require.Equal(t, 1, releases)
	jm.Free()
}

func TestClassifyRowsConservesRows(t *testing.T) {
	hashes := make([]uint64, 257)
	for i := range hashes {
		hashes[i] = uint64(i%7) | (uint64(i&3) << 5)
	}
	rowIDs := make([]int32, len(hashes))
	counts := make([]int32, SpillNumBuckets)
	offsets := make([]int32, SpillNumBuckets+1)
	for _, shift := range []uint64{0, 5} {
		require.NoError(t, classifyRows(
			hashes,
			SpillNumBuckets,
			shift,
			rowIDs,
			counts,
			offsets,
		))
		require.Equal(t, int32(len(hashes)), offsets[SpillNumBuckets])
		seen := make([]bool, len(hashes))
		for _, rowID := range rowIDs {
			row := int(rowID)
			require.GreaterOrEqual(t, row, 0)
			require.Less(t, row, len(hashes))
			require.False(t, seen[row])
			seen[row] = true
		}
	}
	require.ErrorIs(t, classifyRows(
		[]uint64{0},
		SpillNumBuckets*2,
		0,
		make([]int32, 1),
		make([]int32, SpillNumBuckets*2),
		make([]int32, SpillNumBuckets*2+1),
	), process.ErrExecutionResourceInvalid)
}

func TestAccountedBucketReaderRoundTripAndCorruption(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(8<<20, 8<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	source := makeInt32Batch(proc, []int32{1, 2, 3})
	source.Vecs = append(source.Vecs, vector.NewVec(types.T_text.ToType()))
	for _, value := range []string{"raw", "text", "plain"} {
		require.NoError(t, vector.AppendBytes(source.Vecs[1], []byte(value), false, proc.Mp()))
	}
	require.NoError(t, source.Vecs[1].SetIsBinaryStringAt(0, true))
	require.NoError(t, source.Vecs[1].SetPrepareParamKindsWithMP([]vector.PrepareParamKind{
		vector.PrepareParamInteger, vector.PrepareParamNone, vector.PrepareParamFloat,
	}, proc.Mp()))
	defer source.Clean(proc.Mp())
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<20)
	require.NoError(t, err)
	account, err := registry.OpenWithController(8<<20, generation)
	require.NoError(t, err)
	allocation, err := NewSpillAllocationAccount(
		account,
		hashbuild.HashBuildAllocationOwner,
	)
	require.NoError(t, err)
	reader := &BucketReader{
		fd:         writeBuildFile(proc, t.Name(), source),
		allocation: allocation,
	}
	decoded, err := newSpillBatch(0, reader.allocation.decoded)
	require.NoError(t, err)
	got, err := reader.ReadBatch(proc, decoded)
	require.NoError(t, err)
	require.Equal(t, 3, got.RowCount())
	require.Equal(t, []int32{1, 2, 3}, vector.MustFixedColNoTypeCheck[int32](got.Vecs[0]))
	require.True(t, got.Vecs[1].GetIsBinaryStringAt(0))
	require.False(t, got.Vecs[1].GetIsBinaryStringAt(1))
	require.Equal(t, vector.PrepareParamInteger, got.Vecs[1].GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamFloat, got.Vecs[1].GetPrepareParamKindAt(2))
	got.Clean(proc.Mp())
	_, err = reader.ReadBatch(proc, decoded)
	require.ErrorIs(t, err, io.EOF)
	reader.Close()
	require.Zero(t, account.Snapshot().Used)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.Zero(t, generation.Used())

	corruptState := newTestSpillAllocationAccount(t, 8<<20, 16)
	corrupt := &BucketReader{
		fd:         makeCorruptBatchFile(t),
		allocation: corruptState.allocation,
	}
	bad, err := newSpillBatch(0, corruptState.allocation.decoded)
	require.NoError(t, err)
	_, err = corrupt.ReadBatch(proc, bad)
	require.Error(t, err)
	corrupt.Close()
	finalizeTestSpillAllocationAccount(t, corruptState)
}

func TestBucketReaderRejectsSchemaChangeBeforeMerge(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	textBatch := batch.NewWithSize(1)
	textBatch.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(
		textBatch.Vecs[0], []byte("x"), false, proc.Mp(),
	))
	textBatch.SetRowCount(1)
	defer textBatch.Clean(proc.Mp())
	intBatch := makeInt32Batch(proc, []int32{1})
	defer intBatch.Clean(proc.Mp())

	state := newTestSpillAllocationAccount(t, 8<<20, 16)
	reader := &BucketReader{
		fd: writeBuildRecords(
			proc,
			t.Name(),
			textBatch,
			intBatch,
		),
		mergeRecords: true,
		allocation:   state.allocation,
	}
	reuse, err := newSpillBatch(0, state.allocation.decoded)
	require.NoError(t, err)
	_, err = reader.ReadBatch(proc, reuse)
	require.ErrorContains(t, err, "spill batch schema changed")
	reader.Close()
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestSpillAllocationAccount(t, state)
}

func TestRebuildHashmapBasic(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	values := make([]int32, 100)
	for i := range values {
		values[i] = int32(i)
	}
	build := makeInt32Batch(proc, values)
	defer build.Clean(proc.Mp())
	engine := newExactTestSpillEngine(t, SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
	})
	initTestSpillFiles(engine, []*os.File{
		writeBuildFile(proc, t.Name(), build),
	}, int64(len(values)))
	jm, result, err := engine.RebuildHashmap(
		proc,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.NoError(t, err)
	require.Equal(t, BucketReady, result)
	require.Equal(t, int64(100), jm.GetRowCount())
	jm.Free()
	_, result, err = engine.RebuildHashmap(
		proc,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.NoError(t, err)
	require.Equal(t, BucketQueueEmpty, result)
	engine.Cleanup(proc)
}

func TestReSpillConservesBuildAndProbeRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	values := make([]int32, 5_000)
	for i := range values {
		values[i] = int32(i)
	}
	build := makeInt32Batch(proc, values)
	probe := makeInt32Batch(proc, values)
	defer build.Clean(proc.Mp())
	defer probe.Clean(proc.Mp())
	engine := newExactTestSpillEngine(t, SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		SpillThreshold:          500,
		NeedsBuildForEmptyProbe: true,
		NeedsProbeForEmptyBuild: true,
	})
	initTestSpillFiles(engine, []*os.File{
		writeBuildFile(proc, t.Name()+"-build", build),
	}, int64(len(values)))
	engine.buckets[0].ProbeFd = newTestSpillFile(
		writeBuildFile(proc, t.Name()+"-probe", probe),
		int64(len(values)),
	)
	engine.buckets[0].ProbeRows = int64(len(values))
	engine.probeKeyEval = makeTestEvalKeysFn()
	jm, result, err := engine.RebuildHashmap(
		proc,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.NoError(t, err)
	require.Nil(t, jm)
	require.Equal(t, BucketReSpilled, result)
	var buildRows, probeRows int64
	for _, bucket := range engine.buckets {
		buildRows += bucket.BuildRows
		probeRows += bucket.ProbeRows
	}
	require.Equal(t, int64(len(values)), buildRows)
	require.Equal(t, int64(len(values)), probeRows)
	engine.Cleanup(proc)
}

func TestSpillRejectsCompleteRecordTruncation(t *testing.T) {
	for _, test := range []struct {
		name      string
		threshold int64
	}{
		{name: "rebuild", threshold: 1 << 30},
		{name: "re-spill", threshold: 1},
	} {
		for _, metadataRows := range []int64{0, 6} {
			t.Run(fmt.Sprintf("%s/metadata-%d", test.name, metadataRows), func(t *testing.T) {
				proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
				defer proc.Free()
				first := makeInt32Batch(proc, []int32{1, 2, 3})
				defer first.Clean(proc.Mp())
				engine := newExactTestSpillEngine(t, SpillEngineConfig{
					BuildKeyExprs:           makeTestKeyExpr(),
					SpillThreshold:          test.threshold,
					NeedsBuildForEmptyProbe: true,
				})
				engine.InitFromSpilledFiles([]*message.SpillFile{
					newTestSpillFile(
						writeBuildFile(proc, fmt.Sprintf("truncate-%s-%d", test.name, metadataRows), first),
						metadataRows,
					),
				})
				_, _, err := engine.RebuildHashmap(
					proc,
					process.NewAnalyzer(0, false, false, "test"),
				)
				require.ErrorContains(t, err, "row count")
				engine.Cleanup(proc)
			})
		}
	}
}

func TestSpillRejectsPhysicalTruncationBeforeFirstRecord(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	baseline := proc.Mp().CurrNB()
	first := makeInt32Batch(proc, []int32{1})
	second := makeInt32Batch(proc, []int32{2})
	defer first.Clean(proc.Mp())
	defer second.Clean(proc.Mp())

	file := writeBuildRecords(proc, t.Name(), first, second)
	info, err := file.Stat()
	require.NoError(t, err)
	require.NoError(t, file.Truncate(int64(len(marshalTestSpillRecord(first)))))
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)

	engine := newExactTestSpillEngine(t, SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedsBuildForEmptyProbe: true,
	})
	engine.InitFromSpilledFiles([]*message.SpillFile{
		message.NewSpillFile(file, 2, uint64(info.Size()), nil),
	})
	jm, _, err := engine.RebuildHashmap(
		proc,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.Nil(t, jm)
	require.ErrorContains(t, err, "corrupted spill file size")
	engine.Cleanup(proc)
	require.Zero(t, engine.allocation.account.Snapshot().Used)
	require.Equal(t, baseline, proc.Mp().CurrNB())
}

func TestProbeRejectsCompleteRecordTruncation(t *testing.T) {
	for _, test := range []struct {
		name         string
		values       []int32
		metadataRows int64
		rebuildError bool
		firstError   bool
	}{
		{name: "zero metadata", values: []int32{1}, metadataRows: 0, rebuildError: true},
		{name: "row excess", values: []int32{1, 2}, metadataRows: 1, firstError: true},
		{name: "complete record truncation", values: []int32{1}, metadataRows: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			build := makeInt32Batch(proc, []int32{1})
			probe := makeInt32Batch(proc, test.values)
			defer build.Clean(proc.Mp())
			defer probe.Clean(proc.Mp())
			engine := newExactTestSpillEngine(t, SpillEngineConfig{
				BuildKeyExprs:           makeTestKeyExpr(),
				NeedsBuildForEmptyProbe: true,
			})
			engine.InitFromSpilledFiles([]*message.SpillFile{
				newTestSpillFile(writeBuildFile(proc, "probe-build", build), 1),
			})
			engine.buckets[0].ProbeFd = newTestSpillFile(
				writeBuildFile(proc, "probe-data", probe),
				test.metadataRows,
			)
			engine.buckets[0].ProbeRows = test.metadataRows
			jm, result, err := engine.RebuildHashmap(
				proc,
				process.NewAnalyzer(0, false, false, "test"),
			)
			if test.rebuildError {
				require.Nil(t, jm)
				require.ErrorContains(t, err, "row count")
				engine.Cleanup(proc)
				return
			}
			require.NoError(t, err)
			require.Equal(t, BucketReady, result)
			jm.Free()
			got, err := engine.NextProbeBatch(proc)
			if test.firstError {
				require.Nil(t, got)
				require.ErrorContains(t, err, "row count")
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, got.RowCount())
				_, err = engine.NextProbeBatch(proc)
				require.ErrorContains(t, err, "row count")
			}
			engine.Cleanup(proc)
		})
	}
}

func TestReSpillRejectsProbeRowMetadata(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	values := make([]int32, 100)
	for i := range values {
		values[i] = int32(i)
	}
	build := makeInt32Batch(proc, values)
	probe := makeInt32Batch(proc, []int32{1})
	defer build.Clean(proc.Mp())
	defer probe.Clean(proc.Mp())
	engine := newExactTestSpillEngine(t, SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		SpillThreshold:          1,
		NeedsBuildForEmptyProbe: true,
		NeedsProbeForEmptyBuild: true,
	})
	engine.InitFromSpilledFiles([]*message.SpillFile{
		newTestSpillFile(writeBuildFile(proc, t.Name()+"-build", build), int64(len(values))),
	})
	engine.buckets[0].ProbeFd = newTestSpillFile(
		writeBuildFile(proc, t.Name()+"-probe", probe),
		0,
	)
	engine.probeKeyEval = makeTestEvalKeysFn()
	_, _, err := engine.RebuildHashmap(
		proc,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.ErrorContains(t, err, "row count")
	engine.Cleanup(proc)
}

func TestRebuildRejectsRowsWithoutFile(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	for _, bucket := range []SpillBucket{
		{BuildRows: 1},
		{ProbeRows: 1},
	} {
		engine := newExactTestSpillEngine(t, SpillEngineConfig{
			BuildKeyExprs: makeTestKeyExpr(),
		})
		engine.buckets = []SpillBucket{bucket}
		_, _, err := engine.RebuildHashmap(
			proc,
			process.NewAnalyzer(0, false, false, "test"),
		)
		require.ErrorContains(t, err, "file/row metadata")
		engine.Cleanup(proc)
	}
}

func TestReSpillOmitsUnusedBatchMetadata(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	values := make([]int32, 100)
	for i := range values {
		values[i] = int32(i)
	}
	build := makeInt32Batch(proc, values)
	build.Attrs = []string{"key"}
	defer build.Clean(proc.Mp())
	engine := newExactTestSpillEngine(t, SpillEngineConfig{
		BuildKeyExprs:           makeTestKeyExpr(),
		NeedBatches:             true,
		SpillThreshold:          1,
		NeedsBuildForEmptyProbe: true,
	})
	engine.InitFromSpilledFiles([]*message.SpillFile{
		newTestSpillFile(writeBuildFile(proc, t.Name(), build), int64(len(values))),
	})
	engine.buckets[0].Depth = SpillMaxPass - 1
	analyzer := process.NewAnalyzer(0, false, false, "test")
	_, result, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, BucketReSpilled, result)

	found := false
	for len(engine.buckets) > 0 {
		jm, next, err := engine.RebuildHashmap(proc, analyzer)
		require.NoError(t, err)
		if next != BucketReady {
			continue
		}
		found = true
		for _, bat := range jm.GetBatches() {
			require.Empty(t, bat.Attrs)
		}
		jm.Free()
	}
	require.True(t, found)
	engine.Cleanup(proc)
}

func TestCleanupDoubleSafe(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	engine := newExactTestSpillEngine(t, SpillEngineConfig{
		BuildKeyExprs: makeTestKeyExpr(),
	})
	initTestSpillFiles(engine, []*os.File{nil, nil, nil}, 0, 0, 0)
	engine.Cleanup(proc)
	engine.Cleanup(proc)
	require.Nil(t, engine.buckets)
}
