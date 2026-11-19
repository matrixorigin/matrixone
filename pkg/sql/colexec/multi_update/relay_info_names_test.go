// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multi_update

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// issue25590MakeStatsRows builds the production tombstone metadata
// batch ([ObjectMeta_ObjectStats], one serialized ObjectStats row per
// persisted tombstone object) that fillDeleteBlockInfo clones.
func issue25590MakeStatsRows(
	t *testing.T,
	proc *process.Process,
	n int,
) (*batch.Batch, []string) {
	t.Helper()
	inner := colexec.AllocCNS3ResultBat(true)
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rowid := types.RandomRowid()
		oid := rowid.BorrowObjectID()
		stats := objectio.NewObjectStatsWithObjectID(oid, false, false, true)
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			proc.Mp(), true, inner, true, *stats))
		names = append(names, stats.ObjectName().String())
	}
	inner.SetRowCount(inner.Vecs[0].Length())
	return inner, names
}

// issue25590MakeInsertRows builds the production insert metadata batch
// ([BlockMeta_BlockInfo, ObjectMeta_ObjectStats], one row per persisted
// insert object) that FillBlockInfoBat emits.
func issue25590MakeInsertRows(
	t *testing.T,
	proc *process.Process,
	n int,
) (*batch.Batch, []string) {
	t.Helper()
	inner := colexec.AllocCNS3ResultBat(false)
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rowid := types.RandomRowid()
		oid := rowid.BorrowObjectID()
		stats := objectio.NewObjectStatsWithObjectID(oid, false, false, true)
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			proc.Mp(), false, inner, true, *stats))
		names = append(names, stats.ObjectName().String())
	}
	inner.SetRowCount(inner.Vecs[1].Length())
	return inner, names
}

type issue25590RelayRow struct {
	action   actionType
	tableID  uint64
	rowCount uint64
	name     string
	origin   *batch.Batch
}

// issue25590MakeRelayBatch mirrors the fragment writer's own output
// construction (addBatchToOutput over makeS3OutputBatch): every row
// packs [action, tableID, rowCount, name, serializedOriginBatch].
func issue25590MakeRelayBatch(
	t *testing.T,
	proc *process.Process,
	rows []issue25590RelayRow,
) *batch.Batch {
	t.Helper()
	out := makeS3OutputBatch()
	for _, r := range rows {
		require.NoError(t, vector.AppendFixed(
			out.Vecs[0], uint8(r.action), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(
			out.Vecs[1], r.tableID, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(
			out.Vecs[2], r.rowCount, false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(
			out.Vecs[3], []byte(r.name), false, proc.Mp()))
		var val []byte
		if r.origin != nil {
			var buf bytes.Buffer
			var err error
			val, err = r.origin.MarshalBinaryWithBuffer(&buf, true)
			require.NoError(t, err)
		}
		require.NoError(t, vector.AppendBytes(
			out.Vecs[4], val, false, proc.Mp()))
	}
	out.SetRowCount(out.Vecs[0].Length())
	return out
}

// TestIssue25590RelayS3InfoNameExtraction pins the r23 intake
// recognizer: names derive from the serialized tails alone (every
// ObjectStats row of the tail — never the routing-key column),
// row-only deletes own nothing, undecodable or tail-less metadata
// rejects the ack, and everything outside the relay layout is
// ordinary data. The wire leg pins the shape AFTER the pipeline codec
// round trip, which is the shape the client decodes.
func TestIssue25590RelayS3InfoNameExtraction(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	insertInner, insertWant := issue25590MakeInsertRows(t, proc, 3)
	defer insertInner.Clean(mp)
	deleteInner, deleteWant := issue25590MakeStatsRows(t, proc, 1)
	defer deleteInner.Clean(mp)
	rowOnlyInner, _ := issue25590MakeStatsRows(t, proc, 1)
	defer rowOnlyInner.Clean(mp)

	bat := issue25590MakeRelayBatch(t, proc, []issue25590RelayRow{
		{actionAffectedRows, 42, 3, "", nil},
		// S3 tombstone delete: the name column carries the writer's
		// raw-object-id+|type flush key; the object's own name rides
		// in the serialized stats tail.
		{actionDelete, 42, 2, "deadbeef|0", deleteInner},
		{actionInsert, 42, 5, "", insertInner},
		// Row-only delete: the PhysicalAddr marker owns nothing.
		{actionDelete, 42, 1, objectio.PhysicalAddr_Attr, rowOnlyInner},
	})
	defer bat.CleanOnlyData()

	want := append(append([]string{}, deleteWant...), insertWant...)
	names, ok, err := TryExtractRelayS3InfoObjectNames(bat, mp)
	require.NoError(t, err)
	require.True(t, ok, "the relay layout adopts")
	require.Equal(t, want, names,
		"adoption derives the tombstone object's own name from the append tail, every insert tail object, and excludes row-only deletes")

	// The wire shape: the production server encodes batches with
	// MarshalBinaryForPipeline and the client decodes them with
	// UnmarshalBinaryForPipeline before this recognizer runs.
	var transport bytes.Buffer
	data, err := bat.MarshalBinaryForPipeline(&transport, false, true)
	require.NoError(t, err)
	wire := batch.NewOffHeapEmpty()
	require.NoError(t, wire.UnmarshalBinaryForPipeline(data, mp))
	defer wire.CleanOnlyData()
	wireNames, ok, err := TryExtractRelayS3InfoObjectNames(wire, mp)
	require.NoError(t, err)
	require.True(t, ok, "the relay layout survives the pipeline codec")
	require.Equal(t, names, wireNames,
		"adoption derives the same names from the batch the client decodes")

	// F2 counterexample: an insert row accompanying a DELETE row whose
	// tail lost the object-stats column cannot prove its objects, so
	// the whole ack rejects fail-closed instead of adopting a subset.
	typeless := batch.NewWithSize(1)
	typeless.SetAttributes([]string{"not-a-stats-column"})
	typeless.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer typeless.Clean(mp)
	require.NoError(t, vector.AppendFixed(typeless.Vecs[0], int64(1), false, mp))
	typeless.SetRowCount(1)
	partial := issue25590MakeRelayBatch(t, proc, []issue25590RelayRow{
		{actionDelete, 42, 1, "deadbeef|0", deleteInner},
		{actionInsert, 42, 1, "", typeless},
	})
	defer partial.CleanOnlyData()
	_, _, err = TryExtractRelayS3InfoObjectNames(partial, mp)
	require.Error(t, err,
		"a relay tail without its object-stats column rejects the ack - the fragment keeps its local owner")

	// F2 counterexample: undecodable ownership metadata rejects the ack.
	broken := issue25590MakeRelayBatch(t, proc, []issue25590RelayRow{
		{actionDelete, 42, 1, "deadbeef|0", deleteInner},
		{actionInsert, 42, 1, "", nil},
	})
	defer broken.CleanOnlyData()
	require.NoError(t, vector.AppendBytes(
		broken.Vecs[4], []byte("not-a-serialized-batch"), false, mp))
	broken.SetRowCount(broken.Vecs[0].Length())
	_, _, err = TryExtractRelayS3InfoObjectNames(broken, mp)
	require.Error(t, err,
		"undecodable ownership metadata is rejected before the ack - never acknowledged on a subset")

	// F1 counterexample: an ordinary five-column batch whose 1st column
	// value falls outside the transport action enum is ordinary data —
	// the recognizer never interprets it. Rows claimed as transport
	// classes without provable tails (below) reject the ack instead.
	ordinary := issue25590MakeRelayBatch(t, proc, []issue25590RelayRow{
		{actionType(9), 42, 1, "committed_object_name", nil},
	})
	defer ordinary.CleanOnlyData()
	_, ok, err = TryExtractRelayS3InfoObjectNames(ordinary, mp)
	require.NoError(t, err)
	require.False(t, ok,
		"ordinary rows whose first column is not a transport action are ordinary data, whatever the other columns name")

	// F1 counterexample, fail-closed: an actor-crafted row claiming a
	// transport class with no provable tail rejects the ack — the
	// scalar columns alone never admit ownership metadata.
	claimed := issue25590MakeRelayBatch(t, proc, []issue25590RelayRow{
		{actionDelete, 42, 1, "committed_object_name", nil},
	})
	defer claimed.CleanOnlyData()
	_, _, err = TryExtractRelayS3InfoObjectNames(claimed, mp)
	require.Error(t, err,
		"a row claiming a transport class without a serialized tail rejects the ack fail-closed")
	// A non-transport action rejects the whole batch.
	actionUpdateRow := issue25590MakeRelayBatch(t, proc, []issue25590RelayRow{
		{actionUpdate, 42, 5, "some-name", insertInner},
	})
	defer actionUpdateRow.CleanOnlyData()
	_, ok, _ = TryExtractRelayS3InfoObjectNames(actionUpdateRow, mp)
	require.False(t, ok, "actionUpdate is a non-transport class - the whole batch rejects")

	// An ordinary two-column data batch never matches the layout.
	dataBatch := batch.NewWithSize(2)
	dataBatch.SetAttributes([]string{"pk", "v"})
	dataBatch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	dataBatch.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	defer dataBatch.Clean(mp)
	_, ok, _ = TryExtractRelayS3InfoObjectNames(dataBatch, mp)
	require.False(t, ok, "an ordinary data batch does not adopt names")

	// Control rows carry no object but keep the relay class.
	emptyNames := issue25590MakeRelayBatch(t, proc, []issue25590RelayRow{
		{actionAffectedRows, 42, 8, "", nil},
	})
	defer emptyNames.CleanOnlyData()
	got, ok, err := TryExtractRelayS3InfoObjectNames(emptyNames, mp)
	require.NoError(t, err)
	require.True(t, ok, "the layout is a relay batch even with no objects")
	require.Empty(t, got, "control rows carry no object name")
}
