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
	"reflect"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// byteSpan is the backing-region of one string or byte slice, half-open.
type byteSpan struct{ base, limit uintptr }

// The address windows below deliberately avoid importing unsafe: molint's
// checkunsafe gate keeps that import out of this file, while
// reflect.Value.UnsafePointer exposes the same backing-store addresses the
// aliasing assertions below compare.
func spanOfBytes(b []byte) byteSpan {
	if len(b) == 0 {
		return byteSpan{}
	}
	base := uintptr(reflect.ValueOf(b).UnsafePointer())
	return byteSpan{base: base, limit: base + uintptr(len(b))}
}

func spanOfString(s string) byteSpan {
	if len(s) == 0 {
		return byteSpan{}
	}
	base := uintptr(reflect.ValueOf(s).UnsafePointer())
	return byteSpan{base: base, limit: base + uintptr(len(s))}
}

func (s byteSpan) empty() bool { return s.limit <= s.base }

func (s byteSpan) contains(o byteSpan) bool {
	return !o.empty() && !s.empty() && s.base <= o.base && o.limit <= s.limit
}

func (s byteSpan) overlaps(o byteSpan) bool {
	return !s.empty() && !o.empty() && s.base < o.limit && o.base < s.limit
}

// vectorRegions returns every backing region a varlena vector's payload may
// live in: long payloads occupy the shared area, short ones stay inline in
// the varlena headers inside data.
func vectorRegions(v *vector.Vector) []byteSpan {
	regions := make([]byteSpan, 0, 2)
	if sp := spanOfBytes(v.GetData()); !sp.empty() {
		regions = append(regions, sp)
	}
	if sp := spanOfBytes(v.GetArea()); !sp.empty() {
		regions = append(regions, sp)
	}
	return regions
}

func containedInAny(sp byteSpan, regions []byteSpan) bool {
	for _, r := range regions {
		if r.contains(sp) {
			return true
		}
	}
	return false
}

func overlapsAny(sp byteSpan, regions []byteSpan) bool {
	for _, r := range regions {
		if r.overlaps(sp) {
			return true
		}
	}
	return false
}

// pathRecordingFS wraps a real fileservice and records the backing region
// of every FilePath the Read path is asked to serve. The caching layers
// hash and retain strings derived from that path for each cached entry's
// whole lifetime, so a path aliasing a transient batch's memory is exactly
// the shape that let a cache key outlive freed vector storage.
type pathRecordingFS struct {
	fileservice.FileService
	mu    sync.Mutex
	spans []byteSpan
}

func (r *pathRecordingFS) record(path string) {
	if path == "" {
		return
	}
	r.mu.Lock()
	r.spans = append(r.spans, spanOfString(path))
	r.mu.Unlock()
}

func (r *pathRecordingFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if v != nil {
		r.record(v.FilePath)
	}
	return r.FileService.Read(ctx, v)
}

func (r *pathRecordingFS) ReadCache(ctx context.Context, v *fileservice.IOVector) error {
	if v != nil {
		r.record(v.FilePath)
	}
	return r.FileService.ReadCache(ctx, v)
}

func (r *pathRecordingFS) recorded() []byteSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byteSpan(nil), r.spans...)
}

// spillOneLinkObjectForOwnership crafts one spilled pk-move link object
// through the real sinker and returns the writer plus the ferried
// block-info batch, on a fresh disk-backed fileservice with the full
// memory/disk caching stack (as in prepareTestCtx).
func spillOneLinkObjectForOwnership(
	t *testing.T,
) (*process.Process, *s3WriterDelegate, *MultiUpdate, *MultiUpdateCtx) {
	t.Helper()
	_, ctrl, proc := prepareTestCtx(t, true)
	t.Cleanup(ctrl.Finish)
	t.Cleanup(proc.Free)

	prevBudget := pkMoveLinkMemBudget
	pkMoveLinkMemBudget = 1
	t.Cleanup(func() { pkMoveLinkMemBudget = prevBudget })

	mainCtx := prepareTestUpdateMultiUpdateCtx(false, false)[0]
	linkCtx := newTestPkMoveLinkCtx(0)
	update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
	update.resetMultiUpdateCtxs()
	update.addAffectedRowsFunc = update.doAddAffectedRows

	writer, err := newS3Writer(proc.GetService(), update)
	require.NoError(t, err)

	ownerCtx := &MultiUpdateCtx{
		TableDef: &plan.TableDef{
			TblId: 42,
			Name:  "t",
			Cols: []*plan.ColDef{
				{Name: "a", Typ: i32typ, Primary: true},
				{Name: "b", Typ: varcharTyp},
				{Name: catalog.Row_ID, Typ: rowIdTyp},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
		},
		InsertCols: []int{0, 1},
		DeleteCols: []int{2, 3},
	}
	rids := []types.Rowid{types.BuildTestRowid(1, 1), types.BuildTestRowid(1, 2)}
	cb := batch.NewWithSize(4)
	cb.Vecs[0] = testutil.NewInt32Vector(2, types.T_int32.ToType(), proc.Mp(), false, nil, []int32{2, 4})
	cb.Vecs[1] = testutil.MakeVarcharVector([]string{"x", "y"}, nil, proc.Mp())
	cb.Vecs[2] = testutil.MakeRowIdVector(rids, nil, proc.Mp())
	cb.Vecs[3] = testutil.NewInt32Vector(2, types.T_int32.ToType(), proc.Mp(), false, nil, []int32{3, 5})
	cb.SetRowCount(2)
	out, err := collectUpdatePkMoveLinkRows(
		proc, linkCtx, ownerCtx, cb, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	cb.Clean(proc.Mp())
	require.NoError(t, err)

	writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
		pkMoveLinkBuf{idx: 1, bat: out, rows: uint64(out.RowCount())})
	writer.pkMoveLinkBytes = int64(out.Size())
	analyzer := process.NewAnalyzer(0, false, false, "pk-move-link-ownership-test")
	require.NoError(t, writer.spillPkMoveLinkBufs(proc, analyzer))
	require.Len(t, writer.pkMoveLinkBufs, 1)
	require.NotNil(t, writer.pkMoveLinkBufs[0].spilled)
	// The writer owns the spill ledger and every buffered batch; free is the
	// terminal owner action that releases both.
	t.Cleanup(func() { _ = writer.free(proc) })
	return proc, writer, update, linkCtx
}

// TestSpilledPkMoveLinkLoadOwnsObjectLocations pins the CI crash's defect
// class: loadSpilledPkMoveLinkBatch decodes block infos out of the ferried
// batch and used to hand those aliasing bytes straight into the meta load.
// The meta extent read then cached under keys derived from that aliased
// memory for the entry's lifetime, one consumer-batch free away from a
// use-after-free hash inside fifocache eviction. Every path the read-back
// asks the fileservice to serve must own its bytes, disjoint from the
// ferried batch's vector storage.
func TestSpilledPkMoveLinkLoadOwnsObjectLocations(t *testing.T) {
	proc, writer, update, linkCtx := spillOneLinkObjectForOwnership(t)
	spilled := writer.pkMoveLinkBufs[0].spilled
	blkVec := spilled.Vecs[0]
	regions := vectorRegions(blkVec)
	require.NotEmpty(t, regions, "the ferried block-info vector must expose storage")

	// Harness discrimination proof: the decoded meta location genuinely
	// aliases the ferried vector's storage, so a disjointness failure
	// below cannot be vacuous.
	probe := objectio.DecodeBlockInfo(blkVec.GetBytesAt(0))
	probeName := spanOfBytes(probe.MetaLocation().Name())
	require.True(t, containedInAny(probeName, regions),
		"the decoded object-name region must alias the ferried vector storage")

	rec := &pathRecordingFS{FileService: proc.Base.FileService}
	proc.Base.FileService = rec

	back, err := update.loadSpilledPkMoveLinkBatch(proc, spilled, linkCtx)
	require.NoError(t, err)
	require.Greater(t, back.RowCount(), 0)
	back.Clean(proc.Mp())

	spans := rec.recorded()
	require.NotEmpty(t, spans, "the read-back must serve real file reads")
	for _, sp := range spans {
		require.False(t, overlapsAny(sp, regions),
			"a read path aliases the ferried batch's vector memory [0x%x,0x%x); "+
				"cache keys derived from it outlive the batch", sp.base, sp.limit)
	}
}

// TestResetMergeBlockForOldCNOwnsObjectLocations pins the same defect class
// in the old-CN merge-block repair: the incoming batch's block-info column
// is pipeline memory that its scope releases, while the meta load that
// back-fills object stats caches keys derived from the object-name bytes
// for the entry's whole lifetime. The repair must decode from an owned
// copy, so no read path aliases the incoming vector's storage.
func TestResetMergeBlockForOldCNOwnsObjectLocations(t *testing.T) {
	proc, writer, _, _ := spillOneLinkObjectForOwnership(t)
	spilled := writer.pkMoveLinkBufs[0].spilled

	// One real block-info row, re-minted into a minimal old-CN merge batch:
	// a single block-info column, no trailing object-stats column.
	mb := batch.NewWithSize(1)
	mb.SetAttributes([]string{catalog.BlockMeta_BlockInfo})
	mb.Vecs[0] = vector.NewVec(types.T_binary.ToType())
	require.NoError(t, vector.AppendBytes(
		mb.Vecs[0], spilled.Vecs[0].GetBytesAt(0), false, proc.Mp()))
	mb.SetRowCount(1)
	defer mb.Clean(proc.Mp())

	regions := vectorRegions(mb.Vecs[0])
	require.NotEmpty(t, regions, "the merge batch's block-info vector must expose storage")
	probe := objectio.DecodeBlockInfo(mb.Vecs[0].GetBytesAt(0))
	require.True(t, containedInAny(spanOfBytes(probe.MetaLocation().Name()), regions),
		"the decoded object-name region must alias the merge batch's vector storage")

	rec := &pathRecordingFS{FileService: proc.Base.FileService}
	proc.Base.FileService = rec
	require.NoError(t, resetMergeBlockForOldCN(proc, mb))

	spans := rec.recorded()
	require.NotEmpty(t, spans, "the repair must serve real meta reads")
	for _, sp := range spans {
		require.False(t, overlapsAny(sp, regions),
			"a meta-load path aliases the merge batch's vector memory [0x%x,0x%x); "+
				"cache keys derived from it outlive the batch", sp.base, sp.limit)
	}
	// Sanity of the repair itself: the stats column arrived, carrying one
	// entry per block-info row plus the protocol's trailing padding row.
	require.Equal(t, catalog.ObjectMeta_ObjectStats, mb.Attrs[len(mb.Attrs)-1])
	require.Equal(t, mb.RowCount()+1, mb.Vecs[len(mb.Vecs)-1].Length())
}
