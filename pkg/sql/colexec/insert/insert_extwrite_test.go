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

package insert

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// mockExtWriter records ExternalWriter lifecycle calls.
type mockExtWriter struct {
	writes     int
	closes     int
	aborts     int
	rows       uint64
	failure    error
	writeBytes int64
	closeBytes int64
	// onWrite, when set, observes each batch inside its synchronous
	// WriteBatch window - exactly the time the external IGNORE fill's
	// per-batch adaptations are installed for.
	onWrite func(*batch.Batch)
}

func (m *mockExtWriter) WriteBatch(ctx context.Context, bat *batch.Batch, analyzer process.Analyzer) error {
	m.writes++
	if m.onWrite != nil {
		m.onWrite(bat)
	}
	err := process.MeasureFilesystemWaitErr(analyzer, func() error {
		if m.writeBytes > 0 {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.Put.Add(1)
				counter.FileService.S3WriteSize.Add(m.writeBytes)
			})
		}
		return m.failure
	})
	if err != nil {
		return err
	}
	m.rows += uint64(bat.RowCount())
	return nil
}

func (m *mockExtWriter) Close(ctx context.Context) (uint64, error) {
	m.closes++
	if m.closeBytes > 0 {
		perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
			counter.FileService.S3.Put.Add(1)
			counter.FileService.S3WriteSize.Add(m.closeBytes)
		})
	}
	return m.rows, m.failure
}

func (m *mockExtWriter) Abort(ctx context.Context) {
	m.aborts++
}

func notNullCol(name string) *plan.ColDef {
	return &plan.ColDef{
		Name:    name,
		Default: &plan.Default{NullAbility: false},
	}
}

func nullableCol(name string) *plan.ColDef {
	return &plan.ColDef{
		Name:    name,
		Default: &plan.Default{NullAbility: true},
	}
}

// TestCheckExternalNotNull covers the NOT NULL matrix: nulls bitmap, const-null
// vectors, the explicit NotNull flag without a Default, and skipped entries.
func TestCheckExternalNotNull(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := mpool.MustNewZero()

	newInsert := func(cols []*plan.ColDef, attrs []string) *Insert {
		ins := NewArgument()
		ins.ToExternal = true
		ins.InsertCtx = &InsertCtx{Attrs: attrs}
		ins.ctr.extCols = cols
		return ins
	}

	intVec := func(v int64, isNull bool) *vector.Vector {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](vec, v, isNull, mp))
		return vec
	}

	// non-null value through a NOT NULL column: ok
	ins := newInsert([]*plan.ColDef{notNullCol("a")}, []string{"a"})
	defer ins.Release()
	bat := batch.New([]string{"a"})
	bat.Vecs[0] = intVec(7, false)
	bat.SetRowCount(1)
	defer bat.Clean(mp)
	require.NoError(t, ins.checkExternalNotNull(proc, bat))

	// NULL in the nulls bitmap: rejected
	bat2 := batch.New([]string{"a"})
	bat2.Vecs[0] = intVec(0, true)
	bat2.SetRowCount(1)
	defer bat2.Clean(mp)
	err := ins.checkExternalNotNull(proc, bat2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be null")

	// const-null vector: rejected (no bitmap entry to find)
	cn := vector.NewConstNull(types.T_int64.ToType(), 3, mp)
	bat3 := batch.New([]string{"a"})
	bat3.Vecs[0] = cn
	bat3.SetRowCount(3)
	defer bat3.Clean(mp)
	require.Error(t, ins.checkExternalNotNull(proc, bat3))

	// the explicit NotNull flag alone (no Default descriptor) is honored
	flagOnly := &plan.ColDef{Name: "a", NotNull: true}
	ins2 := newInsert([]*plan.ColDef{flagOnly}, []string{"a"})
	defer ins2.Release()
	bat4 := batch.New([]string{"a"})
	bat4.Vecs[0] = intVec(0, true)
	bat4.SetRowCount(1)
	defer bat4.Clean(mp)
	require.Error(t, ins2.checkExternalNotNull(proc, bat4))

	// nullable column and nil extCols entries pass NULLs through
	ins3 := newInsert([]*plan.ColDef{nullableCol("a"), nil}, []string{"a", "b"})
	defer ins3.Release()
	bat5 := batch.New([]string{"a", "b"})
	bat5.Vecs[0] = intVec(0, true)
	bat5.Vecs[1] = intVec(0, true)
	bat5.SetRowCount(1)
	defer bat5.Clean(mp)
	require.NoError(t, ins3.checkExternalNotNull(proc, bat5))
}

// extIgnoreWarningSession collects flushed warning batches for the IGNORE leg.
type extIgnoreWarningSession struct {
	total    uint64
	codes    []uint16
	messages []string
}

func (*extIgnoreWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*extIgnoreWarningSession) AddTempTable(string, string, string)        {}
func (*extIgnoreWarningSession) RemoveTempTable(string, string)             {}
func (*extIgnoreWarningSession) RemoveTempTableByRealName(string)           {}
func (*extIgnoreWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool) {
	return false, false
}

func (s *extIgnoreWarningSession) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	s.total += total
	for i := 0; i < len(codes) && i < len(messages); i++ {
		s.codes = append(s.codes, codes[i])
		s.messages = append(s.messages, messages[i])
	}
}

// TestCheckExternalNotNullIgnoreAdjustsInsteadOfRejecting pins the writable
// external-table gate: under statement IGNORE the same NULL cell that strict
// mode rejects is adjusted to the implicit default with one warning and the
// row is kept.
func TestCheckExternalNotNullIgnoreAdjustsInsteadOfRejecting(t *testing.T) {
	proc := testutil.NewProc(t)
	session := &extIgnoreWarningSession{}
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	defer proc.Free()
	mp := mpool.MustNewZero()

	ins := NewArgument()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{Attrs: []string{"a"}}
	ins.ctr.extCols = []*plan.ColDef{notNullCol("a")}
	defer ins.Release()

	vec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](vec, 7, false, mp))
	require.NoError(t, vector.AppendFixed[int64](vec, 0, true, mp))
	bat := batch.New([]string{"a"})
	bat.Vecs[0] = vec
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	require.NoError(t, ins.checkExternalNotNull(proc, bat))
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](vec, 0))
	require.False(t, vec.GetNulls().Contains(1))
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](vec, 1),
		"the NULL cell was adjusted to the implicit default")
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR}, session.codes)
	require.Equal(t, []string{"Column 'a' cannot be null"}, session.messages)
}

// TestCheckExternalNotNullIgnoreFillsConstNullVector extends the strict
// const-null red leg with IGNORE coverage, mirroring the LOAD fact of the
// review: a writable external-table load omits a NOT NULL column whose folded
// default is a constant NULL (e.g. DEFAULT (abs(null))). The projection
// delivers a NewConstNull broadcast with no backing storage; under statement
// IGNORE the gate must still fill every logical row of the implicit default
// with one warning per row instead of failing the load on the mutation.
func TestCheckExternalNotNullIgnoreFillsConstNullVector(t *testing.T) {
	proc := testutil.NewProc(t)
	session := &extIgnoreWarningSession{}
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	defer proc.Free()
	mp := mpool.MustNewZero()

	ins := NewArgument()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{Attrs: []string{"a"}}
	ins.ctr.extCols = []*plan.ColDef{notNullCol("a")}
	defer ins.Release()

	cn := vector.NewConstNull(types.T_int64.ToType(), 3, mp)
	bat := batch.New([]string{"a"})
	bat.Vecs[0] = cn
	bat.SetRowCount(3)
	defer bat.Clean(mp)

	require.NoError(t, ins.checkExternalNotNull(proc, bat))
	filled := bat.Vecs[0]
	require.NotSame(t, cn, filled,
		"a constant NULL has no backing storage and is replaced by the fill")
	require.True(t, filled.IsConst(), "the replacement keeps the broadcast")
	require.Equal(t, 3, filled.Length(), "logical length is preserved")
	require.False(t, filled.IsConstNull())
	require.False(t, filled.GetNulls().Contains(0))
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](filled, 0),
		"the broadcast row carries the implicit default")
	require.Equal(t, uint64(3), session.total,
		"one warning per adjusted logical row")
	require.Len(t, session.messages, 3)
	for _, msg := range session.messages {
		require.Equal(t, "Column 'a' cannot be null", msg)
	}
}

// TestExternalResetAborts: a writer still alive at Reset/Free means the stream
// did not end cleanly — the file must be aborted, never finalized.
func TestExternalResetAborts(t *testing.T) {
	proc := testutil.NewProc(t)

	ins := NewArgument()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{}
	ins.OpAnalyzer = process.NewAnalyzer(0, false, false, "external-insert")
	ins.ctr.extCounter = new(perfcounter.CounterSet)
	ins.ctr.extCounter.FileService.S3.Put.Add(1)
	ins.ctr.extCounter.FileService.S3WriteSize.Add(9)
	mock := &mockExtWriter{}
	ins.ctr.extWriter = mock

	ins.Reset(proc, true, nil)
	require.Equal(t, 1, mock.aborts)
	require.Equal(t, 0, mock.closes)
	require.Nil(t, ins.ctr.extWriter)
	require.Nil(t, ins.ctr.extCounter)
	delta := ins.OpAnalyzer.GetOpStats().ResourceDelta()
	require.Equal(t, uint64(1), delta.Usage.S3Requests[resource.S3Put])
	require.Equal(t, uint64(9), delta.Usage.S3WriteBytes)
	require.Positive(t, delta.Usage.WaitNS[resource.WaitFilesystem])

	// Free with a live writer aborts too.
	mock2 := &mockExtWriter{}
	ins.ctr.extWriter = mock2
	ins.Free(proc, true, nil)
	require.Equal(t, 1, mock2.aborts)
	require.Equal(t, 0, mock2.closes)

	ins.Release()
}

func TestExternalWriterOwnsAsyncCountersAndWait(t *testing.T) {
	proc := testutil.NewProc(t)
	bat := batch.NewWithSize(0)
	bat.SetRowCount(1)

	ins := NewArgument()
	defer ins.Release()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{AddAffectedRows: true}
	ins.OpAnalyzer = process.NewAnalyzer(0, false, false, "external-insert")
	ins.ctr.extCounter = new(perfcounter.CounterSet)
	writer := &mockExtWriter{writeBytes: 10, closeBytes: 5}
	ins.ctr.extWriter = writer
	ins.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat, nil}))

	for range 2 {
		ins.OpAnalyzer.Start()
		_, err := ins.Call(proc)
		ins.OpAnalyzer.Stop()
		require.NoError(t, err)
	}

	delta := ins.OpAnalyzer.GetOpStats().ResourceDelta()
	require.Equal(t, uint64(2), delta.Usage.S3Requests[resource.S3Put])
	require.Equal(t, uint64(15), delta.Usage.S3WriteBytes)
	require.Positive(t, delta.Usage.WaitNS[resource.WaitFilesystem])
	require.Nil(t, ins.ctr.extWriter)
	require.Nil(t, ins.ctr.extCounter)
}

type externalYieldInput struct {
	*colexec.MockOperator
	results []vm.CallResult
	next    int
}

func (input *externalYieldInput) Call(*process.Process) (vm.CallResult, error) {
	result := input.results[input.next]
	input.next++
	return result, nil
}

func TestExternalInsertPreservesWriterAcrossYield(t *testing.T) {
	for _, outcome := range []string{"eof", "cancel", "write_failure"} {
		t.Run(outcome, func(t *testing.T) {
			proc := testutil.NewProc(t)
			bat := batch.NewWithSize(0)
			bat.SetRowCount(1)
			writer := &mockExtWriter{}
			ins := NewArgument()
			defer ins.Release()
			ins.ToExternal = true
			ins.InsertCtx = &InsertCtx{AddAffectedRows: true}
			ins.OpAnalyzer = process.NewAnalyzer(0, false, false, "external-insert")
			ins.ctr.extWriter = writer
			ins.ctr.extCounter = new(perfcounter.CounterSet)
			ins.AppendChild(&externalYieldInput{MockOperator: colexec.NewMockOperator(), results: []vm.CallResult{
				{Status: vm.ExecYield}, {Batch: bat}, {Status: vm.ExecYield}, {Status: vm.ExecStop},
			}})
			call := func() (vm.CallResult, error) {
				ins.OpAnalyzer.Start()
				defer ins.OpAnalyzer.Stop()
				return ins.Call(proc)
			}
			result, err := call()
			require.NoError(t, err)
			require.Equal(t, vm.ExecYield, result.Status)
			require.Same(t, writer, ins.ctr.extWriter)
			require.Zero(t, writer.closes)
			require.Zero(t, writer.aborts)
			if outcome == "cancel" {
				ins.Reset(proc, true, context.Canceled)
			} else {
				if outcome == "write_failure" {
					writer.failure = errors.New("write failed")
				}
				_, err = call()
				if outcome == "write_failure" {
					require.ErrorIs(t, err, writer.failure)
					ins.Reset(proc, true, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, uint64(1), writer.rows)
					require.Equal(t, uint64(1), ins.ctr.affectedRows)
					result, err = call()
					require.NoError(t, err)
					require.Equal(t, vm.ExecYield, result.Status)
					require.Same(t, writer, ins.ctr.extWriter)
					require.Zero(t, writer.closes)
					_, err = call()
					require.NoError(t, err)
					require.Equal(t, 1, writer.closes)
					ins.Reset(proc, false, nil)
				}
			}
			ins.Free(proc, outcome != "eof", nil)
			require.Nil(t, ins.ctr.extWriter)
			require.Nil(t, ins.ctr.extCounter)
			if outcome == "eof" {
				require.Zero(t, writer.aborts)
			} else {
				require.Equal(t, 1, writer.aborts)
				require.Zero(t, writer.closes)
			}
		})
	}
}

// newExternalIgnoreLoadProc builds the session-facing process of the
// external IGNORE legs: the statement's IGNORE runtime profile and the
// warning-collecting session the fill flushes through.
func newExternalIgnoreLoadProc(t *testing.T, q *mpool.MPool) *process.Process {
	proc := testutil.NewProcessWithOwnedMPool(t, "", q)
	session := &extIgnoreWarningSession{}
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	return proc
}

func int64SupplyVec(t *testing.T, q *mpool.MPool, rows int) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed[int64](vec, int64(100+i), false, q))
	}
	return vec
}

// TestExternalInsertOwnsConstFillReplacements walks the actual
// Projection-to-external-Insert lifecycle the allocation-ownership review is
// about, batch after batch: a folded NULL default (the omitted BIGINT NOT
// NULL DEFAULT (abs(null)) column) broadcasts as a constant NULL with no
// backing storage, and under statement IGNORE the gate replaces it with a
// fresh constant allocation installed straight into the projection's reused
// batch. The projection overwrites every slot on its next Call and frees
// only its own executor-owned vectors, the external path writes the child's
// batch in place, and the writer consumes it synchronously - so once a
// batch's write finished, a correctly owned replacement can be released
// immediately, while a dropped one stays recorded in the mpool's pointer
// registry forever. OnHeapOutstanding - the registry-backed on-heap bytes and
// object counter the mpool closes on - is the steady-state oracle, snapshotted
// after every driven batch: flat across batches once every replacement is
// released, one retained block set per batch when one is not.
func TestExternalInsertOwnsConstFillReplacements(t *testing.T) {
	q := mpool.MustNewZero()
	proc := newExternalIgnoreLoadProc(t, q)

	const batches = 6
	const rowsPerBatch = 3
	inputs := make([]*batch.Batch, 0, batches)
	for range batches {
		src := batch.New([]string{"b"})
		src.Vecs[0] = int64SupplyVec(t, q, rowsPerBatch)
		src.SetRowCount(rowsPerBatch)
		inputs = append(inputs, src)
	}
	inputs = append(inputs, nil) // EOF closes the writer

	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{
		{ // DEFAULT (abs(null)) folds to a constant NULL broadcast.
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
		},
		{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		},
	}
	proj.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	require.NoError(t, proj.Prepare(proc))

	ins := NewArgument()
	defer ins.Release()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{Attrs: []string{"a", "b"}, AddAffectedRows: true}
	ins.OpAnalyzer = process.NewAnalyzer(0, false, false, "external-insert")
	ins.ctr.extCols = []*plan.ColDef{notNullCol("a"), nullableCol("b")}
	writer := &mockExtWriter{}
	ins.ctr.extWriter = writer
	ins.AppendChild(proj)

	objectsAfterBatch := make([]int64, batches)
	bytesAfterBatch := make([]int64, batches)
	for i := range batches {
		ins.OpAnalyzer.Start()
		_, err := ins.Call(proc)
		ins.OpAnalyzer.Stop()
		require.NoError(t, err)
		bytesAfterBatch[i], objectsAfterBatch[i] = q.OnHeapOutstanding()
	}
	ins.OpAnalyzer.Start()
	_, err := ins.Call(proc) // EOF: flush and finalize the writer's file
	ins.OpAnalyzer.Stop()
	require.NoError(t, err)
	ins.Free(proc, false, nil)

	require.Equal(t, uint64(batches)*uint64(rowsPerBatch), writer.rows,
		"every driven batch wrote its rows")
	require.Equal(t, 1, writer.closes)
	require.Equal(t, uint64(batches)*uint64(rowsPerBatch), proc.Session.(*extIgnoreWarningSession).total,
		"one warning per adjusted broadcast row of every driven batch")
	for i := 1; i < batches; i++ {
		require.Equal(t, objectsAfterBatch[0], objectsAfterBatch[i],
			"a constant replacement must not stay recorded in the mpool past its own batch")
		require.Equal(t, bytesAfterBatch[0], bytesAfterBatch[i],
			"a constant replacement's storage must not stay recorded in the mpool past its own batch")
	}
	for _, b := range inputs {
		if b != nil {
			b.Clean(q)
		}
	}
}

// TestExternalInsertReleasesFillOnWriteFailure walks the same ownership
// across the pipeline's write-error path: the fill books its replacement,
// the writer rejects the batch, and the statement fails - the replacement
// must be released with the failed batch instead of staying recorded in
// the mpool's pointer registry, and the outstanding counters must return to
// the value before that call.
func TestExternalInsertReleasesFillOnWriteFailure(t *testing.T) {
	q := mpool.MustNewZero()
	proc := newExternalIgnoreLoadProc(t, q)

	newPipeline := func() (*projection.Projection, *Insert, *mockExtWriter, *batch.Batch) {
		src := batch.New([]string{"b"})
		src.Vecs[0] = int64SupplyVec(t, q, 3)
		src.SetRowCount(3)
		proj := projection.NewArgument()
		proj.ProjectList = []*plan.Expr{
			{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
			},
			{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
			},
		}
		proj.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{src, nil}))
		require.NoError(t, proj.Prepare(proc))
		ins := NewArgument()
		ins.ToExternal = true
		ins.InsertCtx = &InsertCtx{Attrs: []string{"a", "b"}, AddAffectedRows: true}
		ins.OpAnalyzer = process.NewAnalyzer(0, false, false, "external-insert")
		ins.ctr.extCols = []*plan.ColDef{notNullCol("a"), nullableCol("b")}
		writer := &mockExtWriter{}
		ins.ctr.extWriter = writer
		ins.AppendChild(proj)
		return proj, ins, writer, src
	}

	// A clean statement first: the fill's replacement settles inside the
	// call, so the outstanding level a failed statement must return to is
	// known.
	_, ins, writer, cleanSrc := newPipeline()
	beforeBytes, beforeObjects := q.OnHeapOutstanding()
	ins.OpAnalyzer.Start()
	_, err := ins.Call(proc)
	ins.OpAnalyzer.Stop()
	require.NoError(t, err)
	afterBytes, afterObjects := q.OnHeapOutstanding()
	require.Equal(t, beforeBytes, afterBytes, "the replacement is released within the call")
	require.Equal(t, beforeObjects, afterObjects)
	require.Equal(t, uint64(3), writer.rows)
	ins.OpAnalyzer.Start()
	_, err = ins.Call(proc) // EOF: flush and finalize the file
	ins.OpAnalyzer.Stop()
	require.NoError(t, err)
	require.Equal(t, 1, writer.closes)
	ins.Free(proc, false, nil)
	ins.Release()
	cleanSrc.Clean(q)

	// The failing statement: the replacement installed for the write must
	// die with the failed batch, not with the operator's teardown.
	_, ins, writer, failSrc := newPipeline()
	writer.failure = errors.New("simulated stage write failure")
	beforeBytes, beforeObjects = q.OnHeapOutstanding()
	ins.OpAnalyzer.Start()
	_, err = ins.Call(proc)
	ins.OpAnalyzer.Stop()
	require.Error(t, err)
	require.Zero(t, writer.rows, "the failed batch wrote no rows")
	afterBytes, afterObjects = q.OnHeapOutstanding()
	require.Equal(t, beforeBytes, afterBytes,
		"the failed batch's replacement is released with the error")
	require.Equal(t, beforeObjects, afterObjects)
	ins.Reset(proc, true, err)
	ins.Free(proc, true, err)
	afterBytes, afterObjects = q.OnHeapOutstanding()
	require.Equal(t, beforeBytes, afterBytes)
	require.Equal(t, beforeObjects, afterObjects)
	require.Equal(t, 1, writer.aborts)
	failSrc.Clean(q)
}

// TestExternalInsertReleasesPartialFillOnViolation pins the release owner's
// bookkeeping on the partial-fill error path: a batch whose first NOT NULL
// column broadcasts a fillable constant NULL and whose second NOT NULL
// column broadcasts the constant NULL of a type without an implicit default
// (geometry here) forces the shared fill to install the first column's
// replacement and then fail on the second. That intermediate state must not
// stay owned by nobody with the failed statement either: the installed
// replacement is released (the mpool's outstanding on-heap counters return
// to the pre-call value) and the child's slot is handed back to the
// borrowed vector.
func TestExternalInsertReleasesPartialFillOnViolation(t *testing.T) {
	q := mpool.MustNewZero()
	proc := newExternalIgnoreLoadProc(t, q)

	const rowsPerBatch = 3
	src := batch.New([]string{"b"})
	src.Vecs[0] = int64SupplyVec(t, q, rowsPerBatch)
	src.SetRowCount(rowsPerBatch)

	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{
		{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
		},
		{ // geometry has no implicit default, so its fill stays a violation.
			Typ:  plan.Type{Id: int32(types.T_geometry)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
		},
	}
	proj.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{src, nil}))
	require.NoError(t, proj.Prepare(proc))

	ins := NewArgument()
	defer ins.Release()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{Attrs: []string{"a", "g"}, AddAffectedRows: true}
	ins.OpAnalyzer = process.NewAnalyzer(0, false, false, "external-insert")
	ins.ctr.extCols = []*plan.ColDef{notNullCol("a"), notNullCol("g")}
	writer := &mockExtWriter{}
	ins.ctr.extWriter = writer
	ins.AppendChild(proj)

	beforeBytes, beforeObjects := q.OnHeapOutstanding()
	ins.OpAnalyzer.Start()
	_, err := ins.Call(proc)
	ins.OpAnalyzer.Stop()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Column 'g' cannot be null")
	require.Zero(t, writer.writes, "the violating batch never reached the writer")
	afterBytes, afterObjects := q.OnHeapOutstanding()
	require.Equal(t, beforeBytes, afterBytes,
		"the replacement installed for the fillable column before the violation is released too")
	require.Equal(t, beforeObjects, afterObjects)
	ins.Reset(proc, true, err)
	ins.Free(proc, true, err)
	afterBytes, afterObjects = q.OnHeapOutstanding()
	require.Equal(t, beforeBytes, afterBytes)
	require.Equal(t, beforeObjects, afterObjects)
	src.Clean(q)
}

// aliasedNullSiblingRow is one external-write observation of the review's
// aliased-fill shape: a's cell and b's cell, captured while the writer held
// the batch.
type aliasedNullSiblingRow struct {
	aNull bool
	aV    int64
	bNull bool
	bV    int64
}

// TestCheckExternalNotNullIgnoreDetachesAliasedFlatDestination pins the
// component-level fact behind the aliased-fill review: a dependent NOT NULL
// default on a nullable reference (b BIGINT NOT NULL DEFAULT (a)) lowers to
// a raw column reference, so the external projection shares one flat vector
// between a's slot and b's. The IGNORE fill of b runs in place, so it must
// not clear a's live NULL row while it fills b's: the aliased destination is
// detached into operator-owned storage first, the sibling keeps its null
// bitmap, and the borrowed slot is handed back with the copy released.
func TestCheckExternalNotNullIgnoreDetachesAliasedFlatDestination(t *testing.T) {
	q := mpool.MustNewZero()
	proc := newExternalIgnoreLoadProc(t, q)

	ins := NewArgument()
	defer ins.Release()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{Attrs: []string{"a", "b"}}
	ins.ctr.extCols = []*plan.ColDef{nullableCol("a"), notNullCol("b")}

	// both slots share one flat vector holding (1, NULL, 2)
	shared := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](shared, 1, false, q))
	require.NoError(t, vector.AppendFixed[int64](shared, 0, true, q))
	require.NoError(t, vector.AppendFixed[int64](shared, 2, false, q))
	bat := batch.New([]string{"a", "b"})
	bat.Vecs[0] = shared
	bat.Vecs[1] = shared
	bat.SetRowCount(3)
	beforeBytes, beforeObjects := q.OnHeapOutstanding()

	require.NoError(t, ins.checkExternalNotNull(proc, bat))
	require.True(t, shared.IsNull(1),
		"the nullable sibling must keep its live NULL while b is filled")
	require.False(t, bat.Vecs[1] == shared,
		"the aliased fill destination runs in its own detached storage")
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 0))
	require.False(t, bat.Vecs[1].IsNull(1))
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 1),
		"DEFAULT (a) projected a's NULL, and IGNORE filled b with the implicit default")
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 2))
	require.Equal(t, uint64(1), proc.Session.(*extIgnoreWarningSession).total,
		"one warning for b's adjusted row")
	require.Len(t, ins.ctr.extFillOwned, 1)
	require.Equal(t, shared, ins.ctr.extFillOwned[0].orig)
	require.Equal(t, bat.Vecs[1], ins.ctr.extFillOwned[0].fill)

	ins.releaseExtFillOwned(bat, proc)
	require.True(t, bat.Vecs[1] == shared,
		"the child's borrowed slot is handed back once the batch was written")
	require.Empty(t, ins.ctr.extFillOwned)
	afterBytes, afterObjects := q.OnHeapOutstanding()
	require.Equal(t, beforeBytes, afterBytes,
		"the detached copy does not survive its batch")
	require.Equal(t, beforeObjects, afterObjects)
	shared.Free(q)
}

// TestCheckExternalNotNullIgnoreDetachesEachAliasedDestination pins the
// counterexample to a single-detach shortcut: when two NOT NULL dependent
// columns (b1, b2 both DEFAULT (a)) share a's flat vector, each destination
// needs its own detached storage - one shared detachment would keep the
// b1/b2 alias, so filling it for b1 would clear the shared NULL before b2's
// own fill, and b2's adjusted-row warning would never be recorded. Each
// column reports its own adjusted row.
func TestCheckExternalNotNullIgnoreDetachesEachAliasedDestination(t *testing.T) {
	q := mpool.MustNewZero()
	proc := newExternalIgnoreLoadProc(t, q)
	session := proc.Session.(*extIgnoreWarningSession)

	ins := NewArgument()
	defer ins.Release()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{Attrs: []string{"a", "b1", "b2"}}
	ins.ctr.extCols = []*plan.ColDef{nullableCol("a"), notNullCol("b1"), notNullCol("b2")}

	shared := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](shared, 1, false, q))
	require.NoError(t, vector.AppendFixed[int64](shared, 0, true, q))
	require.NoError(t, vector.AppendFixed[int64](shared, 2, false, q))
	bat := batch.New([]string{"a", "b1", "b2"})
	for j := range bat.Vecs {
		bat.Vecs[j] = shared
	}
	bat.SetRowCount(3)
	beforeBytes, beforeObjects := q.OnHeapOutstanding()

	require.NoError(t, ins.checkExternalNotNull(proc, bat))
	require.True(t, shared.IsNull(1), "a keeps its live NULL row")
	require.False(t, bat.Vecs[1] == shared, "b1 runs in its own detached storage")
	require.False(t, bat.Vecs[2] == shared, "b2 runs in its own detached storage")
	require.False(t, bat.Vecs[1] == bat.Vecs[2],
		"the two destinations were not detached from each other into one copy")
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 1))
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[2], 1),
		"the second destination was still a NULL when its own fill ran")
	require.Len(t, ins.ctr.extFillOwned, 2)
	require.Equal(t, uint64(2), session.total,
		"each dependent column reports its own adjusted row")
	require.Equal(t, []string{
		"Column 'b1' cannot be null",
		"Column 'b2' cannot be null",
	}, session.messages)

	ins.releaseExtFillOwned(bat, proc)
	require.True(t, bat.Vecs[1] == shared && bat.Vecs[2] == shared)
	require.Empty(t, ins.ctr.extFillOwned)
	afterBytes, afterObjects := q.OnHeapOutstanding()
	require.Equal(t, beforeBytes, afterBytes)
	require.Equal(t, beforeObjects, afterObjects)
	shared.Free(q)
}

// TestExternalInsertIgnoreFillKeepsAliasedNullableSibling walks the review's
// LOAD shape through the same projection-to-writer lifecycle as the other
// external-write legs: the file supplies only the nullable a with mixed rows
// (1, NULL, 2), and b BIGINT NOT NULL DEFAULT (a) projects a's own vector
// into its slot - a nullable reference gets no coalesce rewrite, and the
// column executor hands out the child's vector for both expressions. The
// writer, which consumes the batch synchronously, must receive a's NULL row
// untouched while b carries the implicit default, the child-owned supply
// vector must not have been modified, and any detached copy must die with
// the batch instead of staying recorded in the mpool.
func TestExternalInsertIgnoreFillKeepsAliasedNullableSibling(t *testing.T) {
	q := mpool.MustNewZero()
	proc := newExternalIgnoreLoadProc(t, q)

	const rowsPerBatch = 3
	const batches = 2
	newSrc := func() *batch.Batch {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](vec, 1, false, q))
		require.NoError(t, vector.AppendFixed[int64](vec, 0, true, q))
		require.NoError(t, vector.AppendFixed[int64](vec, 2, false, q))
		src := batch.New([]string{"s"})
		src.Vecs[0] = vec
		src.SetRowCount(rowsPerBatch)
		return src
	}
	supply := make([]*batch.Batch, 0, batches)
	for range batches {
		supply = append(supply, newSrc())
	}

	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{
		{ // a: the supplied column
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		},
		{ // b: DEFAULT (a) lowers to the same raw reference
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		},
	}
	proj.AppendChild(colexec.NewMockOperator().WithBatchs(append(supply, nil)))
	require.NoError(t, proj.Prepare(proc))

	ins := NewArgument()
	defer ins.Release()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{Attrs: []string{"a", "b"}, AddAffectedRows: true}
	ins.OpAnalyzer = process.NewAnalyzer(0, false, false, "external-insert")
	ins.ctr.extCols = []*plan.ColDef{nullableCol("a"), notNullCol("b")}
	writer := &mockExtWriter{}
	ins.ctr.extWriter = writer
	ins.AppendChild(proj)

	written := make([][]aliasedNullSiblingRow, 0, batches)
	writer.onWrite = func(bat *batch.Batch) {
		rows := make([]aliasedNullSiblingRow, 0, bat.RowCount())
		for r := 0; r < bat.RowCount(); r++ {
			rows = append(rows, aliasedNullSiblingRow{
				aNull: bat.Vecs[0].IsNull(uint64(r)),
				aV:    vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], r),
				bNull: bat.Vecs[1].IsNull(uint64(r)),
				bV:    vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], r),
			})
		}
		written = append(written, rows)
	}

	bytesAfterBatch := make([]int64, batches)
	objectsAfterBatch := make([]int64, batches)
	for i := range batches {
		ins.OpAnalyzer.Start()
		_, err := ins.Call(proc)
		ins.OpAnalyzer.Stop()
		require.NoError(t, err)
		bytesAfterBatch[i], objectsAfterBatch[i] = q.OnHeapOutstanding()
	}
	ins.OpAnalyzer.Start()
	_, err := ins.Call(proc) // EOF: flush and finalize the writer's file
	ins.OpAnalyzer.Stop()
	require.NoError(t, err)
	ins.Free(proc, false, nil)

	require.Equal(t, batches, writer.writes)
	require.Equal(t, uint64(batches*rowsPerBatch), writer.rows)
	require.Equal(t, 1, writer.closes)
	wantRows := []aliasedNullSiblingRow{
		{aNull: false, aV: 1, bNull: false, bV: 1},
		{aNull: true, aV: 0, bNull: false, bV: 0},
		{aNull: false, aV: 2, bNull: false, bV: 2},
	}
	require.Len(t, written, batches)
	for i, rows := range written {
		require.Equal(t, wantRows, rows,
			"batch %d: the writer must observe a's NULL row while b carries the implicit default", i)
	}
	require.Equal(t, uint64(batches), proc.Session.(*extIgnoreWarningSession).total,
		"one warning per written batch for b's adjusted NULL row")
	for i := 1; i < batches; i++ {
		require.Equal(t, bytesAfterBatch[0], bytesAfterBatch[i],
			"an aliased destination's detached copy must not stay recorded in the mpool past its own batch")
		require.Equal(t, objectsAfterBatch[0], objectsAfterBatch[i])
	}
	for _, src := range supply {
		require.True(t, src.Vecs[0].IsNull(1),
			"the child-owned supply vector keeps its NULL row after the write")
		src.Clean(q)
	}
}
