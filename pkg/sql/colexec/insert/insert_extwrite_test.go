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
}

func (m *mockExtWriter) WriteBatch(ctx context.Context, bat *batch.Batch, analyzer process.Analyzer) error {
	m.writes++
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
