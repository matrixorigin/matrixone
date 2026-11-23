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

package function

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newTextReservationFixture(t *testing.T, typ types.Type, values [][]byte) (*process.Process, *vector.Vector, vector.FunctionResultWrapper) {
	t.Helper()
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	source := vector.NewOffHeapVecWithType(typ)
	t.Cleanup(func() { source.Free(proc.Mp()) })
	for _, value := range values {
		require.NoError(t, vector.AppendBytes(source, value, value == nil, proc.Mp()))
	}
	result := vector.NewFunctionResultWrapper(typ, proc.Mp())
	t.Cleanup(result.Free)
	require.NoError(t, result.PreExtendAndReset(len(values)))
	return proc, source, result
}

func runTextReservationCast(proc *process.Process, source *vector.Vector, result vector.FunctionResultWrapper,
	typ types.Type, length int, mode castMode, selected *FunctionSelectList) error {
	return strToStr(context.Background(), proc, vector.GenerateFunctionStrParameter(source),
		vector.MustFunctionResult[types.Varlena](result), length, typ,
		mode.strictStringWidth(), true, true, mode, selected)
}

func TestTextAssignmentReservationPreservesOwnedResults(t *testing.T) {
	for _, oid := range []types.T{types.T_text, types.T_blob} {
		t.Run(oid.String(), func(t *testing.T) {
			typ := types.New(oid, 1024, 0)
			values := [][]byte{bytes.Repeat([]byte("a"), 80), nil, []byte("short"),
				bytes.Repeat([]byte("b"), 110)}
			proc, source, result := newTextReservationFixture(t, typ, values)
			// Ordinary assignment must not propagate transient source metadata.
			source.SetPrepareParamKind(vector.PrepareParamFloat)
			source.SetIsBinaryString(true)

			target := vector.NewVec(typ)
			t.Cleanup(func() { target.Free(proc.Mp()) })
			require.NoError(t, NewAssignCast([]*vector.Vector{source, target}, result, proc, len(values), nil))
			out := result.GetResultVector()
			require.Equal(t, typ, *out.GetType())
			require.Equal(t, len(values), out.Length())
			require.False(t, out.HasPrepareParamKind())
			require.False(t, out.HasBinaryStringMetadata())
			for row, value := range values {
				require.Equal(t, value == nil, out.IsNull(uint64(row)))
				if value != nil {
					require.Equal(t, value, out.GetBytesAt(row))
				}
			}
			// Both directions of mutation prove that no new payload alias escaped.
			out.GetBytesAt(0)[0] = 'z'
			require.Equal(t, byte('a'), source.GetBytesAt(0)[0])
			source.GetBytesAt(3)[0] = 'y'
			require.Equal(t, byte('b'), out.GetBytesAt(3)[0])

			// Reuse of a result with enough capacity must preserve the new input.
			require.NoError(t, result.PreExtendAndReset(len(values)))
			require.NoError(t, runTextReservationCast(proc, source, result, typ, len(values), castModeStrictStringWidth, nil))
			require.Equal(t, byte('a'), result.GetResultVector().GetBytesAt(0)[0])
			require.Equal(t, byte('y'), result.GetResultVector().GetBytesAt(3)[0])
		})
	}
}

func TestTextAssignmentReservationAvoidsRepeatedAreaGrowth(t *testing.T) {
	typ := types.New(types.T_text, 65536, 0)
	values := [][]byte{bytes.Repeat([]byte("a"), 1025), bytes.Repeat([]byte("b"), 2049),
		bytes.Repeat([]byte("c"), 4097), bytes.Repeat([]byte("d"), 8193)}
	proc, source, result := newTextReservationFixture(t, typ, values)
	allocsBefore := proc.Mp().Stats().NumAlloc.Load()
	require.NoError(t, runTextReservationCast(proc, source, result, typ, len(values), castModeStrictStringWidth, nil))
	require.Equal(t, int64(1), proc.Mp().Stats().NumAlloc.Load()-allocsBefore,
		"all non-inline values should fit in one result-area allocation")
	for i, value := range values {
		require.Equal(t, value, result.GetResultVector().GetBytesAt(i))
	}
}

func TestTextAssignmentReservationKeepsDiagnosticsAndSelection(t *testing.T) {
	typ := types.New(types.T_text, 30, 0)
	values := [][]byte{bytes.Repeat([]byte("a"), 24), bytes.Repeat([]byte("b"), 31), []byte("tail")}

	t.Run("strict rejects at original row", func(t *testing.T) {
		proc, source, result := newTextReservationFixture(t, typ, values)
		err := runTextReservationCast(proc, source, result, typ, len(values), castModeStrictStringWidth, nil)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCastWidthExceeded), "%v", err)
		require.Equal(t, 1, result.GetResultVector().Length())
		require.Equal(t, values[0], result.GetResultVector().GetBytesAt(0))
	})
	t.Run("nonstrict truncates and warns", func(t *testing.T) {
		proc, source, result := newTextReservationFixture(t, typ, values)
		sink := &numericWarningSession{}
		proc.WarningSink = sink
		require.NoError(t, runTextReservationCast(proc, source, result, typ, len(values), castModeAssignment, nil))
		require.Equal(t, values[1][:30], result.GetResultVector().GetBytesAt(1))
		require.Len(t, sink.warnings, 1)
		require.Equal(t, moerr.WARN_DATA_TRUNCATED, sink.warnings[0].code)
		require.Contains(t, sink.warnings[0].msg, "row 2")
	})
	t.Run("unselected overlong row stays null", func(t *testing.T) {
		proc, source, result := newTextReservationFixture(t, typ, values)
		sink := &numericWarningSession{}
		proc.WarningSink = sink
		selected := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}
		require.NoError(t, runTextReservationCast(proc, source, result, typ, len(values), castModeStrictStringWidth, selected))
		out := result.GetResultVector()
		require.Equal(t, values[0], out.GetBytesAt(0))
		require.True(t, out.IsNull(1))
		require.Equal(t, values[2], out.GetBytesAt(2))
		require.Empty(t, sink.warnings)
	})
	t.Run("invalid collation remains an error", func(t *testing.T) {
		unicodeType := types.NewWithCharset(types.T_text, 128, 0, types.CharsetUTF8MB4UnicodeCI)
		invalid := bytes.Repeat([]byte{0xff}, 24)
		// Construct physical bytes before assigning their Unicode domain so this
		// exercises cast admission rather than source-vector admission.
		rawType := types.New(types.T_text, 128, 0)
		proc, source, result := newTextReservationFixture(t, rawType, [][]byte{bytes.Repeat([]byte("a"), 24), invalid})
		source.SetType(unicodeType)
		result.GetResultVector().SetType(unicodeType)
		expected := types.ValidateCollationValue(unicodeType, invalid)
		require.Error(t, expected)
		err := runTextReservationCast(proc, source, result, unicodeType, 2, castModeStrictStringWidth, nil)
		require.EqualError(t, err, expected.Error())
		require.Equal(t, 1, result.GetResultVector().Length())
	})
}

func TestTextAssignmentReservationFallsBackForAliasedBackings(t *testing.T) {
	typ := types.New(types.T_text, 1024, 0)
	values := [][]byte{bytes.Repeat([]byte("a"), 80), bytes.Repeat([]byte("b"), 90)}
	proc, _, result := newTextReservationFixture(t, typ, nil)
	out := result.GetResultVector()
	for _, value := range values {
		require.NoError(t, vector.AppendBytes(out, value, false, proc.Mp()))
	}
	source, err := out.Window(0, len(values))
	require.NoError(t, err)
	t.Cleanup(func() { source.Free(nil) })
	// A reused result can share backing storage with a source window.
	source.SetLength(1)
	source.SetLength(2)
	out.ResetWithSameType()
	beforeArea := cap(out.GetArea())
	require.True(t, castVectorsOverlap(source, out))
	done, err := tryReserveAssignedText(proc, vector.GenerateFunctionStrParameter(source),
		vector.MustFunctionResult[types.Varlena](result), len(values), typ, nil)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, beforeArea, cap(out.GetArea()))
	require.Equal(t, values[0], source.GetBytesAt(0))
	require.Equal(t, values[1], source.GetBytesAt(1))
}

func TestTextAssignmentReservationWithSharedPayload(t *testing.T) {
	typ := types.New(types.T_text, 1024, 0)
	proc, source, result := newTextReservationFixture(t, typ, nil)
	value := bytes.Repeat([]byte("s"), 80)
	require.NoError(t, vector.AppendMultiBytes(source, value, false, 3, proc.Mp()))
	require.False(t, source.VarlenaAreaIsDisjoint())
	require.NoError(t, result.PreExtendAndReset(3))
	require.NoError(t, runTextReservationCast(proc, source, result, typ, 3, castModeStrictStringWidth, nil))
	out := result.GetResultVector()
	for i := 0; i < 3; i++ {
		require.Equal(t, value, out.GetBytesAt(i))
	}
	// Repeated source descriptors share one input payload; the output retains
	// the independent per-row semantics of ordinary assignment.
	out.GetBytesAt(0)[0] = 'z'
	require.Equal(t, byte('s'), out.GetBytesAt(1)[0])
	require.Equal(t, byte('s'), source.GetBytesAt(0)[0])

	// Repeated descriptors still fit after reset, so a second assignment must
	// reuse its area without another MPool allocation.
	require.NoError(t, result.PreExtendAndReset(3))
	before := proc.Mp().Stats().NumAllocBytes.Load()
	require.NoError(t, runTextReservationCast(proc, source, result, typ, 3, castModeStrictStringWidth, nil))
	require.Equal(t, before, proc.Mp().Stats().NumAllocBytes.Load())
	require.Equal(t, value, result.GetResultVector().GetBytesAt(2))
}

func TestTextAssignmentReservationAllocationFailureCanReuse(t *testing.T) {
	inputMP := mpool.MustNewZero()
	limitedMP, err := mpool.NewMPool("text-assignment-reservation", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	proc := testutil.NewProcess(t, testutil.WithMPool(limitedMP), testutil.WithFileService(nil))
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, inputMP.CurrNB())
		require.Zero(t, limitedMP.CurrNB())
		mpool.DeleteMPool(inputMP)
		mpool.DeleteMPool(limitedMP)
	})
	typ := types.New(types.T_text, types.MaxLongTextLen, 0)
	source := vector.NewOffHeapVecWithType(typ)
	t.Cleanup(func() { source.Free(inputMP) })
	for i := 0; i < 2; i++ {
		require.NoError(t, vector.AppendBytes(source, bytes.Repeat([]byte("a"), 600<<10), false, inputMP))
	}
	result := vector.NewFunctionResultWrapper(typ, limitedMP)
	t.Cleanup(result.Free)
	require.NoError(t, result.PreExtendAndReset(2))

	err = runTextReservationCast(proc, source, result, typ, 2, castModeStrictStringWidth, nil)
	require.Error(t, err)
	require.True(t, mpool.IsMPoolCapacityFailure(err), "%v", err)
	require.Equal(t, byte('a'), source.GetBytesAt(0)[0])

	source.CleanOnlyData()
	require.NoError(t, vector.AppendBytes(source, bytes.Repeat([]byte("b"), 64), false, inputMP))
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, runTextReservationCast(proc, source, result, typ, 1, castModeStrictStringWidth, nil))
	require.Equal(t, bytes.Repeat([]byte("b"), 64), result.GetResultVector().GetBytesAt(0))
}

func TestTextAssignmentReservationSkipsOneExternalValue(t *testing.T) {
	typ := types.New(types.T_text, 1024, 0)
	values := [][]byte{bytes.Repeat([]byte("x"), 80), []byte("inline")}
	proc, source, result := newTextReservationFixture(t, typ, values)
	done, err := tryReserveAssignedText(proc, vector.GenerateFunctionStrParameter(source),
		vector.MustFunctionResult[types.Varlena](result), len(values), typ, nil)
	require.NoError(t, err)
	require.False(t, done)
	require.NoError(t, runTextReservationCast(proc, source, result, typ, len(values), castModeStrictStringWidth, nil))
	for i := range values {
		require.Equal(t, values[i], result.GetResultVector().GetBytesAt(i))
	}
}

func TestTextAssignmentReservationFromRetainedArea(t *testing.T) {
	typ := types.New(types.T_text, 1024, 0)
	values := [][]byte{bytes.Repeat([]byte("a"), 80), bytes.Repeat([]byte("b"), 90)}
	proc, source, result := newTextReservationFixture(t, typ, values)
	retained := vector.NewOffHeapVecWithType(typ)
	t.Cleanup(func() { retained.Free(proc.Mp()) })
	require.NoError(t, retained.CopyWithReadonlyArea(source, proc.Mp()))
	require.Equal(t, vector.BorrowedLease, retained.AreaBackingKind())

	require.NoError(t, runTextReservationCast(proc, retained, result, typ, len(values), castModeStrictStringWidth, nil))
	// Retiring both input generations cannot invalidate the independent output.
	retained.CleanOnlyData()
	source.CleanOnlyData()
	for i := range values {
		require.Equal(t, values[i], result.GetResultVector().GetBytesAt(i))
	}
}

func TestTextAssignmentReservationConcurrentReaders(t *testing.T) {
	typ := types.New(types.T_text, 1024, 0)
	values := [][]byte{bytes.Repeat([]byte("a"), 80), bytes.Repeat([]byte("b"), 90)}
	proc, source, _ := newTextReservationFixture(t, typ, values)
	const readers = 4
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := vector.NewFunctionResultWrapper(typ, proc.Mp())
			defer result.Free()
			if err := result.PreExtendAndReset(len(values)); err != nil {
				errs <- err
				return
			}
			if err := runTextReservationCast(proc, source, result, typ, len(values), castModeStrictStringWidth, nil); err != nil {
				errs <- err
				return
			}
			for i, want := range values {
				if !bytes.Equal(want, result.GetResultVector().GetBytesAt(i)) {
					errs <- fmt.Errorf("row %d changed", i)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
