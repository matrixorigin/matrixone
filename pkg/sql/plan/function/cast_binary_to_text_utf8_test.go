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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// castFn matches the overload constructors (NewExplicitCast, NewCast, ...).
type castFn func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *FunctionSelectList) error

// runBinarySourceTextCast drives a real cast overload with a binary-flagged
// varlena source (IsBin set, as binary literals arrive) and a text-family target,
// returning the result vector. The numericWarningSession sink records exact
// (code, message) diagnostics appended to the statement warning sink.
func runBinarySourceTextCast(t *testing.T, proc *process.Process, fn castFn, value []byte, target types.Type) *vector.Vector {
	t.Helper()
	input := testutil.MakeVarlenaVector([][]byte{value}, nil, types.T_varbinary.ToType(), proc.Mp())
	input.SetIsBin(true)
	t.Cleanup(func() { input.Free(proc.Mp()) })
	targetMarker := testutil.MakeVarlenaVector([][]byte{{0}}, nil, target, proc.Mp())
	t.Cleanup(func() { targetMarker.Free(proc.Mp()) })
	result := vector.NewFunctionResultWrapper(target, proc.Mp())
	t.Cleanup(result.Free)
	require.NoError(t, result.PreExtendAndReset(input.Length()))
	require.NoError(t, fn([]*vector.Vector{input, targetMarker}, result, proc, input.Length(), nil))
	return result.GetResultVector()
}

// #31921: explicit CAST of a binary literal into UTF8 text must not smuggle
// invalid UTF-8 onto the wire. The row becomes NULL and exactly one Warning 1300
// with a bounded hex prefix is appended per offending value.
func TestExplicitBinaryToTextCastRejectsInvalidUtf8(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []byte
		hex  string
	}{
		{"single_ff", []byte{0xFF}, "ff"},
		{"truncated_cjk", []byte{0xC3, 0x28}, "c328"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			sink := &numericWarningSession{}
			proc.WarningSink = sink
			res := runBinarySourceTextCast(t, proc, NewExplicitCast, tc.in, types.T_varchar.ToType())
			require.True(t, res.IsNull(0), "invalid-UTF8 explicit binary->text cast must be NULL")
			require.Equal(t, []numericWarning{
				{1300, "Invalid utf8mb4 character string: '" + tc.hex + "'"},
			}, sink.warnings)
		})
	}
}

// Legal text, empty string and full-width UTF-8 keep their values and stay
// warning-free through the same explicit overload.
func TestExplicitBinaryToTextCastKeepsLegalInputs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{"ascii", []byte("abc")},
		{"empty", []byte{}},
		{"utf8_cjk", []byte("中")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			sink := &numericWarningSession{}
			proc.WarningSink = sink
			res := runBinarySourceTextCast(t, proc, NewExplicitCast, tc.value, types.T_varchar.ToType())
			require.False(t, res.IsNull(0))
			require.Equal(t, tc.value, res.GetBytesAt(0))
			require.Empty(t, sink.warnings, "legal input must not warn")
		})
	}
}

// Boundary controls: only the real explicit entry (castModeExplicit) into a
// UTF8-family text target carries the new policy. Normal mode, comparison casts
// and binary-charset or BINARY destinations keep their previous contracts.
func TestNonExplicitModesKeepBinaryPayload(t *testing.T) {
	varchar := types.T_varchar.ToType()
	t.Run("normal_mode", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		sink := &numericWarningSession{}
		proc.WarningSink = sink
		res := runBinarySourceTextCast(t, proc, NewCast, []byte{0xFF}, varchar)
		require.False(t, res.IsNull(0), "normal mode keeps raw payload")
		require.Equal(t, []byte{0xFF}, res.GetBytesAt(0))
		require.Empty(t, sink.warnings)
	})
	t.Run("comparison_mode", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		sink := &numericWarningSession{}
		proc.WarningSink = sink
		res := runBinarySourceTextCast(t, proc, NewComparisonCast, []byte{0xFF}, varchar)
		require.False(t, res.IsNull(0), "comparison cast keeps raw payload")
		require.Equal(t, []byte{0xFF}, res.GetBytesAt(0))
		require.Empty(t, sink.warnings)
	})
	t.Run("binary_charset_target", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		sink := &numericWarningSession{}
		proc.WarningSink = sink
		target := types.NewWithCharset(types.T_char, 4, 0, types.CharsetBinary)
		res := runBinarySourceTextCast(t, proc, NewExplicitCast, []byte{0xFF}, target)
		require.False(t, res.IsNull(0), "binary-charset text target keeps raw payload")
		require.Equal(t, byte(0xFF), res.GetBytesAt(0)[0])
		require.Empty(t, sink.warnings)
	})
	t.Run("binary_destination", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		sink := &numericWarningSession{}
		proc.WarningSink = sink
		res := runBinarySourceTextCast(t, proc, NewExplicitCast, []byte{0xFF}, types.New(types.T_binary, 4, 0))
		require.False(t, res.IsNull(0))
		require.Equal(t, []byte{0xFF, 0, 0, 0}, res.GetBytesAt(0), "BINARY target pads and keeps raw bytes")
		require.Empty(t, sink.warnings)
	})
}

// runExplicitCastOn drives the real explicit-cast overload with a caller-built
// source vector; the binary domain comes from the vector itself.
func runExplicitCastOn(t *testing.T, proc *process.Process, input *vector.Vector, target types.Type, selectList *FunctionSelectList) *vector.Vector {
	t.Helper()
	targetMarker := testutil.MakeVarlenaVector([][]byte{{0}}, nil, target, proc.Mp())
	t.Cleanup(func() { targetMarker.Free(proc.Mp()) })
	result := vector.NewFunctionResultWrapper(target, proc.Mp())
	t.Cleanup(result.Free)
	require.NoError(t, result.PreExtendAndReset(input.Length()))
	require.NoError(t, NewExplicitCast([]*vector.Vector{input, targetMarker}, result, proc, input.Length(), selectList))
	return result.GetResultVector()
}

// staticBinaryVector builds a source whose binary domain comes only from its
// static CharsetBinary varbinary type (the unhexReturnType identity).
func staticBinaryVector(t *testing.T, proc *process.Process, rows [][]byte, nulls []uint64) *vector.Vector {
	t.Helper()
	typ := unhexReturnType([]types.Type{types.New(types.T_varchar, 16, 0)})
	vec := testutil.MakeVarlenaVector(rows, nulls, typ, proc.Mp())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	require.False(t, vec.GetIsBin(), "binary domain must come from the static type, not the legacy literal flag")
	ub, pr := stringDomainMode(vec)
	require.True(t, ub, "static CharsetBinary varbinary type must resolve to the uniform binary domain")
	require.False(t, pr)
	return vec
}

// #31921 follow-up: the explicit-cast policy keys on the effective binary
// domain (binary_string_semantics.go), so a real UNHEX function result -- a
// static binary-domain varbinary type without the legacy literal flag -- is
// judged per row exactly like a binary literal.
func TestExplicitCastOfUnhexResultRejectsInvalidUtf8(t *testing.T) {
	proc := testutil.NewProcess(t)
	sink := &numericWarningSession{}
	proc.WarningSink = sink
	srcType := types.New(types.T_varchar, 16, 0)
	src := testutil.MakeVarlenaVector([][]byte{[]byte("C328")}, nil, srcType, proc.Mp())
	t.Cleanup(func() { src.Free(proc.Mp()) })
	unhexRes := vector.NewFunctionResultWrapper(unhexReturnType([]types.Type{srcType}), proc.Mp())
	t.Cleanup(unhexRes.Free)
	require.NoError(t, unhexRes.PreExtendAndReset(1))
	require.NoError(t, Unhex([]*vector.Vector{src}, unhexRes, proc, 1, nil))
	produced := unhexRes.GetResultVector()
	require.Equal(t, []byte{0xC3, 0x28}, produced.GetBytesAt(0), "UNHEX('C328') real bytes")
	res := runExplicitCastOn(t, proc, produced, types.T_varchar.ToType(), nil)
	require.True(t, res.IsNull(0), "explicit CAST(UNHEX('C328') AS CHAR) must be NULL")
	require.Equal(t, []numericWarning{{1300, "Invalid utf8mb4 character string: 'c328'"}}, sink.warnings)
}

// The same real UNHEX producer keeps legal UTF-8 payloads warning-free.
func TestExplicitCastOfUnhexResultKeepsLegalUtf8(t *testing.T) {
	proc := testutil.NewProcess(t)
	sink := &numericWarningSession{}
	proc.WarningSink = sink
	srcType := types.New(types.T_varchar, 16, 0)
	src := testutil.MakeVarlenaVector([][]byte{[]byte("E4B8AD")}, nil, srcType, proc.Mp())
	t.Cleanup(func() { src.Free(proc.Mp()) })
	unhexRes := vector.NewFunctionResultWrapper(unhexReturnType([]types.Type{srcType}), proc.Mp())
	t.Cleanup(unhexRes.Free)
	require.NoError(t, unhexRes.PreExtendAndReset(1))
	require.NoError(t, Unhex([]*vector.Vector{src}, unhexRes, proc, 1, nil))
	res := runExplicitCastOn(t, proc, unhexRes.GetResultVector(), types.T_varchar.ToType(), nil)
	require.False(t, res.IsNull(0))
	require.Equal(t, []byte("中"), res.GetBytesAt(0))
	require.Empty(t, sink.warnings)
}

// Static binary-domain type (binary-typed column/bound-parameter identity)
// without any legacy or runtime marker.
func TestExplicitCastStaticBinaryDomainWithoutLegacyFlag(t *testing.T) {
	proc := testutil.NewProcess(t)
	sink := &numericWarningSession{}
	proc.WarningSink = sink
	res := runExplicitCastOn(t, proc, staticBinaryVector(t, proc, [][]byte{{0xFF}}, nil), types.T_varchar.ToType(), nil)
	require.True(t, res.IsNull(0))
	require.Equal(t, []numericWarning{{1300, "Invalid utf8mb4 character string: 'ff'"}}, sink.warnings)
}

// Runtime uniform binary marker (GetIsBinaryString) on a text-family type.
func TestExplicitCastRuntimeUniformBinaryMarker(t *testing.T) {
	proc := testutil.NewProcess(t)
	sink := &numericWarningSession{}
	proc.WarningSink = sink
	vec := testutil.MakeVarlenaVector([][]byte{{0xFF}}, nil, types.T_varchar.ToType(), proc.Mp())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	vec.SetIsBinaryString(true)
	res := runExplicitCastOn(t, proc, vec, types.T_varchar.ToType(), nil)
	require.True(t, res.IsNull(0))
	require.Equal(t, []numericWarning{{1300, "Invalid utf8mb4 character string: 'ff'"}}, sink.warnings)
}

// Per-row sidecar: binary-marked rows are judged; text-override rows keep
// their payload and stay warning-free even when not valid UTF-8.
func TestExplicitCastPerRowSidecarJudgment(t *testing.T) {
	proc := testutil.NewProcess(t)
	sink := &numericWarningSession{}
	proc.WarningSink = sink
	vec := testutil.MakeVarlenaVector([][]byte{{0xFF}, {0xC3, 0x28}}, nil, types.T_varchar.ToType(), proc.Mp())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	require.NoError(t, vec.SetIsBinaryStringAt(0, true, proc.Mp()))
	ub, pr := stringDomainMode(vec)
	require.False(t, ub)
	require.True(t, pr, "row sidecar must select the per-row domain path")
	res := runExplicitCastOn(t, proc, vec, types.T_varchar.ToType(), nil)
	require.True(t, res.IsNull(0), "binary-marked row 0 becomes NULL")
	require.False(t, res.IsNull(1), "text-override row 1 keeps its payload")
	require.Equal(t, []byte{0xC3, 0x28}, res.GetBytesAt(1))
	require.Equal(t, []numericWarning{{1300, "Invalid utf8mb4 character string: 'ff'"}}, sink.warnings,
		"exactly one warning, only for the binary-marked row")
}

// NULL rows and selection-skipped rows never emit the 1300 warning.
func TestExplicitCastNullAndUnselectedRowsSkipWarning(t *testing.T) {
	t.Run("null_row", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		sink := &numericWarningSession{}
		proc.WarningSink = sink
		res := runExplicitCastOn(t, proc, staticBinaryVector(t, proc, [][]byte{{0xFF}}, []uint64{0}), types.T_varchar.ToType(), nil)
		require.True(t, res.IsNull(0))
		require.Empty(t, sink.warnings, "NULL rows must not warn")
	})
	t.Run("unselected_row", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		sink := &numericWarningSession{}
		proc.WarningSink = sink
		vec := staticBinaryVector(t, proc, [][]byte{{0xFF}, []byte("abc")}, nil)
		// SelectList is inverted: with AnyNull set, SelectList[i]==false skips row i.
		sel := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}}
		res := runExplicitCastOn(t, proc, vec, types.T_varchar.ToType(), sel)
		require.True(t, res.IsNull(0), "skipped row stays NULL")
		require.False(t, res.IsNull(1))
		require.Equal(t, []byte("abc"), res.GetBytesAt(1))
		require.Empty(t, sink.warnings, "unselected rows must not warn")
	})
}
