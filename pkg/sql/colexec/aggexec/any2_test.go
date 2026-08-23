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

package aggexec

import (
	"bytes"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAnyValueBatchFillReturnsSetRawBytesAtError(t *testing.T) {
	inputMp := mpool.MustNewZero()
	input := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(input, make([]byte, 4096), false, inputMp))
	defer input.Free(inputMp)

	exec, limitedMp, filler := newLimitedAnyValueExec(t)
	defer cleanupLimitedAnyValueExec(exec, limitedMp, filler)

	err := exec.BatchFill(0, []uint64{1}, []*vector.Vector{input})
	require.Error(t, err)
	require.True(t, exec.state[0].vecs[0].IsNull(0))
}

func TestAnyValueBatchMergeReturnsSetRawBytesAtError(t *testing.T) {
	inputMp := mpool.MustNewZero()
	input := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(input, make([]byte, 4096), false, inputMp))
	defer input.Free(inputMp)

	sourceMp := mpool.MustNewZero()
	source := makeAnyValueExec(sourceMp, 1, types.T_varchar.ToType()).(*anyExec)
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, source.BatchFill(0, []uint64{1}, []*vector.Vector{input}))
	defer source.Free()

	target, limitedMp, filler := newLimitedAnyValueExec(t)
	defer cleanupLimitedAnyValueExec(target, limitedMp, filler)

	err := target.BatchMerge(source, 0, []uint64{1})
	require.Error(t, err)
	require.True(t, target.state[0].vecs[0].IsNull(0))
}

func TestAnyValuePreservesFirstPrepareParamKind(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("5"), false, mp))
	require.NoError(t, vector.AppendBytes(input, []byte("9"), false, mp))
	input.SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamFloat,
		vector.PrepareParamNone,
	})
	require.NoError(t, input.SetBinaryStringRowsWithMP([]bool{true, false}, mp))

	exec := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
	defer func() {
		input.Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, vector.PrepareParamFloat, results[0].GetPrepareParamKindAt(0))
	require.True(t, results[0].GetBinaryStringMetadataAt(0))
	results[0].Free(mp)
}

func TestAnyValueIntermediateRoundTripAllStringSources(t *testing.T) {
	for _, source := range []types.StringSource{
		types.StringSourceExpression,
		types.StringSourceLiteral,
		types.StringSourceUserVariable,
		types.StringSourceSQLPrepare,
		types.StringSourceCOMStmt,
	} {
		for _, saveChunk := range []bool{false, true} {
			mp := mpool.MustNewZero()
			input := vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(input, []byte("value"), false, mp))
			require.NoError(t, input.SetStringSource(source))
			exec := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			var wire bytes.Buffer
			if saveChunk {
				require.NoError(t, exec.SaveIntermediateResultOfChunk(0, &wire))
			} else {
				require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &wire))
			}

			restored := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
			require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))
			results, err := restored.Flush()
			require.NoError(t, err)
			require.Equal(t, source, results[0].GetStringSourceAt(0))
			results[0].Free(mp)
			restored.Free()
			exec.Free()
			input.Free(mp)
			require.Zero(t, mp.CurrNB(), "source=%v saveChunk=%v", source, saveChunk)
		}
	}
}

func TestAnyValueIntermediateRejectsInvalidStringSourceAndReusesDecoder(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("value"), false, mp))
	require.NoError(t, input.SetStringSource(types.StringSourceCOMStmt))
	source := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, source.BulkFill(0, []*vector.Vector{input}))
	var wire bytes.Buffer
	require.NoError(t, source.SaveIntermediateResultOfChunk(0, &wire))
	valid := append([]byte(nil), wire.Bytes()...)
	invalid := append([]byte(nil), valid...)
	require.GreaterOrEqual(t, len(invalid), 9)
	invalid[len(invalid)-9] = 0xfc // source=63, runtime-domain=inherit

	restored := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
	require.ErrorContains(t,
		restored.UnmarshalFromReader(bytes.NewReader(invalid), mp),
		"invalid aggregate binary provenance")
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(valid), mp))
	results, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, types.StringSourceCOMStmt, results[0].GetStringSourceAt(0))
	results[0].Free(mp)
	restored.Free()
	source.Free()
	input.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestMergeEqualAggregateCandidatesMergesStringSources(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	for _, test := range []struct {
		left, right types.StringSource
		want        types.StringSource
	}{
		{types.StringSourceLiteral, types.StringSourceLiteral, types.StringSourceLiteral},
		{types.StringSourceCOMStmt, types.StringSourceCOMStmt, types.StringSourceCOMStmt},
		{types.StringSourceLiteral, types.StringSourceCOMStmt, types.StringSourceExpression},
		{types.StringSourceExpression, types.StringSourceSQLPrepare, types.StringSourceExpression},
	} {
		left := vector.NewVec(types.T_text.ToType())
		right := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(left, []byte("same"), false, mp))
		require.NoError(t, vector.AppendBytes(right, []byte("same"), false, mp))
		require.NoError(t, left.SetStringSource(test.left))
		require.NoError(t, right.SetStringSource(test.right))
		require.NoError(t, mergeEqualRuntimeStringDomain(left, 0, right, 0, mp))
		require.Equal(t, test.want, left.GetStringSourceAt(0))
		left.Free(mp)
		right.Free(mp)
	}
}

func TestAnyValueIntermediateRoundTripPreservesStringSemantics(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("binary"), false, mp))
	input.SetIsBinaryString(true)
	require.NoError(t, input.SetStringSource(types.StringSourceCOMStmt))
	source := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, source.BulkFill(0, []*vector.Vector{input}))
	var wire bytes.Buffer
	require.NoError(t, source.SaveIntermediateResultOfChunk(0, &wire))

	restored := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))
	results, err := restored.Flush()
	require.NoError(t, err)
	require.True(t, results[0].GetBinaryStringMetadataAt(0))
	require.Equal(t, types.StringSourceCOMStmt, results[0].GetStringSourceAt(0))

	results[0].Free(mp)
	restored.Free()
	source.Free()
	input.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestAnyValueIntermediateOldPeerOmitsStringSource(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("literal"), false, mp))
	require.NoError(t, input.SetStringSource(types.StringSourceLiteral))
	source := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, source.BulkFill(0, []*vector.Vector{input}))
	var wire bytes.Buffer
	protocolWriter := source.(interface {
		SaveIntermediateResultOfChunkWithStringSource(int, io.Writer, bool) error
	})
	require.NoError(t, protocolWriter.SaveIntermediateResultOfChunkWithStringSource(0, &wire, false))
	restored := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))
	results, err := restored.Flush()
	require.NoError(t, err)
	require.False(t, results[0].HasStringSourceMetadata())
	results[0].Free(mp)
	restored.Free()
	source.Free()
	input.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestAnyValuePreservesExplicitTextFromNullSlot(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("text"), false, mp))
	require.NoError(t, input.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))
	exec := makeAnyValueExec(mp, AggIdOfAny, types.T_varbinary.ToType())
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
	results, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, types.RuntimeStringText, results[0].GetRuntimeStringDomainAt(0))
	results[0].Free(mp)
	exec.Free()
	input.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func newLimitedAnyValueExec(t *testing.T) (*anyExec, *mpool.MPool, []byte) {
	t.Helper()

	limitedMp, err := mpool.NewMPool("any-value-limited", 1024*1024, mpool.NoFixed)
	require.NoError(t, err)

	exec := makeAnyValueExec(limitedMp, 1, types.T_varchar.ToType()).(*anyExec)
	require.NoError(t, exec.GroupGrow(1))

	remaining := 1024*1024 - limitedMp.CurrNB()
	require.Greater(t, remaining, int64(4096))
	filler, err := limitedMp.Alloc(int(remaining-1024), true)
	require.NoError(t, err)

	return exec, limitedMp, filler
}

func cleanupLimitedAnyValueExec(exec *anyExec, mp *mpool.MPool, filler []byte) {
	if filler != nil {
		mp.Free(filler)
	}
	exec.Free()
	mpool.DeleteMPool(mp)
}
