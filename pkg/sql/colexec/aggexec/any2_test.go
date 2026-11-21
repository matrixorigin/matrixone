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

// TestAnyValueIntermediateDecodesLegacyStringStateTrailer checks wire
// compatibility with older writers that emitted the string-state trailer,
// which packs a string source above the runtime domain in each row byte. The
// source bits must be validated and dropped; the domains must be restored.
func TestAnyValueIntermediateDecodesLegacyStringStateTrailer(t *testing.T) {
	domains := []types.RuntimeStringDomain{
		types.RuntimeStringBinary,
		types.RuntimeStringText,
		types.RuntimeStringInherit,
	}
	for _, saveChunk := range []bool{false, true} {
		mp := mpool.MustNewZero()
		input := vector.NewVec(types.T_text.ToType())
		for row, domain := range domains {
			require.NoError(t, vector.AppendBytes(input, []byte("value"), false, mp))
			require.NoError(t, input.SetRuntimeStringDomainAtWithMP(row, domain, mp))
		}
		source := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
		require.NoError(t, source.GroupGrow(len(domains)))
		require.NoError(t, source.BatchFill(0, []uint64{1, 2, 3}, []*vector.Vector{input}))
		var wire bytes.Buffer
		if saveChunk {
			require.NoError(t, source.SaveIntermediateResultOfChunk(0, &wire))
		} else {
			require.NoError(t, source.SaveIntermediateResult(3, [][]uint8{{1, 1, 1}}, &wire))
		}

		// Current writers emit the domain-only trailer.
		var marker bytes.Buffer
		require.NoError(t, types.WriteUint64(&marker, aggStringDomainTrailerMagic))
		at := bytes.LastIndex(wire.Bytes(), marker.Bytes())
		require.GreaterOrEqual(t, at, 0)
		var stateMarker bytes.Buffer
		require.NoError(t, types.WriteUint64(&stateMarker, aggStringStateTrailerMagic))
		require.Equal(t, -1, bytes.Index(wire.Bytes(), stateMarker.Bytes()),
			"writers must not emit the retired string-state trailer")

		// Rewrite it into the legacy string-state form carrying sources.
		legacy := append([]byte(nil), wire.Bytes()...)
		copy(legacy[at:], stateMarker.Bytes())
		rows := at + len(marker.Bytes()) + 4
		legacySources := []types.StringSource{
			types.StringSourceCOMStmt,
			types.StringSourceLiteral,
			types.StringSourceSQLPrepare,
		}
		for row := range domains {
			require.Equal(t, byte(domains[row]), legacy[rows+row])
			legacy[rows+row] |= byte(legacySources[row]) << 2
		}
		invalid := append([]byte(nil), legacy...)
		invalid[rows] = byte(domains[0]) | 0xfc // source=63

		restored := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
		require.ErrorContains(t,
			restored.UnmarshalFromReader(bytes.NewReader(invalid), mp),
			"invalid aggregate binary provenance")
		require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(legacy), mp))
		results, err := restored.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		for row, domain := range domains {
			require.Equal(t, []byte("value"), results[0].GetBytesAt(row))
			require.Equal(t, domain, results[0].GetRuntimeStringDomainAt(row), "row %d", row)
		}

		// Re-encoding the decoded state yields the source-free current form.
		reencoder := makeAnyValueExec(mp, AggIdOfAny, types.T_text.ToType())
		require.NoError(t, reencoder.UnmarshalFromReader(bytes.NewReader(legacy), mp))
		var reencoded bytes.Buffer
		if saveChunk {
			require.NoError(t, reencoder.SaveIntermediateResultOfChunk(0, &reencoded))
		} else {
			require.NoError(t, reencoder.SaveIntermediateResult(3, [][]uint8{{1, 1, 1}}, &reencoded))
		}
		require.Equal(t, wire.Bytes(), reencoded.Bytes())

		reencoder.Free()
		results[0].Free(mp)
		restored.Free()
		source.Free()
		input.Free(mp)
		require.Zero(t, mp.CurrNB(), "saveChunk=%v", saveChunk)
	}
}

func TestAnyValueIntermediateRoundTripPreservesStringSemantics(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("binary"), false, mp))
	input.SetIsBinaryString(true)
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
