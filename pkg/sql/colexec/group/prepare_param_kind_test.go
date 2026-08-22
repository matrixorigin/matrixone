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

package group

import (
	"bufio"
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// unknownGroupServiceLockService supplies only the service identity needed by
// Process.GetService. The embedded interface keeps this regression focused on
// the runtime lookup boundary rather than lock-service behavior.
type unknownGroupServiceLockService struct {
	lockservice.LockService
	cfg lockservice.Config
}

type prepareParamKindTestAccessor struct {
	vecs []*vector.Vector
}

func (a *prepareParamKindTestAccessor) PrepareParamKindChunkCount() int {
	return len(a.vecs)
}

func (a *prepareParamKindTestAccessor) PrepareParamKindVectorForChunk(chunk int) *vector.Vector {
	if chunk < 0 || chunk >= len(a.vecs) {
		return nil
	}
	return a.vecs[chunk]
}

func (s *unknownGroupServiceLockService) GetConfig() lockservice.Config {
	return s.cfg
}

func makeSpillGroupBatchForTest(t *testing.T, mp *mpool.MPool, prepared bool) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, mp))
	if prepared {
		bat.Vecs[0].SetPrepareParamKind(vector.PrepareParamFloat)
	}
	bat.SetRowCount(2)
	return bat
}

func TestPrepareParamKindWireUnknownServiceFailsClosed(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.LockService = &unknownGroupServiceLockService{
		cfg: lockservice.Config{ServiceID: "group-unknown-service"},
	}

	var enabled bool
	require.NotPanics(t, func() {
		enabled = prepareParamKindWireV1Enabled(proc)
	})
	require.False(t, enabled)

	// Keep the nil-process behavior paired with the unknown-service boundary:
	// both must fail closed without consulting a runtime.
	var nilEnabled bool
	require.NotPanics(t, func() {
		nilEnabled = prepareParamKindWireV1Enabled((*process.Process)(nil))
	})
	require.False(t, nilEnabled)
}

func TestGroupSpillGroupKeyPrepareParamKindCodec(t *testing.T) {
	mp := mpool.MustNewZero()
	legacy := makeSpillGroupBatchForTest(t, mp, false)
	prepared := makeSpillGroupBatchForTest(t, mp, true)
	defer func() {
		prepared.Clean(mp)
		legacy.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, prepared.Vecs[0].SetPrepareParamKindsWithMP(
		[]vector.PrepareParamKind{vector.PrepareParamFloat, vector.PrepareParamInteger}, mp))

	rows := []int32{0, 1}
	newDestination := func() *batch.Batch {
		decoded := batch.NewOffHeapWithSize(1)
		decoded.Vecs[0] = vector.NewOffHeapVecWithType(types.T_text.ToType())
		return decoded
	}
	var legacyRecord bytes.Buffer
	require.NoError(t, appendSpillGroupByRows(&legacyRecord, legacy, rows))
	decoded := newDestination()
	require.NoError(t, unmarshalSpillGroupByRows(
		bytes.NewReader(legacyRecord.Bytes()), decoded, len(rows), mp))
	require.False(t, decoded.Vecs[0].HasPrepareParamKind())
	decoded.Clean(mp)

	var preparedRecord bytes.Buffer
	require.NoError(t, appendSpillGroupByRows(&preparedRecord, prepared, rows))
	decoded = newDestination()
	require.NoError(t, unmarshalSpillGroupByRows(
		bytes.NewReader(preparedRecord.Bytes()), decoded, len(rows), mp))
	require.Equal(t, vector.PrepareParamFloat, decoded.Vecs[0].GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamInteger, decoded.Vecs[0].GetPrepareParamKindAt(1))
	decoded.Clean(mp)

	truncated := append([]byte(nil), preparedRecord.Bytes()[:preparedRecord.Len()-1]...)
	decoded = newDestination()
	require.Error(t, unmarshalSpillGroupByRows(
		bytes.NewReader(truncated), decoded, len(rows), mp))
	decoded.Clean(mp)

	invalid := append([]byte(nil), preparedRecord.Bytes()...)
	// column count (4 bytes), followed by selected row count (4 bytes), then
	// the selected-vector metadata byte.
	require.Greater(t, len(invalid), 8)
	invalid[8] |= 0xc0 // source mode 3 is reserved
	decoded = newDestination()
	require.ErrorContains(t, unmarshalSpillGroupByRows(
		bytes.NewReader(invalid), decoded, len(rows), mp), "metadata")
	decoded.Clean(mp)
}

func prepareParamKindRowsTrailerForTest(rowCount int32, rows []byte) []byte {
	var buf bytes.Buffer
	buf.Write([]byte{prepareParamKindTrailerMagic0, prepareParamKindTrailerMagic1,
		prepareParamKindTrailerMagic2, prepareParamKindTrailerRowsVersion})
	nAggs := int32(1)
	buf.Write(types.EncodeInt32(&nAggs))
	buf.WriteByte(prepareParamKindTrailerRowsMarker)
	buf.Write(types.EncodeInt32(&rowCount))
	buf.Write(rows)
	return buf.Bytes()
}

func makePrepareParamKindVectorForTest(
	t *testing.T,
	mp *mpool.MPool,
	kinds []vector.PrepareParamKind,
) *vector.Vector {
	t.Helper()
	values := make([]int64, len(kinds))
	vec := testutil.MakeInt64Vector(values, nil, mp)
	require.NoError(t, vec.SetPrepareParamKindsWithMP(kinds, mp))
	return vec
}

func TestPrepareParamKindTrailerV1ScalarRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.MakeInt64Vector([]int64{1, 2}, nil, mp)
	vec.SetPrepareParamKind(vector.PrepareParamFloat)
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	aggs := []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
	}
	var states aggexec.PrepareParamKindStates
	states.Reset(aggs)
	states.Observe(0, vector.PrepareParamFloat)
	source, err := newPrepareParamKindRowsSource(vec, nil)
	require.NoError(t, err)
	require.Zero(t, source.rowCount)

	var encoded bytes.Buffer
	require.NoError(t, writePrepareParamKindTrailer(
		context.Background(), &encoded, aggs, &states,
		[]prepareParamKindRowsSource{source}))
	require.Equal(t, prepareParamKindTrailerVersion, encoded.Bytes()[3])

	var decoded aggexec.PrepareParamKindStates
	decoded.Reset(aggs)
	summaries, err := readPrepareParamKindTrailer(
		context.Background(), bytes.NewReader(encoded.Bytes()), 1, &decoded,
		[]prepareParamKindRowsTarget{{expectedRows: -1}}, mp, true)
	require.NoError(t, err)
	require.Equal(t, []prepareParamKindSummary{{
		kind: vector.PrepareParamFloat,
		seen: true,
	}}, summaries)
	kind, seen := decoded.GetState(0)
	require.Equal(t, vector.PrepareParamFloat, kind)
	require.True(t, seen)
}

func TestPrepareParamKindTrailerV4UniformTextUsesScalarEncoding(t *testing.T) {
	mp := mpool.MustNewZero()
	vec, err := vector.NewConstBytes(types.T_varbinary.ToType(), []byte("selected"), 65536, mp)
	require.NoError(t, err)
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, vec.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))

	aggs := []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
	}
	var states aggexec.PrepareParamKindStates
	states.Reset(aggs)
	source, err := newPrepareParamKindRowsSource(vec, nil)
	require.NoError(t, err)
	require.Zero(t, source.rowCount)
	require.True(t, source.summary.textString)

	var encoded bytes.Buffer
	require.NoError(t, writePrepareParamKindTrailer(
		context.Background(), &encoded, aggs, &states,
		[]prepareParamKindRowsSource{source}))
	require.Equal(t, prepareParamKindTrailerDomainVersion, encoded.Bytes()[3])
	require.Less(t, encoded.Len(), 64)

	var decoded aggexec.PrepareParamKindStates
	decoded.Reset(aggs)
	summaries, err := readPrepareParamKindTrailer(
		context.Background(), bytes.NewReader(encoded.Bytes()), 1, &decoded,
		[]prepareParamKindRowsTarget{{expectedRows: -1}}, mp, true, true)
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	require.True(t, summaries[0].textString)
	require.False(t, summaries[0].rows)
}

func TestPrepareParamKindDomainTrailerRequiresExplicitTextCapability(t *testing.T) {
	var encoded bytes.Buffer
	encoded.Write([]byte{prepareParamKindTrailerMagic0, prepareParamKindTrailerMagic1,
		prepareParamKindTrailerMagic2, prepareParamKindTrailerDomainVersion})
	nAggs := int32(1)
	encoded.Write(types.EncodeInt32(&nAggs))
	encoded.WriteByte(0)
	encoded.WriteByte(byte(types.RuntimeStringText))
	var states aggexec.PrepareParamKindStates
	states.Reset([]aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
	})
	_, err := readPrepareParamKindTrailer(context.Background(), bytes.NewReader(encoded.Bytes()),
		1, &states, nil, mpool.MustNewZero(), true)
	require.ErrorContains(t, err, "MORPCVersion23")
}

func TestPrepareParamKindTrailerV2StreamsSelectedRowsForMultipleAggregates(t *testing.T) {
	mp := mpool.MustNewZero()
	sourceA := makePrepareParamKindVectorForTest(t, mp, []vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamFloat,
		vector.PrepareParamDecimal,
		vector.PrepareParamBoolean,
	})
	sourceB := makePrepareParamKindVectorForTest(t, mp, []vector.PrepareParamKind{
		vector.PrepareParamDecimal,
		vector.PrepareParamInteger,
		vector.PrepareParamFloat,
		vector.PrepareParamNone,
	})
	destinationA := testutil.MakeInt64Vector([]int64{0, 0}, nil, mp)
	destinationB := testutil.MakeInt64Vector([]int64{0, 0}, nil, mp)
	defer func() {
		sourceA.Free(mp)
		sourceB.Free(mp)
		destinationA.Free(mp)
		destinationB.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	flags := []uint8{0, 1, 0, 1}
	flagsA, err := newPrepareParamKindRowsSource(sourceA, flags)
	require.NoError(t, err)
	flagsB, err := newPrepareParamKindRowsSource(sourceB, flags)
	require.NoError(t, err)
	rowsA, err := newPrepareParamKindSelectedRowsSource(sourceA, []int32{1, 3})
	require.NoError(t, err)
	rowsB, err := newPrepareParamKindSelectedRowsSource(sourceB, []int32{1, 3})
	require.NoError(t, err)
	require.Equal(t, 2, rowsA.rowCount)
	require.Equal(t, 2, rowsB.rowCount)

	aggs := []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMax, false, nil, nil),
	}
	var states aggexec.PrepareParamKindStates
	states.Reset(aggs)
	var encodedFlags bytes.Buffer
	require.NoError(t, writePrepareParamKindTrailer(
		context.Background(), &encodedFlags, aggs, &states,
		[]prepareParamKindRowsSource{flagsA, flagsB}))
	var encoded bytes.Buffer
	require.NoError(t, writePrepareParamKindTrailer(
		context.Background(), &encoded, aggs, &states,
		[]prepareParamKindRowsSource{rowsA, rowsB}))
	require.Equal(t, encodedFlags.Bytes(), encoded.Bytes())
	require.Equal(t, prepareParamKindTrailerRowsVersion, encoded.Bytes()[3])

	targetA := &prepareParamKindTestAccessor{vecs: []*vector.Vector{destinationA}}
	targetB := &prepareParamKindTestAccessor{vecs: []*vector.Vector{destinationB}}
	var decoded aggexec.PrepareParamKindStates
	decoded.Reset(aggs)
	summaries, err := readPrepareParamKindTrailer(
		context.Background(), bytes.NewReader(encoded.Bytes()), 2, &decoded,
		[]prepareParamKindRowsTarget{
			prepareParamKindChunkTarget(targetA, 0),
			prepareParamKindChunkTarget(targetB, 0),
		}, mp, true)
	require.NoError(t, err)
	require.True(t, summaries[0].rows)
	require.True(t, summaries[1].rows)
	require.Equal(t, []vector.PrepareParamKind{
		vector.PrepareParamFloat,
		vector.PrepareParamBoolean,
	}, destinationA.GetPrepareParamKinds())
	require.Equal(t, []vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamNone,
	}, destinationB.GetPrepareParamKinds())
}

func TestPrepareParamKindTrailerFailureDoesNotPublishCumulativeState(t *testing.T) {
	aggs := []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMax, false, nil, nil),
	}
	var states aggexec.PrepareParamKindStates
	states.Reset(aggs)
	states.Observe(0, vector.PrepareParamDecimal)
	states.Observe(1, vector.PrepareParamBoolean)

	var payload bytes.Buffer
	payload.Write([]byte{
		prepareParamKindTrailerMagic0,
		prepareParamKindTrailerMagic1,
		prepareParamKindTrailerMagic2,
		prepareParamKindTrailerVersion,
	})
	nAggs := int32(2)
	payload.Write(types.EncodeInt32(&nAggs))
	payload.WriteByte(byte(vector.PrepareParamFloat) + 1)
	payload.WriteByte(255)

	_, err := readPrepareParamKindTrailer(
		context.Background(), bytes.NewReader(payload.Bytes()), nAggs, &states,
		[]prepareParamKindRowsTarget{{expectedRows: -1}, {expectedRows: -1}}, nil, true)
	require.ErrorContains(t, err, "invalid aggregate prepared parameter state")
	kind, seen := states.GetState(0)
	require.Equal(t, vector.PrepareParamDecimal, kind)
	require.True(t, seen)
	kind, seen = states.GetState(1)
	require.Equal(t, vector.PrepareParamBoolean, kind)
	require.True(t, seen)
}

func TestPrepareParamKindTrailerRejectsRowAmplificationBeforeAllocation(t *testing.T) {
	states := aggexec.PrepareParamKindStates{}
	states.Reset([]aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
	})

	tests := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{
			name:    "zero rows marker",
			payload: prepareParamKindRowsTrailerForTest(0, nil),
			wantErr: "invalid aggregate prepared parameter row count 0",
		},
		{
			name:    "amplified rows mismatch",
			payload: prepareParamKindRowsTrailerForTest(1<<24, nil),
			wantErr: "row count 16777216 does not match 1",
		},
		{
			name:    "truncated rows",
			payload: prepareParamKindRowsTrailerForTest(1, nil),
			wantErr: "unexpected EOF",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			localStates := aggexec.PrepareParamKindStates{}
			localStates.Reset([]aggexec.AggFuncExecExpression{
				aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
			})
			_, err := readPrepareParamKindTrailer(
				context.Background(), bytes.NewReader(tc.payload), 1, &localStates,
				[]prepareParamKindRowsTarget{{expectedRows: 1}}, nil, true)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestPrepareParamKindTrailerRejectsBufferedSpillAmplification(t *testing.T) {
	states := aggexec.PrepareParamKindStates{}
	states.Reset([]aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
	})
	payload := prepareParamKindRowsTrailerForTest(1<<24, nil)
	_, err := readPrepareParamKindTrailer(
		context.Background(), bufio.NewReader(bytes.NewReader(payload)), 1, &states,
		[]prepareParamKindRowsTarget{{expectedRows: 1}}, nil, true)
	require.ErrorContains(t, err, "row count 16777216 does not match 1")
}

func TestPrepareParamKindTrailerRejectsRowsWithoutStateBound(t *testing.T) {
	states := aggexec.PrepareParamKindStates{}
	states.Reset([]aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
	})
	payload := prepareParamKindRowsTrailerForTest(1<<24, nil)
	_, err := readPrepareParamKindTrailer(
		context.Background(), bufio.NewReader(bytes.NewReader(payload)), 1, &states,
		[]prepareParamKindRowsTarget{{expectedRows: -1}}, nil, true)
	require.ErrorContains(t, err, "does not expose a prepared parameter row count")
}

func TestPrepareParamKindStateCodec(t *testing.T) {
	tests := []struct {
		name    string
		kind    vector.PrepareParamKind
		seen    bool
		encoded byte
	}{
		{name: "unseen", kind: vector.PrepareParamNone, seen: false, encoded: 0},
		{name: "observed-string", kind: vector.PrepareParamNone, seen: true, encoded: 1},
		{name: "integer", kind: vector.PrepareParamInteger, seen: true, encoded: 2},
		{name: "float", kind: vector.PrepareParamFloat, seen: true, encoded: 3},
		{name: "decimal", kind: vector.PrepareParamDecimal, seen: true, encoded: 4},
		{name: "boolean", kind: vector.PrepareParamBoolean, seen: true, encoded: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded, ok := encodePrepareParamKindState(tc.kind, tc.seen)
			require.True(t, ok)
			require.Equal(t, tc.encoded, encoded)

			kind, seen, ok := decodePrepareParamKindState(encoded)
			require.True(t, ok)
			require.Equal(t, tc.kind, kind)
			require.Equal(t, tc.seen, seen)
		})
	}
	for _, kind := range []vector.PrepareParamKind{
		vector.PrepareParamBoolean + 1,
		vector.PrepareParamKind(255),
	} {
		encoded, ok := encodePrepareParamKindState(kind, true)
		require.False(t, ok)
		require.Zero(t, encoded)
	}

	for _, encoded := range []byte{6, 255} {
		kind, seen, ok := decodePrepareParamKindState(encoded)
		require.False(t, ok)
		require.False(t, seen)
		require.Equal(t, vector.PrepareParamNone, kind)
	}
}
