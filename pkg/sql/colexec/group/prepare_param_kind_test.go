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

	var payload bytes.Buffer
	var legacyRecord bytes.Buffer
	require.NoError(t, appendSpillGroupByBatch(&legacyRecord, legacy, &payload))
	decoded := batch.NewWithSize(0)
	require.NoError(t, unmarshalSpillGroupByBatch(
		bufio.NewReader(bytes.NewReader(legacyRecord.Bytes())), decoded, mp))
	require.False(t, decoded.Vecs[0].HasPrepareParamKind())
	decoded.Clean(mp)

	var preparedRecord bytes.Buffer
	require.NoError(t, appendSpillGroupByBatch(&preparedRecord, prepared, &payload))
	decoded = batch.NewWithSize(0)
	require.NoError(t, unmarshalSpillGroupByBatch(
		bufio.NewReader(bytes.NewReader(preparedRecord.Bytes())), decoded, mp))
	require.Equal(t, vector.PrepareParamFloat, decoded.Vecs[0].GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamInteger, decoded.Vecs[0].GetPrepareParamKindAt(1))
	decoded.Clean(mp)

	truncated := append([]byte(nil), preparedRecord.Bytes()[:preparedRecord.Len()-1]...)
	decoded = batch.NewWithSize(0)
	require.Error(t, unmarshalSpillGroupByBatch(
		bufio.NewReader(bytes.NewReader(truncated)), decoded, mp))
	decoded.Clean(mp)

	invalid := append([]byte(nil), preparedRecord.Bytes()...)
	trailer := bytes.Index(invalid, []byte{'P', 'P', 'B'})
	require.Positive(t, trailer)
	invalid[trailer] = 'X'
	decoded = batch.NewWithSize(0)
	require.Error(t, unmarshalSpillGroupByBatch(
		bufio.NewReader(bytes.NewReader(invalid)), decoded, mp))
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
			_, _, err := readPrepareParamKindTrailer(
				context.Background(), bytes.NewReader(tc.payload), 1, &localStates, []int{1})
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
	_, _, err := readPrepareParamKindTrailer(
		context.Background(), bufio.NewReader(bytes.NewReader(payload)), 1, &states, []int{1})
	require.ErrorContains(t, err, "row count 16777216 does not match 1")
}

func TestPrepareParamKindTrailerRejectsRowsWithoutStateBound(t *testing.T) {
	states := aggexec.PrepareParamKindStates{}
	states.Reset([]aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
	})
	payload := prepareParamKindRowsTrailerForTest(1<<24, nil)
	_, _, err := readPrepareParamKindTrailer(
		context.Background(), bufio.NewReader(bytes.NewReader(payload)), 1, &states, []int{-1})
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

func TestPrepareParamKindTrailerCarriesBinaryStringState(t *testing.T) {
	aggs := []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, nil, nil),
	}
	var states aggexec.PrepareParamKindStates
	states.Reset(aggs)
	var payload bytes.Buffer
	require.NoError(t, writePrepareParamKindTrailer(
		context.Background(), &payload, aggs, &states, nil,
		[]prepareParamKindSummary{{binaryString: true}},
	))
	require.Equal(t, prepareParamKindTrailerBinaryVersion, payload.Bytes()[3])

	_, summaries, err := readPrepareParamKindTrailer(
		context.Background(), bytes.NewReader(payload.Bytes()), 1, &states, []int{-1})
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	require.True(t, summaries[0].binaryString)
}
