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

func TestRowMetadataTrailerWireUnknownServiceFailsClosed(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.LockService = &unknownGroupServiceLockService{
		cfg: lockservice.Config{ServiceID: "group-unknown-service"},
	}

	var enabled bool
	require.NotPanics(t, func() {
		enabled = rowMetadataTrailerWireEnabled(proc)
	})
	require.False(t, enabled)

	var nilEnabled bool
	require.NotPanics(t, func() {
		nilEnabled = rowMetadataTrailerWireEnabled((*process.Process)(nil))
	})
	require.False(t, nilEnabled)
}

func TestGroupSpillGroupKeyCodec(t *testing.T) {
	mp := mpool.MustNewZero()
	gb := batch.NewWithSize(1)
	gb.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	require.NoError(t, vector.AppendBytes(gb.Vecs[0], []byte("5"), false, mp))
	require.NoError(t, vector.AppendBytes(gb.Vecs[0], []byte("5"), false, mp))
	gb.SetRowCount(2)
	defer func() {
		gb.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()

	rows := []int32{0, 1}
	newDestination := func() *batch.Batch {
		decoded := batch.NewWithSize(1)
		decoded.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
		return decoded
	}
	var record bytes.Buffer
	require.NoError(t, appendSpillGroupByRows(&record, gb, rows))
	decoded := newDestination()
	require.NoError(t, unmarshalSpillGroupByRows(
		bytes.NewReader(record.Bytes()), decoded, len(rows), mp))
	require.Equal(t, 2, decoded.Vecs[0].Length())
	decoded.Clean(mp)

	truncated := append([]byte(nil), record.Bytes()[:record.Len()-1]...)
	decoded = newDestination()
	require.Error(t, unmarshalSpillGroupByRows(
		bytes.NewReader(truncated), decoded, len(rows), mp))
	decoded.Clean(mp)

	invalid := append([]byte(nil), record.Bytes()...)
	// column count (4 bytes), followed by selected row count (4 bytes), then
	// the selected-vector metadata byte.
	require.Greater(t, len(invalid), 8)
	invalid[8] |= 0xc0 // source mode 3 is reserved
	decoded = newDestination()
	require.ErrorContains(t, unmarshalSpillGroupByRows(
		bytes.NewReader(invalid), decoded, len(rows), mp), "metadata")
	decoded.Clean(mp)
}

func rowMetadataTrailerHeaderForTest(buf *bytes.Buffer, version byte, nAggs int32) {
	buf.Write([]byte{rowMetadataTrailerMagic0, rowMetadataTrailerMagic1,
		rowMetadataTrailerMagic2, version})
	buf.Write(types.EncodeInt32(&nAggs))
}

func rowMetadataRowsTrailerForTest(rowCount int32, rows []byte) []byte {
	var buf bytes.Buffer
	rowMetadataTrailerHeaderForTest(&buf, rowMetadataTrailerRowsVersion, 1)
	buf.WriteByte(rowMetadataTrailerRowsMarker)
	buf.Write(types.EncodeInt32(&rowCount))
	buf.Write(rows)
	return buf.Bytes()
}

func TestRowMetadataTrailerAcceptsEveryLegacyVersion(t *testing.T) {
	ctx := context.Background()
	// v1: one retired summary byte per aggregate, all legal values.
	for encoded := byte(0); encoded <= rowMetadataTrailerMaxRetired+1; encoded++ {
		var buf bytes.Buffer
		rowMetadataTrailerHeaderForTest(&buf, rowMetadataTrailerVersion, 1)
		buf.WriteByte(encoded)
		reader := bytes.NewReader(buf.Bytes())
		require.NoError(t, readRowMetadataTrailer(ctx, reader, []int{3}, false, false))
		require.Zero(t, reader.Len())
	}
	// v2: per-row retired values 0..4.
	reader := bytes.NewReader(rowMetadataRowsTrailerForTest(5, []byte{0, 1, 2, 3, 4}))
	require.NoError(t, readRowMetadataTrailer(ctx, reader, []int{5}, false, false))
	require.Zero(t, reader.Len())
	// v3: summary + binary flag, and a per-row section with binary bits.
	var v3 bytes.Buffer
	rowMetadataTrailerHeaderForTest(&v3, rowMetadataTrailerBinaryVersion, 2)
	v3.Write([]byte{3, 1})
	v3.WriteByte(rowMetadataTrailerRowsMarker)
	rows := int32(2)
	v3.Write(types.EncodeInt32(&rows))
	v3.Write([]byte{rowMetadataTrailerRowsMarker | 2, 4})
	reader = bytes.NewReader(v3.Bytes())
	require.NoError(t, readRowMetadataTrailer(ctx, reader, []int{1, 2}, true, false))
	require.Zero(t, reader.Len())
}

func TestRowMetadataTrailerRejectsMalformedInput(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{
			name:    "zero rows marker",
			payload: rowMetadataRowsTrailerForTest(0, nil),
			wantErr: "invalid aggregate row metadata row count 0",
		},
		{
			name:    "amplified rows mismatch",
			payload: rowMetadataRowsTrailerForTest(1<<24, nil),
			wantErr: "row count 16777216 does not match 1",
		},
		{
			name:    "truncated rows",
			payload: rowMetadataRowsTrailerForTest(1, nil),
			wantErr: "unexpected EOF",
		},
		{
			name:    "invalid row value",
			payload: rowMetadataRowsTrailerForTest(1, []byte{5}),
			wantErr: "invalid aggregate row metadata byte 5",
		},
		{
			name: "invalid summary value",
			payload: func() []byte {
				var buf bytes.Buffer
				rowMetadataTrailerHeaderForTest(&buf, rowMetadataTrailerVersion, 1)
				buf.WriteByte(6)
				return buf.Bytes()
			}(),
			wantErr: "invalid aggregate row metadata state 6",
		},
		{
			name: "bad magic",
			payload: func() []byte {
				payload := rowMetadataRowsTrailerForTest(1, []byte{0})
				payload[2] = 'X'
				return payload
			}(),
			wantErr: "invalid aggregate row metadata trailer",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := readRowMetadataTrailer(
				ctx, bytes.NewReader(tc.payload), []int{1}, true, true)
			require.ErrorContains(t, err, tc.wantErr)
			err = readRowMetadataTrailer(
				ctx, bufio.NewReader(bytes.NewReader(tc.payload)), []int{1}, true, true)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
	err := readRowMetadataTrailer(ctx,
		bytes.NewReader(rowMetadataRowsTrailerForTest(1, []byte{0})), []int{1, 1}, true, true)
	require.ErrorContains(t, err, "count 1 does not match 2")
}

func TestRowMetadataTrailerDomainRequiresCapabilities(t *testing.T) {
	var encoded bytes.Buffer
	rowMetadataTrailerHeaderForTest(&encoded, rowMetadataTrailerDomainVersion, 1)
	encoded.WriteByte(0)
	encoded.WriteByte(byte(types.RuntimeStringText))
	err := readRowMetadataTrailer(context.Background(),
		bytes.NewReader(encoded.Bytes()), []int{1}, true, false)
	require.ErrorContains(t, err, "MORPCVersion23")
	err = readRowMetadataTrailer(context.Background(),
		bytes.NewReader(encoded.Bytes()), []int{1}, false, true)
	require.ErrorContains(t, err, "MORPCVersion18")
	require.NoError(t, readRowMetadataTrailer(context.Background(),
		bytes.NewReader(encoded.Bytes()), []int{1}, true, true))
}

// An older writer appended the row-metadata trailer, including retired
// per-aggregate (uniform) or per-row category data, after the aggregate state.
// Current readers must accept and discard it and still restore the aggregate
// state's values.
func TestOldRowMetadataTrailerWithRetiredKindsDecodes(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, vector.AppendBytes(input, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(input, []byte("b"), false, mp))
	source, err := aggexec.MakeGroupAgg(
		mp, aggexec.AggIdOfAny, false, nil, nil, types.T_varchar.ToType())
	require.NoError(t, err)
	require.NoError(t, source.GroupGrow(2))
	require.NoError(t, source.BatchFill(0, []uint64{1, 2}, []*vector.Vector{input}))
	var state bytes.Buffer
	require.NoError(t, source.SaveIntermediateResultOfChunk(0, &state))

	var uniform bytes.Buffer
	rowMetadataTrailerHeaderForTest(&uniform, rowMetadataTrailerDomainVersion, 1)
	uniform.WriteByte(3) // observed retired Float (2), shifted by one
	uniform.WriteByte(byte(types.RuntimeStringBinary))

	var perRow bytes.Buffer
	rowMetadataTrailerHeaderForTest(&perRow, rowMetadataTrailerDomainVersion, 1)
	perRow.WriteByte(rowMetadataTrailerRowsMarker)
	rows := int32(2)
	perRow.Write(types.EncodeInt32(&rows))
	perRow.Write([]byte{
		rowMetadataTrailerRowsMarker | 1, // binary, retired Integer
		rowMetadataTrailerTextMarker | 4, // text, retired Boolean
	})

	for name, trailer := range map[string][]byte{
		"uniform": uniform.Bytes(), "per-row": perRow.Bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			payload := append(append([]byte(nil), state.Bytes()...), trailer...)
			reader := bytes.NewReader(payload)
			restored, err := aggexec.MakeGroupAgg(
				mp, aggexec.AggIdOfAny, false, nil, nil, types.T_varchar.ToType())
			require.NoError(t, err)
			defer restored.Free()
			require.NoError(t, restored.UnmarshalFromReader(reader, mp))
			require.NoError(t, readRowMetadataTrailer(context.Background(), reader,
				[]int{restored.GetNumGroups()}, true, true))
			require.Zero(t, reader.Len())

			results, err := restored.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, "a", results[0].GetStringAt(0))
			require.Equal(t, "b", results[0].GetStringAt(1))
			results[0].Free(mp)
		})
	}
	source.Free()
	input.Free(mp)
	require.Zero(t, mp.CurrNB())
}
