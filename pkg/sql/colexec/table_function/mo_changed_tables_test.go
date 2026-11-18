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

package table_function

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/changedtables"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMoChangedTablesBoundedOutput(t *testing.T) {
	proc := testutil.NewProcess(t)
	schema := []types.Type{
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint32.ToType(), types.T_varchar.ToType(),
		types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(),
		types.New(types.T_text, types.MaxMediumTextLen, 0), types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
	}
	tf := &TableFunction{Attrs: make([]string, len(schema))}
	tf.ctr.retSchema = schema
	s := &moChangedTablesState{}
	defer s.free(tf, proc, false, nil)

	// An encoded marker for 5,000 20-byte source entries exceeds TEXT.
	marker := strings.Repeat("A", 133_440)
	rows := make([]changedtables.Row, changedTablesBatchRows+1)
	for i := range rows {
		rows[i] = changedtables.Row{Kind: "BASELINE", AccountID: 1, AccountName: "tenant", DatabaseName: "db", TableName: "t"}
	}
	rows[len(rows)-1].Kind = "CHANGED"
	rows[len(rows)-1].TableID = 7
	from := timestamp.Timestamp{PhysicalTime: 1234, LogicalTime: 4}
	to := timestamp.Timestamp{PhysicalTime: 1234, LogicalTime: 5}
	require.NoError(t, s.admitResult(proc, changedtables.Result{Rows: rows, Marker: marker, From: &from, To: to, SourceCount: 1}))

	result, err := s.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, changedTablesBatchRows, result.Batch.RowCount())
	require.Equal(t, "BASELINE", result.Batch.Vecs[0].GetStringAt(0))
	require.True(t, result.Batch.Vecs[6].IsNull(0))
	require.True(t, result.Batch.Vecs[8].IsNull(0))

	result, err = s.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, "CHANGED", result.Batch.Vecs[0].GetStringAt(0))
	require.Equal(t, uint64(7), vector.GetFixedAtNoTypeCheck[uint64](result.Batch.Vecs[6], 0))

	result, err = s.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())
	cols := result.Batch.Vecs
	require.Equal(t, "END", cols[0].GetStringAt(0))
	require.True(t, cols[2].IsNull(0))
	require.Equal(t, marker, cols[8].GetStringAt(0))
	for i, count := range []uint64{1, changedTablesBatchRows, 1} {
		require.Equal(t, count, vector.GetFixedAtNoTypeCheck[uint64](cols[11+i], 0))
	}
	require.Equal(t, "1234-4", cols[9].GetStringAt(0))
	require.Equal(t, "1234-5", cols[10].GetStringAt(0))

	result, err = s.call(tf, proc)
	require.NoError(t, err)
	require.True(t, result.Batch.IsDone())
	s.reset(tf, proc)
	require.NoError(t, s.admitResult(proc, changedtables.Result{Marker: marker, To: to}))
	result, err = s.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, "END", result.Batch.Vecs[0].GetStringAt(0))
	require.True(t, result.Batch.Vecs[9].IsNull(0))
	require.Zero(t, vector.GetFixedAtNoTypeCheck[uint64](result.Batch.Vecs[12], 0))
}
