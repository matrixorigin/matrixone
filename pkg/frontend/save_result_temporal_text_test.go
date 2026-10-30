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

package frontend

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

// TestConvertRowsIntoBatchTemporalText covers #31547 and #31550: DATA BRANCH
// DIFF renders temporal values as text. Saving that result must parse
// DATE/TIME/DATETIME text instead of panicking on a type assertion, keep the
// fractional-second precision the column declares, and read TIMESTAMP text in
// the session time zone rather than the process zone.
func TestConvertRowsIntoBatchTemporalText(t *testing.T) {
	newCol := func(typ defines.MysqlType, decimal int32) *MysqlColumn {
		col := new(MysqlColumn)
		col.SetColumnType(typ)
		col.SetName("c")
		col.SetDecimal(decimal)
		return col
	}
	mrs := &MysqlResultSet{}
	mrs.AddColumn(newCol(defines.MYSQL_TYPE_DATE, 0))
	mrs.AddColumn(newCol(defines.MYSQL_TYPE_TIME, 6))
	mrs.AddColumn(newCol(defines.MYSQL_TYPE_DATETIME, 6))
	mrs.AddColumn(newCol(defines.MYSQL_TYPE_TIMESTAMP, 6))
	mrs.AddColumn(newCol(defines.MYSQL_TYPE_TIMESTAMP, 3))
	mrs.AddRow([]any{"2024-01-02", "04:05:06.654321", "2024-01-02 00:00:00.654321",
		"2024-01-01 00:00:00.123456", "2024-01-01 00:00:00.999"})
	mrs.AddRow([]any{nil, nil, nil, nil, nil})

	session := time.FixedZone("session", 0)
	pool, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	// Use a process-local zone that differs from the session zone.
	oldLocal := time.Local
	time.Local = time.FixedZone("process", 8*3600)
	defer func() { time.Local = oldLocal }()

	data, colDefs, err := convertRowsIntoBatchInLocation(pool, mrs.Columns, mrs.Data, session)
	require.NoError(t, err)
	defer data.Clean(pool)

	require.Equal(t, int32(6), colDefs.ResultCols[1].Typ.Scale)
	require.Equal(t, int32(6), colDefs.ResultCols[2].Typ.Scale)
	require.Equal(t, int32(6), colDefs.ResultCols[3].Typ.Scale)
	require.Equal(t, int32(3), colDefs.ResultCols[4].Typ.Scale)

	require.Equal(t, "2024-01-02", vector.GetFixedAtNoTypeCheck[types.Date](data.Vecs[0], 0).String())
	require.Equal(t, "04:05:06.654321", vector.GetFixedAtNoTypeCheck[types.Time](data.Vecs[1], 0).String2(6))
	require.Equal(t, "2024-01-02 00:00:00.654321", vector.GetFixedAtNoTypeCheck[types.Datetime](data.Vecs[2], 0).String2(6))
	require.Equal(t, "2024-01-01 00:00:00.123456",
		vector.GetFixedAtNoTypeCheck[types.Timestamp](data.Vecs[3], 0).String2(session, 6))
	require.Equal(t, "2024-01-01 00:00:00.999",
		vector.GetFixedAtNoTypeCheck[types.Timestamp](data.Vecs[4], 0).String2(session, 3))
	for i := range data.Vecs {
		require.True(t, data.Vecs[i].GetNulls().Contains(1), "column %d", i)
	}

	// Values that are not temporal text report an error instead of panicking.
	bad := &MysqlResultSet{}
	bad.AddColumn(newCol(defines.MYSQL_TYPE_DATETIME, 6))
	bad.AddRow([]any{int64(1)})
	_, _, err = convertRowsIntoBatchInLocation(pool, bad.Columns, bad.Data, session)
	require.Error(t, err)
}
