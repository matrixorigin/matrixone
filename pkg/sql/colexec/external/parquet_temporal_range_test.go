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

package external

import (
	"bytes"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// Numeric Parquet DATE/TIMESTAMP values beyond year 9999 must be rejected
// like SQL assignment does, not persisted (issue #31519).
func TestParquetTemporalRejectsOutOfRangeYears(t *testing.T) {
	proc := testutil.NewProc(t)
	previousZone := proc.Base.SessionInfo.TimeZone
	t.Cleanup(func() { proc.Base.SessionInfo.TimeZone = previousZone })
	proc.Base.SessionInfo.TimeZone = time.UTC

	const (
		days9999    = 2932896            // 9999-12-31
		days10000   = 2932897            // 10000-01-01
		usMax       = 253402300799999999 // 9999-12-31 23:59:59.999999 UTC
		us10000     = 253402300800000000 // 10000-01-01 00:00:00 UTC
		usEpochPlus = 1_000_000
	)
	run := func(t *testing.T, node parquet.Node, v parquet.Value, target types.T) error {
		var buf bytes.Buffer
		w := parquet.NewWriter(&buf, parquet.NewSchema("x", parquet.Group{"c": node}))
		_, err := w.WriteRows([]parquet.Row{{v.Level(0, 0, 0)}})
		require.NoError(t, err)
		require.NoError(t, w.Close())
		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)
		vec := vector.NewVec(types.New(target, 0, 6))
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(target), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		return mp.mapping(page, proc, vec)
	}
	date := parquet.Date()
	ts := parquet.Timestamp(parquet.Microsecond)
	for _, tc := range []struct {
		name   string
		node   parquet.Node
		v      parquet.Value
		target types.T
		ok     bool
	}{
		{"date-9999", date, parquet.Int32Value(days9999), types.T_date, true},
		{"date-10000", date, parquet.Int32Value(days10000), types.T_date, false},
		{"date-to-datetime-9999", date, parquet.Int32Value(days9999), types.T_datetime, true},
		{"date-to-datetime-10000", date, parquet.Int32Value(days10000), types.T_datetime, false},
		{"ts-to-timestamp-epoch", ts, parquet.Int64Value(usEpochPlus), types.T_timestamp, true},
		{"ts-to-timestamp-max", ts, parquet.Int64Value(usMax), types.T_timestamp, true},
		{"ts-to-timestamp-10000", ts, parquet.Int64Value(us10000), types.T_timestamp, false},
		{"ts-to-datetime-max", ts, parquet.Int64Value(usMax), types.T_datetime, true},
		{"ts-to-datetime-10000", ts, parquet.Int64Value(us10000), types.T_datetime, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := run(t, tc.node, tc.v, tc.target)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "out of range")
			}
		})
	}
}
