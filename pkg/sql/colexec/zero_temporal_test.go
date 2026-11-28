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

package colexec

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func TestCheckZeroTemporalVector(t *testing.T) {
	mp := mpool.MustNewZero()
	ctx := context.Background()

	// fixed builds a vector of vals; a nil entry is a NULL row.
	fixed := func(oid types.T, vals ...any) *vector.Vector {
		v := vector.NewVec(oid.ToType(), mp)
		for _, x := range vals {
			var err error
			switch oid {
			case types.T_date:
				d, _ := x.(types.Date)
				err = vector.AppendFixed(v, d, x == nil, mp)
			case types.T_datetime:
				d, _ := x.(types.Datetime)
				err = vector.AppendFixed(v, d, x == nil, mp)
			case types.T_timestamp:
				d, _ := x.(types.Timestamp)
				err = vector.AppendFixed(v, d, x == nil, mp)
			case types.T_int64:
				d, _ := x.(int64)
				err = vector.AppendFixed(v, d, x == nil, mp)
			}
			require.NoError(t, err)
		}
		return v
	}
	okDate := types.Date(1000)
	okDatetime := types.Datetime(1 << 40)
	okTimestamp := types.TimestampMinValue
	belowMin := types.TimestampMinValue - 1

	for _, tc := range []struct {
		name    string
		vec     func() *vector.Vector
		reject  bool
		wantRow int // 1-based row named by the error; 0 means no error
	}{
		{"zero date rejected", func() *vector.Vector { return fixed(types.T_date, okDate, types.ZeroDate) }, true, 2},
		{"zero date allowed", func() *vector.Vector { return fixed(types.T_date, okDate, types.ZeroDate) }, false, 0},
		{"null date row skipped", func() *vector.Vector { return fixed(types.T_date, nil, okDate) }, true, 0},
		{"zero datetime rejected", func() *vector.Vector { return fixed(types.T_datetime, types.ZeroDatetime) }, true, 1},
		{"zero datetime allowed", func() *vector.Vector { return fixed(types.T_datetime, okDatetime, types.ZeroDatetime) }, false, 0},
		{"zero timestamp rejected", func() *vector.Vector { return fixed(types.T_timestamp, okTimestamp, nil, types.ZeroTimestamp) }, true, 3},
		{"zero timestamp allowed", func() *vector.Vector { return fixed(types.T_timestamp, types.ZeroTimestamp) }, false, 0},
		{"timestamp below minimum always rejected", func() *vector.Vector { return fixed(types.T_timestamp, okTimestamp, belowMin) }, false, 2},
		{"other types not checked", func() *vector.Vector { return fixed(types.T_int64, int64(0)) }, true, 0},
		{"const zero date names first row", func() *vector.Vector {
			v, err := vector.NewConstFixed(types.T_date.ToType(), types.ZeroDate, 3, mp)
			require.NoError(t, err)
			return v
		}, true, 1},
		{"const null date passes", func() *vector.Vector {
			return vector.NewConstNull(types.T_date.ToType(), 3, mp)
		}, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.vec()
			defer v.Free(mp)
			err := CheckZeroTemporalVector(ctx, v, tc.reject)
			if tc.wantRow == 0 {
				require.NoError(t, err)
				return
			}
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTruncatedWrongValueForField), "got %v", err)
			require.True(t, strings.HasSuffix(err.Error(), fmt.Sprintf(", %d", tc.wantRow)), "got %v", err)
		})
	}
	require.Zero(t, mp.CurrNB())
}
