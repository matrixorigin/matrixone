// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestGroupConcatFinalizationAfterNamedTimeZoneDecode(t *testing.T) {
	for _, test := range []struct{ name, want string }{
		{"Asia/Shanghai", "2024-01-01 08:00:00,2024-07-01 08:00:00"},
		{"America/New_York", "2023-12-31 19:00:00,2024-06-30 20:00:00"}} {
		t.Run(test.name, func(t *testing.T) {
			loc, err := time.LoadLocation(test.name)
			require.NoError(t, err)
			legacy, err := time.Time{}.In(loc).MarshalBinary()
			require.NoError(t, err)
			session, err := process.ConvertToProcessSessionInfo(pipeline.SessionInfo{TimeZone: legacy, TimeZoneName: process.TimeZoneLocationName(loc)})
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			typ := types.New(types.T_timestamp, 0, 0)
			agg, err := MakeAgg(mp, AggIdOfGroupConcat, false, typ)
			require.NoError(t, err)
			defer agg.Free()
			ConfigureGroupConcatTimeZone(agg, session.TimeZone)
			require.NoError(t, agg.GroupGrow(1))
			input := vector.NewVec(typ)
			defer input.Free(mp)
			for _, date := range []string{"2024-01-01 00:00:00", "2024-07-01 00:00:00"} {
				v, err := types.ParseTimestamp(time.UTC, date, 0)
				require.NoError(t, err)
				require.NoError(t, vector.AppendFixed(input, v, false, mp))
			}
			require.NoError(t, agg.BatchFill(0, []uint64{1, 1}, []*vector.Vector{input}))
			result, err := FlushWithContext(context.Background(), agg)
			require.NoError(t, err)
			require.Len(t, result, 1)
			defer result[0].Free(mp)
			require.Equal(t, test.want, string(result[0].GetBytesAt(0)))
		})
	}
}
