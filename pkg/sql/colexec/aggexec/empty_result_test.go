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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetEmptyResultKind(t *testing.T) {
	for _, tt := range []struct {
		name string
		ids  []int64
		want EmptyResultKind
	}{
		{
			name: "null",
			ids: []int64{
				AggIdOfAny, AggIdOfAvg, AggIdOfGroupConcat, AggIdOfJsonArrayAgg,
				AggIdOfJsonObjectAgg, AggIdOfMax, AggIdOfMaxBy, AggIdOfMaxByNonNull,
				AggIdOfMedian, AggIdOfMin, AggIdOfApproxPercentile, AggIdOfStdDevPop,
				AggIdOfStdDevSample, AggIdOfSum, AggIdOfVarPop, AggIdOfVarSample,
			},
			want: EmptyResultNull,
		},
		{
			name: "zero",
			ids: []int64{
				AggIdOfCountColumn, AggIdOfCountStar, AggIdOfApproxCount,
				AggIdOfApproxCountDistinct, AggIdOfBitOr, AggIdOfBitXor,
			},
			want: EmptyResultZero,
		},
		{name: "all bits set", ids: []int64{AggIdOfBitAnd}, want: EmptyResultAllBitsSet},
		{
			name: "unsupported",
			ids: []int64{
				AggIdOfAvgTwCache, AggIdOfAvgTwResult, AggIdOfBitmapConstruct,
				AggIdOfBitmapOr, AggIdOfHllAdd, AggIdOfHllMerge,
			},
			want: EmptyResultUnsupported,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, id := range tt.ids {
				require.Equal(t, tt.want, GetEmptyResultKind(id), "aggregate ID %d", id)
			}
		})
	}

	require.Equal(t, EmptyResultUnsupported, GetEmptyResultKind(-1))
}
