// Copyright 2022 Matrix Origin
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

package plan

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

// Keep exact microsecond boundaries without binary multiplication artifacts;
// discard fractional microseconds instead of rounding them.
func TestResetDateFunctionArgsDecimalIntervalRounding(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value float64
		unit  string
		want  int64
	}{
		{"1.000009 SECOND - critical precision case", 1.000009, "SECOND", 1000009},
		{"1.1 SECOND", 1.1, "SECOND", 1100000},
		{"0.000001 SECOND - 1 microsecond", 0.000001, "SECOND", 1},
		{"1.999999 SECOND - near 2 seconds", 1.999999, "SECOND", 1999999},
		{"1.5 MINUTE", 1.5, "MINUTE", 90000000},
		{"0.5 HOUR", 0.5, "HOUR", 1800000000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args, err := resetDateFunctionArgs(context.Background(), makeDatetimeConst("2000-01-01 01:00:00"), makeIntervalExpr(makeFloat64Const(tc.value), tc.unit))
			require.NoError(t, err)
			require.Equal(t, tc.want, foldedIntervalInt64(t, args[1]))
			require.Equal(t, int64(types.MicroSecond), args[2].GetLit().GetI64Val())
		})
	}
}

func TestResetDateFunctionArgsDecimalIntervalRoundingEdgeCases(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value float64
		want  int64
	}{
		{"0.9999995 SECOND - exactly halfway", 0.9999995, 999999},
		{"0.9999994 SECOND - just below halfway", 0.9999994, 999999},
		{"0.0000005 SECOND - very small value", 0.0000005, 0},
		{"negative submicrosecond", -0.0000009, 0},
		{"below exact microsecond", math.Nextafter(1.000001, 0), 1000000},
		{"above exact microsecond", math.Nextafter(1.000001, 2), 1000001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args, err := resetDateFunctionArgs(context.Background(), makeDatetimeConst("2000-01-01 01:00:00"), makeIntervalExpr(makeFloat64Const(tc.value), "SECOND"))
			require.NoError(t, err)
			require.Equal(t, tc.want, foldedIntervalInt64(t, args[1]))
		})
	}
}
