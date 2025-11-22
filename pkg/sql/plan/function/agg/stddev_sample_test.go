// Copyright 2024 Matrix Origin
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

package agg

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestAggStdDevSampleFlush(t *testing.T) {
	tests := []struct {
		name    string
		sum     float64
		count   int64
		sumPow2 float64
		want    float64
		wantErr bool
	}{
		{
			name:    "single value",
			sum:     1.0,
			count:   1,
			sumPow2: 1.0,
			want:    0.0, // When count <= 1, var_sample = 0, so stddev_sample = sqrt(0) = 0
			wantErr: false,
		},
		{
			name:    "two values",
			sum:     3.0,
			count:   2,
			sumPow2: 5.0,                // 1^2 + 2^2 = 1 + 4 = 5
			want:    0.7071067811865476, // sqrt((5/2 - (3/2)^2) * 2/1) = sqrt(0.5) ≈ 0.707
			wantErr: false,
		},
		{
			name:    "three values 1,2,3",
			sum:     6.0,
			count:   3,
			sumPow2: 14.0, // 1^2 + 2^2 + 3^2 = 1 + 4 + 9 = 14
			want:    1.0,  // sqrt((14/3 - (6/3)^2) * 3/2) = sqrt(1) = 1.0
			wantErr: false,
		},
		{
			name:    "zero count",
			sum:     0.0,
			count:   0,
			sumPow2: 0.0,
			want:    0.0,
			wantErr: false,
		},
		{
			name:    "negative values",
			sum:     -6.0,
			count:   3,
			sumPow2: 14.0, // (-1)^2 + (-2)^2 + (-3)^2 = 1 + 4 + 9 = 14
			want:    1.0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupCtx := generateAggVarSampleGroupContext(types.T_float64.ToType())
			ctx := groupCtx.(*aggVarSampleGroupContext)
			ctx.sum = tt.sum
			ctx.count = tt.count

			// Use a getter/setter that tracks the value
			var currentValue float64 = tt.sumPow2
			getter := func() float64 {
				return currentValue
			}
			setter := func(v float64) {
				currentValue = v
			}

			err := aggStdDevSampleFlush(
				groupCtx,
				nil,
				getter,
				setter,
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.InDelta(t, tt.want, currentValue, 1e-10)
			}
		})
	}
}

func TestAggStdDevSampleFlushWithVarSample(t *testing.T) {
	// Test that stddev_sample = sqrt(var_sample)
	tests := []struct {
		name       string
		sum        float64
		count      int64
		sumPow2    float64
		wantVar    float64
		wantStdDev float64
		wantErr    bool
	}{
		{
			name:       "three values 1,2,3",
			sum:        6.0,
			count:      3,
			sumPow2:    14.0,
			wantVar:    1.0, // var_samp = (14/3 - (6/3)^2) * 3/2 = 1.0
			wantStdDev: 1.0, // sqrt(1.0) = 1.0
			wantErr:    false,
		},
		{
			name:       "five values 1,2,3,4,5",
			sum:        15.0,
			count:      5,
			sumPow2:    55.0,           // 1^2 + 2^2 + 3^2 + 4^2 + 5^2 = 55
			wantVar:    2.5,            // var_samp = (55/5 - (15/5)^2) * 5/4 = 2.5
			wantStdDev: math.Sqrt(2.5), // sqrt(2.5) ≈ 1.581
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupCtx := generateAggVarSampleGroupContext(types.T_float64.ToType())
			ctx := groupCtx.(*aggVarSampleGroupContext)
			ctx.sum = tt.sum
			ctx.count = tt.count

			// First calculate var_sample
			var varSampleValue float64 = tt.sumPow2
			getter1 := func() float64 { return varSampleValue }
			setter1 := func(v float64) { varSampleValue = v }
			err := aggVarSampleFlush(groupCtx, nil, getter1, setter1)
			require.NoError(t, err)
			require.InDelta(t, tt.wantVar, varSampleValue, 1e-10)

			// Reset context for stddev_sample test
			groupCtx2 := generateAggVarSampleGroupContext(types.T_float64.ToType())
			ctx2 := groupCtx2.(*aggVarSampleGroupContext)
			ctx2.sum = tt.sum
			ctx2.count = tt.count

			// Then calculate stddev_sample (should be sqrt of var_sample)
			var stdDevSampleValue float64 = tt.sumPow2
			getter2 := func() float64 { return stdDevSampleValue }
			setter2 := func(v float64) { stdDevSampleValue = v }
			err = aggStdDevSampleFlush(groupCtx2, nil, getter2, setter2)
			require.NoError(t, err)
			require.InDelta(t, tt.wantStdDev, stdDevSampleValue, 1e-10)
			require.InDelta(t, math.Sqrt(varSampleValue), stdDevSampleValue, 1e-10)
		})
	}
}

type mockFloat64Getter struct {
	value float64
}

func (m *mockFloat64Getter) Get() float64 {
	return m.value
}

type mockFloat64Setter struct {
	result float64
}

func (m *mockFloat64Setter) Set(value float64) {
	m.result = value
}
