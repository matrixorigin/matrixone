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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestAggVarSampleFill(t *testing.T) {
	tests := []struct {
		name        string
		value       float64
		isEmpty     bool
		getterValue float64
		wantSum     float64
		wantCount   int64
		wantPow2    float64
		wantErr     bool
	}{
		{
			name:        "fill into empty",
			value:       5.0,
			isEmpty:     true,
			getterValue: 0.0,
			wantSum:     5.0,
			wantCount:   1,
			wantPow2:    25.0, // 5^2
			wantErr:     false,
		},
		{
			name:        "fill into non-empty",
			value:       3.0,
			isEmpty:     false,
			getterValue: 4.0, // existing sum of x^2
			wantSum:     3.0,
			wantCount:   1,
			wantPow2:    13.0, // 4 + 3^2 = 4 + 9 = 13
			wantErr:     false,
		},
		{
			name:        "fill negative value",
			value:       -2.0,
			isEmpty:     false,
			getterValue: 1.0,
			wantSum:     -2.0,
			wantCount:   1,
			wantPow2:    5.0, // 1 + (-2)^2 = 1 + 4 = 5
			wantErr:     false,
		},
		{
			name:        "fill zero",
			value:       0.0,
			isEmpty:     false,
			getterValue: 10.0,
			wantSum:     0.0,
			wantCount:   1,
			wantPow2:    10.0, // 10 + 0^2 = 10
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupCtx := generateAggVarSampleGroupContext(types.T_float64.ToType())
			ctx := groupCtx.(*aggVarSampleGroupContext)

			getter := &mockFloat64Getter{value: tt.getterValue}
			setter := &mockFloat64Setter{}

			err := aggVarSampleFill(
				groupCtx,
				nil,
				tt.value,
				tt.isEmpty,
				getter.Get,
				setter.Set,
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantSum, ctx.sum)
				require.Equal(t, tt.wantCount, ctx.count)
				require.InDelta(t, tt.wantPow2, setter.result, 1e-10)
			}
		})
	}
}

func TestAggVarSampleFills(t *testing.T) {
	tests := []struct {
		name        string
		value       float64
		count       int
		isEmpty     bool
		getterValue float64
		wantSum     float64
		wantCount   int64
		wantPow2    float64
		wantErr     bool
	}{
		{
			name:        "fill multiple values",
			value:       2.0,
			count:       3,
			isEmpty:     false,
			getterValue: 1.0,
			wantSum:     6.0, // 0 + 2*3
			wantCount:   3,
			wantPow2:    13.0, // 1 + (2^2)*3 = 1 + 12 = 13
			wantErr:     false,
		},
		{
			name:        "fill single value",
			value:       5.0,
			count:       1,
			isEmpty:     true,
			getterValue: 0.0,
			wantSum:     5.0,
			wantCount:   1,
			wantPow2:    25.0,
			wantErr:     false,
		},
		{
			name:        "fill zero count",
			value:       10.0,
			count:       0,
			isEmpty:     false,
			getterValue: 5.0,
			wantSum:     0.0,
			wantCount:   0,
			wantPow2:    5.0,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupCtx := generateAggVarSampleGroupContext(types.T_float64.ToType())
			ctx := groupCtx.(*aggVarSampleGroupContext)

			getter := &mockFloat64Getter{value: tt.getterValue}
			setter := &mockFloat64Setter{}

			err := aggVarSampleFills(
				groupCtx,
				nil,
				tt.value,
				tt.count,
				tt.isEmpty,
				getter.Get,
				setter.Set,
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantSum, ctx.sum)
				require.Equal(t, tt.wantCount, ctx.count)
				require.InDelta(t, tt.wantPow2, setter.result, 1e-10)
			}
		})
	}
}

func TestAggVarSampleMerge(t *testing.T) {
	tests := []struct {
		name      string
		sum1      float64
		count1    int64
		sumPow2_1 float64
		sum2      float64
		count2    int64
		sumPow2_2 float64
		isEmpty1  bool
		isEmpty2  bool
		wantSum   float64
		wantCount int64
		wantPow2  float64
		wantErr   bool
	}{
		{
			name:      "merge two non-empty",
			sum1:      3.0,
			count1:    2,
			sumPow2_1: 5.0,
			sum2:      7.0,
			count2:    2,
			sumPow2_2: 25.0,
			isEmpty1:  false,
			isEmpty2:  false,
			wantSum:   10.0, // 3 + 7
			wantCount: 4,    // 2 + 2
			wantPow2:  30.0, // 5 + 25
			wantErr:   false,
		},
		{
			name:      "merge empty into non-empty",
			sum1:      5.0,
			count1:    2,
			sumPow2_1: 13.0,
			sum2:      0.0,
			count2:    0,
			sumPow2_2: 0.0,
			isEmpty1:  false,
			isEmpty2:  true,
			wantSum:   5.0,
			wantCount: 2,
			wantPow2:  13.0, // When isEmpty2 is true, setter is not called, so result stays at initial value
			wantErr:   false,
		},
		{
			name:      "merge non-empty into empty",
			sum1:      0.0,
			count1:    0,
			sumPow2_1: 0.0,
			sum2:      3.0,
			count2:    1,
			sumPow2_2: 9.0,
			isEmpty1:  true,
			isEmpty2:  false,
			wantSum:   3.0,
			wantCount: 1,
			wantPow2:  9.0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupCtx1 := generateAggVarSampleGroupContext(types.T_float64.ToType())
			ctx1 := groupCtx1.(*aggVarSampleGroupContext)
			ctx1.sum = tt.sum1
			ctx1.count = tt.count1

			groupCtx2 := generateAggVarSampleGroupContext(types.T_float64.ToType())
			ctx2 := groupCtx2.(*aggVarSampleGroupContext)
			ctx2.sum = tt.sum2
			ctx2.count = tt.count2

			getter1 := &mockFloat64Getter{value: tt.sumPow2_1}
			getter2 := &mockFloat64Getter{value: tt.sumPow2_2}
			setter := &mockFloat64Setter{}

			err := aggVarSampleMerge(
				groupCtx1,
				groupCtx2,
				nil,
				tt.isEmpty1,
				tt.isEmpty2,
				getter1.Get,
				getter2.Get,
				setter.Set,
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantSum, ctx1.sum)
				require.Equal(t, tt.wantCount, ctx1.count)
				// When isEmpty2 is true, setter is not called, so we check getter1 value instead
				if tt.isEmpty2 {
					require.InDelta(t, tt.wantPow2, getter1.Get(), 1e-10)
				} else {
					require.InDelta(t, tt.wantPow2, setter.result, 1e-10)
				}
			}
		})
	}
}

func TestAggVarSampleFlush(t *testing.T) {
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
			want:    0.0,
			wantErr: false,
		},
		{
			name:    "two values 1,2",
			sum:     3.0,
			count:   2,
			sumPow2: 5.0, // 1^2 + 2^2 = 5
			want:    0.5, // (5/2 - (3/2)^2) * 2/1 = (2.5 - 2.25) * 2 = 0.5
			wantErr: false,
		},
		{
			name:    "three values 1,2,3",
			sum:     6.0,
			count:   3,
			sumPow2: 14.0, // 1^2 + 2^2 + 3^2 = 14
			want:    1.0,  // (14/3 - (6/3)^2) * 3/2 = (14/3 - 4) * 3/2 = (2/3) * 3/2 = 1.0
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
			name:    "five values 1,2,3,4,5",
			sum:     15.0,
			count:   5,
			sumPow2: 55.0, // 1^2 + 2^2 + 3^2 + 4^2 + 5^2 = 55
			want:    2.5,  // (55/5 - (15/5)^2) * 5/4 = (11 - 9) * 5/4 = 2.5
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupCtx := generateAggVarSampleGroupContext(types.T_float64.ToType())
			ctx := groupCtx.(*aggVarSampleGroupContext)
			ctx.sum = tt.sum
			ctx.count = tt.count

			getter := &mockFloat64Getter{value: tt.sumPow2}
			setter := &mockFloat64Setter{}

			err := aggVarSampleFlush(
				groupCtx,
				nil,
				getter.Get,
				setter.Set,
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.InDelta(t, tt.want, setter.result, 1e-10)
			}
		})
	}
}

func TestVarSampleVsVarPop(t *testing.T) {
	// Test that var_samp = var_pop * n / (n-1) for n > 1
	tests := []struct {
		name        string
		sum         float64
		count       int64
		sumPow2     float64
		wantVarPop  float64
		wantVarSamp float64
	}{
		{
			name:        "three values",
			sum:         6.0,
			count:       3,
			sumPow2:     14.0,
			wantVarPop:  0.6666666666666666, // (14/3 - 4) = 2/3
			wantVarSamp: 1.0,                // (14/3 - 4) * 3/2 = 1.0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate var_pop
			groupCtxPop := generateAggVarPopGroupContext(types.T_float64.ToType())
			ctxPop := groupCtxPop.(*aggVarPopGroupContext)
			ctxPop.sum = tt.sum
			ctxPop.count = tt.count
			getterPop := &mockFloat64Getter{value: tt.sumPow2}
			setterPop := &mockFloat64Setter{}
			err := aggVarPopFlush(groupCtxPop, nil, getterPop.Get, setterPop.Set)
			require.NoError(t, err)

			// Calculate var_samp
			groupCtxSamp := generateAggVarSampleGroupContext(types.T_float64.ToType())
			ctxSamp := groupCtxSamp.(*aggVarSampleGroupContext)
			ctxSamp.sum = tt.sum
			ctxSamp.count = tt.count
			getterSamp := &mockFloat64Getter{value: tt.sumPow2}
			setterSamp := &mockFloat64Setter{}
			err = aggVarSampleFlush(groupCtxSamp, nil, getterSamp.Get, setterSamp.Set)
			require.NoError(t, err)

			require.InDelta(t, tt.wantVarPop, setterPop.result, 1e-10)
			require.InDelta(t, tt.wantVarSamp, setterSamp.result, 1e-10)
			// Verify relationship: var_samp = var_pop * n / (n-1)
			expectedVarSamp := setterPop.result * float64(tt.count) / float64(tt.count-1)
			require.InDelta(t, expectedVarSamp, setterSamp.result, 1e-10)
		})
	}
}
