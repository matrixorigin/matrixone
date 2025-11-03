// Copyright 2025 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

// Test_addInt64SumWithOverflowCheck tests the addInt64SumWithOverflowCheck function
func Test_addInt64SumWithOverflowCheck(t *testing.T) {
	tests := []struct {
		name    string
		v1      int64
		v2      int64
		want    int64
		wantErr bool
	}{
		{
			name:    "normal positive addition",
			v1:      100,
			v2:      200,
			want:    300,
			wantErr: false,
		},
		{
			name:    "normal negative addition",
			v1:      -100,
			v2:      -200,
			want:    -300,
			wantErr: false,
		},
		{
			name:    "positive and negative (no overflow)",
			v1:      100,
			v2:      -50,
			want:    50,
			wantErr: false,
		},
		{
			name:    "zero addition",
			v1:      0,
			v2:      0,
			want:    0,
			wantErr: false,
		},
		{
			name:    "positive overflow: max + 1",
			v1:      math.MaxInt64,
			v2:      1,
			want:    0,
			wantErr: true,
		},
		{
			name:    "positive overflow: max + max",
			v1:      math.MaxInt64,
			v2:      math.MaxInt64,
			want:    0,
			wantErr: true,
		},
		{
			name:    "negative overflow: min + (-1)",
			v1:      math.MinInt64,
			v2:      -1,
			want:    0,
			wantErr: true,
		},
		{
			name:    "negative overflow: min + min",
			v1:      math.MinInt64,
			v2:      math.MinInt64,
			want:    0,
			wantErr: true,
		},
		{
			name:    "edge case: max + 0",
			v1:      math.MaxInt64,
			v2:      0,
			want:    math.MaxInt64,
			wantErr: false,
		},
		{
			name:    "edge case: min + 0",
			v1:      math.MinInt64,
			v2:      0,
			want:    math.MinInt64,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := addInt64SumWithOverflowCheck(tt.v1, tt.v2)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "SUM overflow")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

// Test_mulInt64SumWithOverflowCheck tests the mulInt64SumWithOverflowCheck function
func Test_mulInt64SumWithOverflowCheck(t *testing.T) {
	tests := []struct {
		name    string
		v1      int64
		v2      int64
		want    int64
		wantErr bool
	}{
		{
			name:    "normal positive multiplication",
			v1:      100,
			v2:      200,
			want:    20000,
			wantErr: false,
		},
		{
			name:    "normal negative multiplication",
			v1:      -100,
			v2:      200,
			want:    -20000,
			wantErr: false,
		},
		{
			name:    "two negatives",
			v1:      -100,
			v2:      -200,
			want:    20000,
			wantErr: false,
		},
		{
			name:    "multiply by zero (v1 is zero)",
			v1:      0,
			v2:      123456789,
			want:    0,
			wantErr: false,
		},
		{
			name:    "multiply by zero (v2 is zero)",
			v1:      123456789,
			v2:      0,
			want:    0,
			wantErr: false,
		},
		{
			name:    "multiply by one",
			v1:      123456789,
			v2:      1,
			want:    123456789,
			wantErr: false,
		},
		{
			name:    "multiply by negative one",
			v1:      123456789,
			v2:      -1,
			want:    -123456789,
			wantErr: false,
		},
		{
			name:    "overflow: MinInt64 * (-1)",
			v1:      math.MinInt64,
			v2:      -1,
			want:    0,
			wantErr: true,
		},
		{
			name:    "overflow: (-1) * MinInt64",
			v1:      -1,
			v2:      math.MinInt64,
			want:    0,
			wantErr: true,
		},
		{
			name:    "overflow: large positive * large positive",
			v1:      math.MaxInt64 / 2,
			v2:      3,
			want:    0,
			wantErr: true,
		},
		{
			name:    "overflow: MaxInt64 * 2",
			v1:      math.MaxInt64,
			v2:      2,
			want:    0,
			wantErr: true,
		},
		{
			name:    "overflow: large negative multiplication",
			v1:      math.MinInt64 / 2,
			v2:      3,
			want:    0,
			wantErr: true,
		},
		{
			name:    "edge case: no overflow near max",
			v1:      math.MaxInt64 / 2,
			v2:      2,
			want:    math.MaxInt64 - 1,
			wantErr: false,
		},
		{
			name:    "edge case: no overflow near min",
			v1:      math.MinInt64 / 2,
			v2:      2,
			want:    math.MinInt64,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mulInt64SumWithOverflowCheck(tt.v1, tt.v2)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "SUM overflow")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

// Test_aggSumInt64Fill tests the aggSumInt64Fill function
func Test_aggSumInt64Fill(t *testing.T) {
	tests := []struct {
		name        string
		value       int64
		isEmpty     bool
		getterValue int64
		want        int64
		wantErr     bool
	}{
		{
			name:        "normal fill",
			value:       100,
			isEmpty:     false,
			getterValue: 200,
			want:        300,
			wantErr:     false,
		},
		{
			name:        "fill into empty",
			value:       100,
			isEmpty:     true,
			getterValue: 0,
			want:        100,
			wantErr:     false,
		},
		{
			name:        "fill negative",
			value:       -100,
			isEmpty:     false,
			getterValue: 200,
			want:        100,
			wantErr:     false,
		},
		{
			name:        "overflow: add to MaxInt64",
			value:       1,
			isEmpty:     false,
			getterValue: math.MaxInt64,
			want:        0,
			wantErr:     true,
		},
		{
			name:        "overflow: add to MinInt64",
			value:       -1,
			isEmpty:     false,
			getterValue: math.MinInt64,
			want:        0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getter := &mockInt64Getter{value: tt.getterValue}
			setter := &mockInt64Setter{}

			err := aggSumInt64Fill(
				aggexec.AggGroupExecContext(nil),
				aggexec.AggCommonExecContext(nil),
				tt.value,
				tt.isEmpty,
				getter.Get,
				setter.Set,
			)

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "SUM overflow")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, setter.result)
			}
		})
	}
}

// Test_aggSumInt64Fills tests the aggSumInt64Fills function
func Test_aggSumInt64Fills(t *testing.T) {
	tests := []struct {
		name        string
		value       int64
		count       int
		isEmpty     bool
		getterValue int64
		want        int64
		wantErr     bool
	}{
		{
			name:        "normal fills",
			value:       100,
			count:       3,
			isEmpty:     false,
			getterValue: 200,
			want:        500, // 200 + (100 * 3)
			wantErr:     false,
		},
		{
			name:        "fills with count 1",
			value:       100,
			count:       1,
			isEmpty:     false,
			getterValue: 200,
			want:        300,
			wantErr:     false,
		},
		{
			name:        "fills with count 0",
			value:       100,
			count:       0,
			isEmpty:     false,
			getterValue: 200,
			want:        200, // 200 + (100 * 0)
			wantErr:     false,
		},
		{
			name:        "fills negative value",
			value:       -50,
			count:       4,
			isEmpty:     false,
			getterValue: 300,
			want:        100, // 300 + (-50 * 4)
			wantErr:     false,
		},
		{
			name:        "overflow in multiplication",
			value:       math.MaxInt64,
			count:       2,
			isEmpty:     false,
			getterValue: 0,
			want:        0,
			wantErr:     true,
		},
		{
			name:        "overflow in multiplication with MinInt64",
			value:       math.MinInt64,
			count:       -1,
			isEmpty:     false,
			getterValue: 0,
			want:        0,
			wantErr:     true,
		},
		{
			name:        "overflow in addition after multiplication",
			value:       math.MaxInt64 / 2,
			count:       2,
			isEmpty:     false,
			getterValue: math.MaxInt64 / 2,
			want:        0,
			wantErr:     true,
		},
		{
			name:        "large count causing overflow",
			value:       1000000000,
			count:       10,
			isEmpty:     false,
			getterValue: 0,
			want:        10000000000,
			wantErr:     false,
		},
		{
			name:        "very large count causing overflow",
			value:       math.MaxInt64 / 1000,
			count:       1001,
			isEmpty:     false,
			getterValue: 0,
			want:        0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getter := &mockInt64Getter{value: tt.getterValue}
			setter := &mockInt64Setter{}

			err := aggSumInt64Fills(
				aggexec.AggGroupExecContext(nil),
				aggexec.AggCommonExecContext(nil),
				tt.value,
				tt.count,
				tt.isEmpty,
				getter.Get,
				setter.Set,
			)

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "SUM overflow")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, setter.result)
			}
		})
	}
}

// Test_aggSumInt64Merge tests the aggSumInt64Merge function
func Test_aggSumInt64Merge(t *testing.T) {
	tests := []struct {
		name         string
		isEmpty1     bool
		isEmpty2     bool
		getter1Value int64
		getter2Value int64
		want         int64
		wantErr      bool
	}{
		{
			name:         "merge two normal values",
			isEmpty1:     false,
			isEmpty2:     false,
			getter1Value: 100,
			getter2Value: 200,
			want:         300,
			wantErr:      false,
		},
		{
			name:         "merge positive and negative",
			isEmpty1:     false,
			isEmpty2:     false,
			getter1Value: 500,
			getter2Value: -200,
			want:         300,
			wantErr:      false,
		},
		{
			name:         "merge zeros",
			isEmpty1:     false,
			isEmpty2:     false,
			getter1Value: 0,
			getter2Value: 0,
			want:         0,
			wantErr:      false,
		},
		{
			name:         "merge with one empty (isEmpty1 = true)",
			isEmpty1:     true,
			isEmpty2:     false,
			getter1Value: 0,
			getter2Value: 200,
			want:         200,
			wantErr:      false,
		},
		{
			name:         "merge with one empty (isEmpty2 = true)",
			isEmpty1:     false,
			isEmpty2:     true,
			getter1Value: 100,
			getter2Value: 0,
			want:         100,
			wantErr:      false,
		},
		{
			name:         "merge two empty",
			isEmpty1:     true,
			isEmpty2:     true,
			getter1Value: 0,
			getter2Value: 0,
			want:         0,
			wantErr:      false,
		},
		{
			name:         "overflow: merge MaxInt64 + 1",
			isEmpty1:     false,
			isEmpty2:     false,
			getter1Value: math.MaxInt64,
			getter2Value: 1,
			want:         0,
			wantErr:      true,
		},
		{
			name:         "overflow: merge MinInt64 + (-1)",
			isEmpty1:     false,
			isEmpty2:     false,
			getter1Value: math.MinInt64,
			getter2Value: -1,
			want:         0,
			wantErr:      true,
		},
		{
			name:         "overflow: merge two large positive values",
			isEmpty1:     false,
			isEmpty2:     false,
			getter1Value: math.MaxInt64/2 + 1,
			getter2Value: math.MaxInt64/2 + 1,
			want:         0,
			wantErr:      true,
		},
		{
			name:         "overflow: merge two large negative values",
			isEmpty1:     false,
			isEmpty2:     false,
			getter1Value: math.MinInt64/2 - 1,
			getter2Value: math.MinInt64/2 - 1,
			want:         0,
			wantErr:      true,
		},
		{
			name:         "edge case: merge near max without overflow",
			isEmpty1:     false,
			isEmpty2:     false,
			getter1Value: math.MaxInt64 - 100,
			getter2Value: 100,
			want:         math.MaxInt64,
			wantErr:      false,
		},
		{
			name:         "edge case: merge near min without overflow",
			isEmpty1:     false,
			isEmpty2:     false,
			getter1Value: math.MinInt64 + 100,
			getter2Value: -100,
			want:         math.MinInt64,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getter1 := &mockInt64Getter{value: tt.getter1Value}
			getter2 := &mockInt64Getter{value: tt.getter2Value}
			setter := &mockInt64Setter{}

			err := aggSumInt64Merge(
				aggexec.AggGroupExecContext(nil),
				aggexec.AggGroupExecContext(nil),
				aggexec.AggCommonExecContext(nil),
				tt.isEmpty1,
				tt.isEmpty2,
				getter1.Get,
				getter2.Get,
				setter.Set,
			)

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "SUM overflow")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, setter.result)
			}
		})
	}
}

// Mock implementations for testing

type mockInt64Getter struct {
	value int64
}

func (m *mockInt64Getter) Get() int64 {
	return m.value
}

type mockInt64Setter struct {
	result int64
}

func (m *mockInt64Setter) Set(value int64) {
	m.result = value
}
