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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

func TestBitAndLengthCheck(t *testing.T) {
	tests := []struct {
		name          string
		currentLength int
		inputLength   int
	}{
		{
			name:          "length mismatch 1 vs 2",
			currentLength: 1,
			inputLength:   2,
		},
		{
			name:          "empty vs non-empty",
			currentLength: 0,
			inputLength:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := make([]byte, tt.currentLength)
			input := make([]byte, tt.inputLength)
			err := types.BitAnd(nil, current, input)
			require.Error(t, err)
			require.Contains(t, err.Error(),
				"Binary operands of bitwise operators must be of equal length")
		})
	}
}

type testCase struct {
	name        string
	value       []byte
	isEmpty     bool
	getterValue []byte
	setterErr   error
	wantErr     bool
}

func TestAggBitAndOfBinaryFill(t *testing.T) {
	tests := []testCase{
		{
			name:    "initial empty state",
			value:   []byte{0b10101010},
			isEmpty: true,
			wantErr: false,
		},
		{
			name:        "normal merge",
			value:       []byte{0b11110000},
			getterValue: []byte{0b10101010},
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getter := &mockBytesGetter{value: tc.getterValue}
			setter := &mockBytesSetter{err: tc.setterErr}
			err := aggBitAndOfBinaryFill(
				aggexec.AggGroupExecContext(nil),
				aggexec.AggCommonExecContext(nil),
				tc.value,
				tc.isEmpty,
				getter.Get,
				setter.Set,
			)

			if tc.wantErr {
				require.Error(t, err)
				if tc.setterErr != nil {
					require.ErrorIs(t, err, tc.setterErr)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type mergeTestCase struct {
	name         string
	isEmpty1     bool
	isEmpty2     bool
	getter1Value []byte
	getter2Value []byte
	setterErr    error
	wantErr      bool
}

func TestAggBitAndOfBinaryMerge(t *testing.T) {
	tests := []mergeTestCase{
		{
			name:     "merge two empty states",
			isEmpty1: true,
			isEmpty2: true,
			wantErr:  false,
		},
		{
			name:         "merge empty into non-empty",
			isEmpty1:     false,
			isEmpty2:     true,
			getter1Value: []byte{0b11110000},
			wantErr:      false,
		},
		{
			name:         "merge non-empty into empty",
			isEmpty1:     true,
			getter2Value: []byte{0b10101010},
			wantErr:      false,
		},
		{
			name:         "normal merge",
			getter1Value: []byte{0b11110000},
			getter2Value: []byte{0b10101010},
			wantErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getter1 := &mockBytesGetter{value: tc.getter1Value}
			getter2 := &mockBytesGetter{value: tc.getter2Value}
			setter := &mockBytesSetter{err: tc.setterErr}

			err := aggBitAndOfBinaryMerge(
				aggexec.AggGroupExecContext(nil),
				aggexec.AggGroupExecContext(nil),
				aggexec.AggCommonExecContext(nil),
				tc.isEmpty1,
				tc.isEmpty2,
				getter1.Get,
				getter2.Get,
				setter.Set,
			)

			if tc.wantErr {
				require.Error(t, err)
				if tc.setterErr != nil {
					require.ErrorIs(t, err, tc.setterErr)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type mockBytesGetter struct {
	value []byte
}

func (m *mockBytesGetter) Get() []byte {
	return m.value
}

type mockBytesSetter struct {
	result []byte
	err    error
}

func (m *mockBytesSetter) Set(value []byte) error {
	if m.err != nil {
		return m.err
	}
	m.result = make([]byte, len(value))
	copy(m.result, value)
	return nil
}
