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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

func TestSumSupportedTypes(t *testing.T) {
	found := false
	for _, typ := range SumSupportedTypes {
		if typ == types.T_bool {
			found = true
			break
		}
	}
	require.True(t, found, "T_bool should be in SumSupportedTypes")
}

func TestSumReturnType(t *testing.T) {
	result := SumReturnType([]types.Type{types.T_bool.ToType()})
	require.Equal(t, types.T_uint64, result.Oid)
}

func TestAggSumOfBoolFill(t *testing.T) {
	var result uint64
	getter := aggexec.AggGetter[uint64](func() uint64 { return result })
	setter := aggexec.AggSetter[uint64](func(v uint64) { result = v })

	err := aggSumOfBoolFill(nil, nil, true, false, getter, setter)
	require.NoError(t, err)
	require.Equal(t, uint64(1), result)

	err = aggSumOfBoolFill(nil, nil, true, false, getter, setter)
	require.NoError(t, err)
	require.Equal(t, uint64(2), result)

	err = aggSumOfBoolFill(nil, nil, false, false, getter, setter)
	require.NoError(t, err)
	require.Equal(t, uint64(2), result)
}

func TestAggSumOfBoolFills(t *testing.T) {
	var result uint64
	getter := aggexec.AggGetter[uint64](func() uint64 { return result })
	setter := aggexec.AggSetter[uint64](func(v uint64) { result = v })

	err := aggSumOfBoolFills(nil, nil, true, 5, false, getter, setter)
	require.NoError(t, err)
	require.Equal(t, uint64(5), result)

	err = aggSumOfBoolFills(nil, nil, false, 3, false, getter, setter)
	require.NoError(t, err)
	require.Equal(t, uint64(5), result)

	err = aggSumOfBoolFills(nil, nil, true, 2, false, getter, setter)
	require.NoError(t, err)
	require.Equal(t, uint64(7), result)
}

func TestAggSumOfBoolMerge(t *testing.T) {
	var result uint64
	getter1 := aggexec.AggGetter[uint64](func() uint64 { return 10 })
	getter2 := aggexec.AggGetter[uint64](func() uint64 { return 25 })
	setter := aggexec.AggSetter[uint64](func(v uint64) { result = v })

	err := aggSumOfBoolMerge(nil, nil, nil, false, false, getter1, getter2, setter)
	require.NoError(t, err)
	require.Equal(t, uint64(35), result)
}
