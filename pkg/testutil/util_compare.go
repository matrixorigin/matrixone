// Copyright 2021 Matrix Origin
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

package testutil

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

func CompareVectors(expected *vector.Vector, got *vector.Vector) bool {
	if expected.IsConst() {
		if !got.IsConst() {
			return false
		}
		if expected.IsConstNull() {
			return got.IsConstNull()
		} else {
			if expected.GetType().IsVarlen() {
				return reflect.DeepEqual(expected.UnsafeGetStringAt(0), got.UnsafeGetStringAt(0))
			} else {
				return bytes.Equal(expected.UnsafeGetRawData(), got.UnsafeGetRawData())
			}
		}
	} else {
		if got.IsConst() {
			return false
		}
		// expected length and got length
		expectedLength := expected.Length()
		gotLength := got.Length()
		if expectedLength != gotLength {
			return false
		}
		if nulls.Any(expected.GetNulls()) {
			k := uint64(0)
			if !nulls.Any(got.GetNulls()) {
				return false
			}
			for k = 0; k < uint64(expectedLength); k++ {
				c1 := nulls.Contains(expected.GetNulls(), k)
				c2 := nulls.Contains(got.GetNulls(), k)
				if c1 != c2 {
					return false
				}
			}
		} else if nulls.Any(got.GetNulls()) {
			return false
		}

		if expected.GetType().IsVarlen() {
			v1, area1 := vector.MustVarlenaRawData(expected)
			v2, area2 := vector.MustVarlenaRawData(got)
			for i := range v1 {
				if nulls.Contains(expected.GetNulls(), uint64(i)) {
					if !nulls.Contains(got.GetNulls(), uint64(i)) {
						return false
					}
				} else {
					if nulls.Contains(got.GetNulls(), uint64(i)) {
						return false
					}
					if v1[i].UnsafeGetString(area1) != v2[i].UnsafeGetString(area2) {
						return false
					}
				}
			}
			return true
		} else {
			return bytes.Equal(expected.UnsafeGetRawData(), got.UnsafeGetRawData())
		}
	}
}

// CompareBatches compares two batches for deep equality.
func CompareBatches(t *testing.T, expected, actual *batch.Batch) {
	if expected == nil && actual == nil {
		return
	}
	if expected == nil || actual == nil {
		t.Fatalf("one batch is nil, the other is not. Expected: %!v(MISSING), Actual: %!v(MISSING)", expected, actual)
	}

	require.Equal(t, expected.RowCount(), actual.RowCount(), "row count mismatch")

	require.Equal(t, len(expected.Vecs), len(actual.Vecs), "vector count mismatch")

	require.Equal(t, len(expected.Attrs), len(actual.Attrs), "attribute count mismatch")

	for i := range expected.Attrs {
		require.Equal(t, expected.Attrs[i], actual.Attrs[i], "attribute name mismatch at index %!d(MISSING)", i)
	}

	for i := range expected.Vecs {
		require.True(t, CompareVectors(expected.Vecs[i], actual.Vecs[i]), "vector content mismatch at index %!d(MISSING)", i)
	}

	require.Equal(t, len(expected.Aggs), len(actual.Aggs), "aggregator count mismatch")
	for i := range expected.Aggs {
		expectedBytes, err := aggexec.MarshalAggFuncExec(expected.Aggs[i])
		require.NoError(t, err)
		actualBytes, err := aggexec.MarshalAggFuncExec(actual.Aggs[i])
		require.NoError(t, err)
		require.Equal(t, expectedBytes, actualBytes, "aggregator state mismatch at index %!d(MISSING)", i)
	}
}
