// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// DuckDB 2.0 separates NUMBER from REAL in its VARIANT comparison keys. MO
// deliberately retains MySQL JSON's common numeric domain, including hashing.
// A file-encoding optimization must not change DISTINCT/JOIN semantics.
func TestJSONCommonNumericDomainTenThousandCases(t *testing.T) {
	rng := rand.New(rand.NewSource(20261013))
	for i := 0; i < 10000; i++ {
		n := rng.Int63n(1 << 53)
		integer, err := CreateByteJSON(n)
		require.NoError(t, err)
		floating, err := CreateByteJSON(float64(n))
		require.NoError(t, err)
		decimal, err := ParseFromStringExact(fmt.Sprintf("%d.000", n))
		require.NoError(t, err)
		for _, other := range []ByteJson{floating, decimal} {
			require.Zero(t, CompareByteJson(integer, other), i)
			left, ok := AppendCanonicalNumber(nil, integer)
			require.True(t, ok)
			right, ok := AppendCanonicalNumber(nil, other)
			require.True(t, ok)
			require.Equal(t, left, right, i)
		}
		exactFraction, err := ParseFromStringExact(fmt.Sprintf("%d.000000001", n))
		require.NoError(t, err)
		require.Greater(t, CompareByteJson(exactFraction, integer), 0, i)
		left, ok := AppendCanonicalNumber(nil, integer)
		require.True(t, ok)
		right, ok := AppendCanonicalNumber(nil, exactFraction)
		require.True(t, ok)
		require.NotEqual(t, left, right, i)
	}
}

func TestJSONCaseSensitivePathsAndPresence(t *testing.T) {
	// DuckDB's STRUCT-based shredding cannot freely combine a/A. MO JSON
	// must continue retaining both keys, empty keys, missing and explicit null.
	doc, err := ParseFromStringExact(`{"A":2,"a":1,"":3,"nested":{"present":null}}`)
	require.NoError(t, err)
	for _, tc := range []struct {
		path, value string
		exists      bool
	}{
		{`$.A`, "2", true}, {`$.a`, "1", true}, {`$.""`, "3", true},
		{`$.nested.present`, "null", true}, {`$.nested.missing`, "null", false},
	} {
		path, err := ParseJsonPath(tc.path)
		require.NoError(t, err)
		value, exists := doc.QueryWithExists([]*Path{&path})
		require.Equal(t, tc.exists, exists, tc.path)
		if exists {
			require.Equal(t, tc.value, value.String())
		}
	}
}
