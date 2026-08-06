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

package bytejson

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalNumberCommonPathDoesNotAllocate(t *testing.T) {
	integer := makeJson(t, "1")
	floating := makeJson(t, "1.25")
	integralFloat := makeJsonWithoutParse(1)
	for _, value := range []ByteJson{integer, floating} {
		var storage [9]byte
		allocs := testing.AllocsPerRun(100, func() {
			encoded, ok := AppendCanonicalNumber(storage[:0], value)
			if !ok || len(encoded) != len(storage) {
				t.Fatalf("unexpected canonical number result: ok=%v len=%d", ok, len(encoded))
			}
		})
		require.Zero(t, allocs)
	}

	allocs := testing.AllocsPerRun(100, func() {
		if CompareByteJson(integer, integralFloat) != 0 {
			t.Fatal("integer and integral float must compare equal")
		}
	})
	require.Zero(t, allocs)
}

func makeJsonWithoutParse(value float64) ByteJson {
	var data [8]byte
	endian.PutUint64(data[:], math.Float64bits(value))
	return ByteJson{Type: TpCodeFloat64, Data: data[:]}
}

func makeIntJSONWithoutParse(value int64) ByteJson {
	var data [8]byte
	endian.PutUint64(data[:], uint64(value))
	return ByteJson{Type: TpCodeInt64, Data: data[:]}
}

func BenchmarkCompareByteJsonNumeric(b *testing.B) {
	tests := []struct {
		name  string
		left  ByteJson
		right ByteJson
	}{
		{name: "integer-float", left: makeIntJSONWithoutParse(1), right: makeJsonWithoutParse(1)},
		{name: "decimal-float", left: makeDecimalJson("0.100"), right: makeJsonWithoutParse(0.1)},
		{name: "extreme-decimal", left: makeDecimalJson("1e2147483647"), right: makeDecimalJson("10e2147483646")},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if CompareByteJson(test.left, test.right) != 0 {
					b.Fatal("values must compare equal")
				}
			}
		})
	}
}

func BenchmarkAppendCanonicalNumber(b *testing.B) {
	tests := []struct {
		name  string
		value ByteJson
	}{
		{name: "float", value: makeJsonWithoutParse(0.1)},
		{name: "decimal", value: makeDecimalJson("0.100")},
		{name: "extreme-decimal", value: makeDecimalJson("1e2147483647")},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			dst := make([]byte, 0, 64)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				encoded, ok := AppendCanonicalNumber(dst[:0], test.value)
				if !ok || len(encoded) == 0 {
					b.Fatal("value must have a canonical number key")
				}
			}
		})
	}
}
