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

package types

import (
	"math/big"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var decimal128CoefficientSink Decimal128
var decimal128CoefficientError error

// Same source runs unchanged on base/head; expected words are computed once
// outside timing using math/big. No wall/allocation threshold belongs in a UT.
func BenchmarkDecimal128FromCoefficient(b *testing.B) {
	for _, tc := range []struct {
		name    string
		digits  []byte
		invalid bool
	}{
		{name: "nil"},
		{name: "empty", digits: []byte{}},
		{name: "leading-zero18", digits: []byte("001732206349000167")},
		{name: "microseconds16", digits: []byte("1732206349000167")},
		{name: "max18", digits: []byte(strings.Repeat("9", 18))},
		{name: "min19-fallback", digits: []byte("1000000000000000000")},
		{name: "max38-fallback", digits: []byte(strings.Repeat("9", 38))},
		{name: "invalid18-control", digits: []byte(strings.Repeat("1", 17) + "x"), invalid: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			before := string(tc.digits)
			var want Decimal128
			if !tc.invalid && len(tc.digits) > 0 {
				value, ok := new(big.Int).SetString(string(tc.digits), 10)
				if !ok {
					b.Fatal("invalid independent benchmark fixture")
				}
				want = Decimal128{B0_63: value.Uint64(), B64_127: new(big.Int).Rsh(new(big.Int).Set(value), 64).Uint64()}
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.digits)))
			b.ResetTimer()
			for b.Loop() {
				decimal128CoefficientSink, decimal128CoefficientError = Decimal128FromCoefficient(tc.digits)
			}
			b.StopTimer()
			if tc.invalid {
				if !moerr.IsMoErrCode(decimal128CoefficientError, moerr.ErrInvalidInput) || decimal128CoefficientError.Error() != "invalid input: invalid Decimal128 coefficient" || decimal128CoefficientSink != (Decimal128{}) {
					b.Fatal("invalid coefficient result/error contract changed")
				}
			} else if decimal128CoefficientError != nil || decimal128CoefficientSink != want {
				b.Fatalf("coefficient = %#v, error = %v, want %#v", decimal128CoefficientSink, decimal128CoefficientError, want)
			}
			if string(tc.digits) != before {
				b.Fatal("coefficient input changed")
			}
		})
	}
}
