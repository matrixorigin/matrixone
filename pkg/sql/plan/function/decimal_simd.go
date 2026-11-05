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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Provisional minima are qualified against dispatched scalar controls before
// adoption. Fixed expression chunk capacity is independent of these thresholds.
const (
	decimalSIMDPreparedMinRows = 256
	decimalSIMDFusedMinRows    = 32
	decimalSIMDMultiplyMinRows = 32
	decimalSIMDCastMinRows     = 64
)

func decimalArithmeticSIMDMinRows(op int32, x, y, out *vector.Vector) int {
	if op == MULTI && x.GetType().Oid == types.T_decimal64 && y.GetType().Oid == types.T_decimal64 && out.GetType().Oid == types.T_decimal128 {
		return decimalSIMDMultiplyMinRows
	}
	return decimalSIMDPreparedMinRows
}

// TryDecimalArithmeticSIMD optionally computes admitted aggregate operands.
// Refusal leaves output untouched; unsupported shapes keep scalar arithmetic.
func TryDecimalArithmeticSIMD(op int32, x, y, out *vector.Vector, xi, yi, oi, n int) bool {
	return n >= 32 && n >= decimalArithmeticSIMDMinRows(op, x, y, out) && decimalSIMDQuickCheck(op, x, y, xi, yi) && tryDecimalSIMD(op, x, y, out, xi, yi, oi, n)
}
