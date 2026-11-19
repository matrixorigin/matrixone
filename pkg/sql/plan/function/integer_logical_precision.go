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

package function

import "github.com/matrixorigin/matrixone/pkg/container/types"

// IntegerLogicalPrecision describes display semantics, never a physical vector
// width or an arithmetic value bound. Raw precision can exceed 65 for a long
// integer token or ADD; only ordinary expression consumers impose that cap.
type IntegerLogicalPrecision struct {
	Raw      uint64
	Unsigned bool
	Known    bool
}

func (p IntegerLogicalPrecision) DisplayLength() uint64 {
	if !p.Known {
		return 0
	}
	if p.Unsigned {
		return p.Raw
	}
	return p.Raw + 1
}

func (p IntegerLogicalPrecision) Offered(literal bool) uint64 {
	if literal {
		return p.Raw
	}
	return min(max(p.Raw, 1), 65)
}

// IntegerDeclaredPrecision includes MEDIUMINT's MySQL declaration display
// envelope. Explicit SIGNED/UNSIGNED casts and parameters have a different
// 21-character contract and must not call this constructor.
func IntegerDeclaredPrecision(t types.Type) IntegerLogicalPrecision {
	var digits uint64
	switch t.Oid {
	case types.T_int8, types.T_uint8:
		digits = 3
	case types.T_int16, types.T_uint16:
		digits = 5
	case types.T_int32, types.T_uint32:
		digits = 10
		if t.IsMediumInt() {
			digits = 8
		}
	case types.T_int64:
		digits = 19
	case types.T_uint64:
		digits = 20
	default:
		return IntegerLogicalPrecision{}
	}
	return IntegerLogicalPrecision{Raw: digits, Unsigned: t.Oid.IsUnsignedInt(), Known: true}
}

func IntegerCastPrecision(unsigned bool) IntegerLogicalPrecision {
	p := IntegerLogicalPrecision{Raw: 20, Unsigned: unsigned, Known: true}
	if unsigned {
		p.Raw++
	}
	return p
}

// IntegerAddMulPrecision is pure: callers supply each source's method-dispatched
// precision. Bounds/admission are checked by the query metadata owner before
// calling it, so these additions cannot overflow uint64.
func IntegerAddMulPrecision(multiply bool, left, right IntegerLogicalPrecision, leftLiteral, rightLiteral bool) IntegerLogicalPrecision {
	if !left.Known || !right.Known {
		return IntegerLogicalPrecision{}
	}
	l, r := left.Offered(leftLiteral), right.Offered(rightLiteral)
	p := min(l+r, uint64(65))
	if !multiply {
		p = max(l, r) + 1
	}
	return IntegerLogicalPrecision{Raw: p, Unsigned: left.Unsigned || right.Unsigned, Known: true}
}
