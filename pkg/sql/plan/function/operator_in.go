// Copyright 2022 Matrix Origin
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
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type TGenericOfIn interface {
	constraints.Integer | constraints.Float | bool | types.Uuid |
		types.Time | types.Timestamp | types.Date | types.Datetime | types.Decimal64 | types.Decimal128 | types.Decimal256 | types.MoYear
}

type opOperatorFixedIn[T TGenericOfIn] struct {
	ready   bool
	hasNull bool
	mp      map[T]bool
}

type opOperatorStrIn struct {
	ready    bool
	hasNull  bool
	padSpace bool
	typ      types.Type
	mp       map[string]bool
}

func newOpOperatorFixedIn[T TGenericOfIn]() *opOperatorFixedIn[T] {
	op := new(opOperatorFixedIn[T])
	op.ready = false
	return op
}

func newOpOperatorStrIn() *opOperatorStrIn {
	op := new(opOperatorStrIn)
	op.ready = false
	return op
}

func (op *opOperatorFixedIn[T]) init(tuple *vector.Vector) {
	op.ready = true
	op.hasNull = false

	if tuple.IsConstNull() {
		op.hasNull = true
		op.mp = make(map[T]bool)
		return
	}
	p := vector.GenerateFunctionFixedTypeParameter[T](tuple)

	if tuple.IsConst() {
		v, null := p.GetValue(0)
		if null {
			op.hasNull = true
			op.mp = make(map[T]bool)
			return
		}
		op.mp = make(map[T]bool, 1)
		op.mp[v] = true
		return
	}

	op.mp = make(map[T]bool, tuple.Length())
	for i := uint64(0); i < uint64(tuple.Length()); i++ {
		v, null := p.GetValue(i)
		if null {
			op.hasNull = true
			continue
		}
		op.mp[v] = true
	}
}

func (op *opOperatorStrIn) init(left, tuple *vector.Vector) {
	op.ready = true
	op.hasNull = false
	op.typ = *tuple.GetType()
	// The left operand is the resolved comparison domain for an IN
	// expression. A prepared/literal list can arrive with the transport
	// charset, so deriving the key type from the list alone silently drops a
	// native Unicode collation. Keep a Unicode type from either side and prefer
	// the left operand when both sides already agree on the domain.
	if left != nil && (types.IsUnicodeCollation(left.GetType().Charset) ||
		!types.IsUnicodeCollation(op.typ.Charset)) {
		op.typ = *left.GetType()
	}
	op.padSpace = op.typ.Oid == types.T_char || tuple.GetType().Oid == types.T_char

	if tuple.IsConstNull() {
		op.hasNull = true
		op.mp = make(map[string]bool)
		return
	}
	p := vector.GenerateFunctionStrParameter(tuple)

	if tuple.IsConst() {
		v, null := p.GetStrValue(0)
		if null {
			op.hasNull = true
			op.mp = make(map[string]bool)
			return
		}
		op.mp = make(map[string]bool, 1)
		op.mp[string(op.keyBytes(v))] = true
		return
	}

	op.mp = make(map[string]bool)
	for i := uint64(0); i < uint64(tuple.Length()); i++ {
		v, null := p.GetStrValue(i)
		if null {
			op.hasNull = true
			continue
		}
		op.mp[string(op.keyBytes(v))] = true
	}
}

func (op *opOperatorStrIn) keyBytes(value []byte) []byte {
	if op.padSpace {
		value = bytes.TrimRight(value, " ")
	}
	if types.IsUnicodeCollation(op.typ.Charset) {
		// IN/NOT IN must use the same tagged comparison domain as equality and
		// grouping. In particular, an invalid/repertoire-invalid value gets a
		// distinct marker instead of being inserted as an untagged raw key that
		// can collide with a valid UCA weight string.
		value = types.CollationKeyOrOriginal(op.typ.Charset, value)
	}
	return value
}

func (op *opOperatorStrIn) containsKey(value []byte) bool {
	// A lookup borrows the normalized bytes only for this expression. Map
	// insertion above retains an owned string; probes need no owned copy.
	_, ok := op.mp[string(op.keyBytes(value))]
	return ok
}

func (op *opOperatorFixedIn[T]) operatorIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !op.ready {
		op.init(parameters[1])
	}

	p := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			_, ok := op.mp[v]
			if !ok && op.hasNull {
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opOperatorFixedIn[T]) operatorNotIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !op.ready {
		op.init(parameters[1])
	}

	p := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			_, ok := op.mp[v]
			if !ok && op.hasNull {
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(!ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opOperatorStrIn) operatorIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !op.ready {
		op.init(parameters[0], parameters[1])
	}

	p := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetStrValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			ok := op.containsKey(v)
			if !ok && op.hasNull {
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opOperatorStrIn) operatorNotIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !op.ready {
		op.init(parameters[0], parameters[1])
	}

	p := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetStrValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			ok := op.containsKey(v)
			if !ok && op.hasNull {
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(!ok, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// A sorted integer list can use a bounded exact bitmap. The embedded operator
// remains the owner of readiness and fallback map construction.
type opOperatorIntegerIn[T constraints.Integer] struct {
	opOperatorFixedIn[T]
	lower uint64
	bits  *bitmap.Bitmap
}

func newOpOperatorIntegerIn[T constraints.Integer]() *opOperatorIntegerIn[T] {
	return new(opOperatorIntegerIn[T])
}

func (op *opOperatorIntegerIn[T]) initBitmap(tuple *vector.Vector) bool {
	const maxBitmapBits = 64 * 1024 * 8
	if tuple.IsConst() || tuple.Length() < 16 || !tuple.GetSorted() || tuple.GetNulls().Any() {
		return false
	}
	values := vector.MustFixedColWithTypeCheck[T](tuple)
	lower := uint64(values[0])
	span := uint64(values[len(values)-1]) - lower
	// Subtract in the unsigned domain before adding one, including signed
	// endpoints. Reject huge spans before length arithmetic or allocation.
	// Rounded storage must also fit one uint64 word per input value.
	if span >= maxBitmapBits || span/64 >= uint64(len(values)) {
		return false
	}
	op.lower = lower
	op.bits = new(bitmap.Bitmap)
	op.bits.InitWithSize(int64(span) + 1)
	for _, v := range values {
		op.bits.Add(uint64(v) - lower)
	}
	op.ready = true
	return true
}

func (op *opOperatorIntegerIn[T]) operatorIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.eval(parameters, result, length, false)
}

func (op *opOperatorIntegerIn[T]) operatorNotIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.eval(parameters, result, length, true)
}

func (op *opOperatorIntegerIn[T]) eval(parameters []*vector.Vector, result vector.FunctionResultWrapper, length int, negate bool) error {
	if !op.ready && !op.initBitmap(parameters[1]) {
		op.init(parameters[1])
	}
	probe := parameters[0]
	rs := result.GetResultVector()
	resultNulls := rs.GetNulls()
	if probe.IsConstNull() {
		resultNulls.AddRange(0, uint64(length))
		return nil
	}
	values := vector.MustFixedColWithTypeCheck[T](probe)
	output := vector.MustFixedColWithTypeCheck[bool](rs)[:length]
	if probe.IsConst() {
		var found bool
		if op.bits != nil {
			found = op.bits.Contains(uint64(values[0]) - op.lower)
		} else {
			_, found = op.mp[values[0]]
		}
		if !found && op.hasNull {
			resultNulls.AddRange(0, uint64(length))
		} else {
			for i := range output {
				output[i] = found != negate
			}
		}
		return nil
	}
	// Nullable probes must skip membership work for NULL rows: a sparse map
	// lookup can cost more than the entire NULL-result path.
	probeNulls := probe.GetNulls()
	if probeNulls.Any() {
		for i, v := range values[:length] {
			if probeNulls.Contains(uint64(i)) {
				resultNulls.Add(uint64(i))
				continue
			}
			var found bool
			if op.bits != nil {
				found = op.bits.Contains(uint64(v) - op.lower)
			} else {
				_, found = op.mp[v]
			}
			output[i] = found != negate
			if !found && op.hasNull {
				resultNulls.Add(uint64(i))
			}
		}
		return nil
	}
	// Keep representation and vector-shape decisions outside the hot loop.
	// PreExtendAndReset owns result allocation and length, as for comparisons.
	if op.bits != nil {
		for i, v := range values[:length] {
			output[i] = op.bits.Contains(uint64(v)-op.lower) != negate
		}
	} else {
		for i, v := range values[:length] {
			_, found := op.mp[v]
			output[i] = found != negate
			if !found && op.hasNull {
				resultNulls.Add(uint64(i))
			}
		}
	}
	return nil
}
