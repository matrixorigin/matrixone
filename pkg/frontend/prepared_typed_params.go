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

package frontend

import (
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// typedPrepareParams is the typed form of a prepared statement's parameters.
// The statement owns it for its whole life: vectors are allocated the first
// time a position is bound in a type and are overwritten in place by every
// later execution, so steady-state parameter binding allocates nothing. The
// process only borrows the batch and the conversion store.
type typedPrepareParams struct {
	mp *mpool.MPool
	// batch is what the process sees: column i is parameter i, or nil when
	// this execution has no typed value for it.
	batch *batch.Batch
	// slots own the vectors; a slot keeps its vector while the batch hides it.
	slots       []*vector.Vector
	conversions *process.TypedParamConversions
}

// release frees everything the statement owns. A process that still borrows
// the batch is detached first.
func (t *typedPrepareParams) release(procs ...*process.Process) {
	if t == nil || t.mp == nil {
		return
	}
	for _, proc := range procs {
		if proc != nil && t.batch != nil && proc.GetTypedPrepareParams() == t.batch {
			proc.SetBorrowedTypedPrepareParams(nil, nil)
		}
	}
	for i, vec := range t.slots {
		if vec != nil {
			vec.Free(t.mp)
			t.slots[i] = nil
		}
	}
	t.conversions.Free()
	*t = typedPrepareParams{}
}

// installTypedPrepareParams gives the process each bound parameter as one row
// of its source SQL type, so a ParamRef bound to that type is evaluated without
// a TEXT round trip. The values come from the TEXT transport, which holds the
// canonical rendering of a typed source (protocol value or user variable).
//
// A position is left without a typed value (nil column) when its binding is a
// string or unsupported type, or when its text does not parse exactly; the
// executor then keeps the TEXT transport for that position.
//
// With a statement-owned store the values are written into the store's
// vectors and the process borrows them. Without one (store == nil) the
// process gets a batch of its own.
func installTypedPrepareParams(proc *process.Process, bindings []plan2.PreparedSourceBinding, store *typedPrepareParams) error {
	params := proc.GetPrepareParams()
	if params == nil || params.Length() == 0 || len(bindings) == 0 {
		return nil
	}
	if store == nil {
		return installOwnedTypedPrepareParams(proc, params.Length(), bindings)
	}
	if store.mp != nil && store.mp != proc.Mp() {
		// The store is rewritten through this execution's pool.
		store.release(proc)
	}
	n := params.Length()
	if store.mp == nil {
		store.mp = proc.Mp()
		store.conversions = process.NewTypedParamConversions(store.mp)
	}
	if store.batch == nil || len(store.batch.Vecs) != n {
		for i := n; i < len(store.slots); i++ {
			if store.slots[i] != nil {
				store.slots[i].Free(store.mp)
			}
		}
		if cap(store.slots) >= n {
			store.slots = store.slots[:n]
		} else {
			store.slots = append(store.slots, make([]*vector.Vector, n-len(store.slots))...)
		}
		store.batch = batch.NewWithSize(n)
	}
	bat := store.batch
	clear(bat.Vecs)
	installed := false
	for _, binding := range bindings {
		pos := int(binding.Position)
		if pos < 0 || pos >= n || params.IsNull(uint64(pos)) {
			continue
		}
		raw, err := proc.GetPrepareParamsAt(pos)
		if err != nil {
			return err
		}
		ok, err := setTypedPrepareParam(&store.slots[pos], proc, binding.Type, raw, store.mp)
		if err != nil {
			return err
		}
		if ok {
			bat.Vecs[pos] = store.slots[pos]
			installed = true
		}
	}
	if !installed {
		return nil
	}
	bat.SetRowCount(1)
	if err := store.conversions.Refresh(bat); err != nil {
		return err
	}
	proc.SetBorrowedTypedPrepareParams(bat, store.conversions)
	return nil
}

func installOwnedTypedPrepareParams(proc *process.Process, n int, bindings []plan2.PreparedSourceBinding) error {
	var typed *batch.Batch
	for _, binding := range bindings {
		pos := int(binding.Position)
		if pos < 0 || pos >= n || proc.GetPrepareParams().IsNull(uint64(pos)) {
			continue
		}
		raw, err := proc.GetPrepareParamsAt(pos)
		if err != nil {
			return err
		}
		var vec *vector.Vector
		ok, err := setTypedPrepareParam(&vec, proc, binding.Type, raw, proc.Mp())
		if err != nil {
			if typed != nil {
				typed.Clean(proc.Mp())
			}
			return err
		}
		if !ok {
			continue
		}
		if typed == nil {
			typed = batch.NewWithSize(n)
		}
		typed.Vecs[pos] = vec
	}
	if typed != nil {
		typed.SetRowCount(1)
		proc.SetTypedPrepareParams(typed)
	}
	return nil
}

// typedPrepareParam parses the canonical text of a typed parameter into a new
// constant one-row vector. ok is false when typ is not handled or the text is
// not an exact rendering of a typ value.
func typedPrepareParam(proc *process.Process, typ types.Type, text string) (*vector.Vector, bool, error) {
	var vec *vector.Vector
	ok, err := setTypedPrepareParam(&vec, proc, typ, []byte(text), proc.Mp())
	return vec, ok, err
}

// setFixedParam writes v into *slot, reusing the slot's vector when it already
// holds a constant of typ and allocating one only otherwise.
func setFixedParam[T any](slot **vector.Vector, typ types.Type, v T, mp *mpool.MPool) (bool, error) {
	if vec := *slot; vec != nil {
		if sameTypedParamSlotType(*vec.GetType(), typ) {
			// Empty it first so the value is written into its one-row buffer.
			vec.CleanOnlyData()
			return true, vector.SetConstFixed(vec, v, 1, mp)
		}
		vec.Free(mp)
		*slot = nil
	}
	vec, err := vector.NewConstFixed(typ, v, 1, mp)
	if err != nil {
		return false, err
	}
	*slot = vec
	return true, nil
}

func setBytesParam(slot **vector.Vector, typ types.Type, v []byte, mp *mpool.MPool) (bool, error) {
	if vec := *slot; vec != nil {
		if sameTypedParamSlotType(*vec.GetType(), typ) {
			// Drop the previous value but keep the capacity.
			vec.CleanOnlyData()
			return true, vector.SetConstBytes(vec, v, 1, mp)
		}
		vec.Free(mp)
		*slot = nil
	}
	vec, err := vector.NewConstBytes(typ, v, 1, mp)
	if err != nil {
		return false, err
	}
	*slot = vec
	return true, nil
}

func sameTypedParamSlotType(a, b types.Type) bool {
	return a.Oid == b.Oid && a.Width == b.Width && a.Scale == b.Scale
}

// setTypedPrepareParam parses the canonical text of a typed parameter into
// *slot, reusing the slot's vector when its type matches. ok is false when typ
// is not handled or the text is not an exact rendering of a typ value; *slot
// is then left as it was.
func setTypedPrepareParam(slot **vector.Vector, proc *process.Process, typ types.Type, raw []byte, mp *mpool.MPool) (bool, error) {
	// The parsers below do not retain their input, so the TEXT transport's
	// bytes are read in place.
	text := util.UnsafeBytesToString(raw)
	signed := func(bits int) (int64, bool) {
		v, err := strconv.ParseInt(text, 10, bits)
		return v, err == nil
	}
	unsigned := func(bits int) (uint64, bool) {
		v, err := strconv.ParseUint(text, 10, bits)
		return v, err == nil
	}

	// typeswitch:partial a type without a case keeps the TEXT transport, which is always correct
	switch typ.Oid {
	case types.T_int8:
		if v, ok := signed(8); ok {
			return setFixedParam(slot, typ, int8(v), mp)
		}
	case types.T_int16:
		if v, ok := signed(16); ok {
			return setFixedParam(slot, typ, int16(v), mp)
		}
	case types.T_int32:
		if v, ok := signed(32); ok {
			return setFixedParam(slot, typ, int32(v), mp)
		}
	case types.T_int64:
		if v, ok := signed(64); ok {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_uint8:
		if v, ok := unsigned(8); ok {
			return setFixedParam(slot, typ, uint8(v), mp)
		}
	case types.T_uint16:
		if v, ok := unsigned(16); ok {
			return setFixedParam(slot, typ, uint16(v), mp)
		}
	case types.T_uint32:
		if v, ok := unsigned(32); ok {
			return setFixedParam(slot, typ, uint32(v), mp)
		}
	case types.T_uint64:
		if v, ok := unsigned(64); ok {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_float32:
		if v, err := strconv.ParseFloat(text, 32); err == nil {
			return setFixedParam(slot, typ, float32(v), mp)
		}
	case types.T_float64:
		if v, err := strconv.ParseFloat(text, 64); err == nil {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_bool:
		if v, err := strconv.ParseBool(text); err == nil {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_decimal64:
		if v, err := types.ParseDecimal64(text, typ.Width, typ.Scale); err == nil {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_decimal128:
		if v, err := types.ParseDecimal128(text, typ.Width, typ.Scale); err == nil {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_decimal256:
		if v, err := types.ParseDecimal256(text, typ.Width, typ.Scale); err == nil {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_bit:
		// The protocol renders BIT as its unsigned value.
		if v, ok := unsigned(64); ok {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_date:
		if v, err := types.ParseDateCast(text); err == nil {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_datetime:
		if v, err := types.ParseDatetime(text, typ.Scale); err == nil {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_timestamp:
		loc := proc.GetSessionInfo().TimeZone
		if loc == nil {
			loc = time.Local
		}
		if v, err := types.ParseTimestamp(loc, text, typ.Scale); err == nil {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_time:
		if v, err := types.ParseTime(text, typ.Scale); err == nil {
			return setFixedParam(slot, typ, v, mp)
		}
	case types.T_json:
		// The JSON parser may keep substrings of its input: give it a copy.
		bj, err := types.ParseStringToByteJson(string(raw))
		if err != nil {
			return false, nil
		}
		encoded, err := types.EncodeJson(bj)
		if err != nil {
			return false, nil
		}
		return setBytesParam(slot, typ, encoded, mp)
	}
	return false, nil
}
