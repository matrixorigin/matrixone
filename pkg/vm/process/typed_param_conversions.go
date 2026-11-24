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

package process

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// TypedParamConversions holds typed prepared parameters materialized in a
// narrower integer type than their source. The planner binds an integer
// parameter to its consumer's integer type when the value fits, so every
// expression executor of an execution reads the same value in that type.
//
// A prepared statement owns one store for its lifetime: an entry is allocated
// the first time a (position, type) is read and is then overwritten in place by
// Refresh at every later execution, so steady-state executions allocate
// nothing. A process that installs typed parameters it owns gets a private
// store, freed with them.
//
// Parallel pipelines share Process.Base, so access is serialized.
type TypedParamConversions struct {
	mu   sync.Mutex
	mp   *mpool.MPool
	vecs []typedParamConversion
}

type typedParamConversion struct {
	pos int
	typ types.Type
	vec *vector.Vector
	// valid is false while this execution's value does not fit typ.
	valid bool
}

// NewTypedParamConversions returns an empty store whose vectors come from mp.
func NewTypedParamConversions(mp *mpool.MPool) *TypedParamConversions {
	return &TypedParamConversions{mp: mp}
}

// Free releases every converted value.
func (c *TypedParamConversions) Free() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.vecs {
		c.vecs[i].vec.Free(c.mp)
	}
	c.vecs = nil
}

// Refresh rewrites every converted value, in place, from the typed parameters
// of a new execution. Call it before the execution reads any parameter.
func (c *TypedParamConversions) Refresh(params *batch.Batch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.vecs {
		conversion := &c.vecs[i]
		conversion.valid = false
		if params == nil || conversion.pos >= len(params.Vecs) || params.Vecs[conversion.pos] == nil {
			continue
		}
		bits, ok := TypedIntegerParamBits(params.Vecs[conversion.pos], conversion.typ)
		if !ok {
			continue
		}
		if err := SetIntegerParamConst(conversion.vec, bits, c.mp); err != nil {
			return err
		}
		conversion.valid = true
	}
	return nil
}

func (c *TypedParamConversions) get(pos int, typ types.Type, src *vector.Vector) (*vector.Vector, error) {
	bits, ok := TypedIntegerParamBits(src, typ)
	if !ok {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.vecs {
		if c.vecs[i].pos == pos && c.vecs[i].typ.Oid == typ.Oid {
			if !c.vecs[i].valid {
				return nil, nil
			}
			return c.vecs[i].vec, nil
		}
	}
	// First read of this (position, type): the only allocation it ever makes.
	vec, err := NewIntegerParamConst(typ, bits, c.mp)
	if err != nil {
		return nil, err
	}
	c.vecs = append(c.vecs, typedParamConversion{pos: pos, typ: typ, vec: vec, valid: true})
	return vec, nil
}

// setTypedPrepareParamsWithConversions installs params together with their
// conversion store. An owned install gets a private store (freed with params);
// a borrowed install uses the lender's store, which the lender frees.
func (proc *Process) setTypedPrepareParamsWithConversions(params *batch.Batch, owned bool, conversions *TypedParamConversions) {
	old := proc.Base.typedPrepareParams
	if old != params {
		if proc.Base.typedPrepareParamsOwned {
			if old != nil {
				old.Clean(proc.Mp())
			}
			proc.Base.typedPrepareParamConversions.Free()
		}
		proc.Base.typedPrepareParamConversions = nil
	}
	proc.Base.typedPrepareParams = params
	proc.Base.typedPrepareParamsOwned = owned && params != nil
	if conversions != nil {
		proc.Base.typedPrepareParamConversions = conversions
	}
	if params == nil {
		proc.Base.typedPrepareParamConversions = nil
	} else if proc.Base.typedPrepareParamConversions == nil && proc.Base.typedPrepareParamsOwned {
		proc.Base.typedPrepareParamConversions = NewTypedParamConversions(proc.Mp())
	}
}

// SetBorrowedTypedPrepareParams installs typed parameters owned by the caller
// (a prepared statement), together with the caller's conversion store. The
// process never frees either; the caller must keep both alive while the
// process uses them and refresh the store for every new set of values.
func (proc *Process) SetBorrowedTypedPrepareParams(params *batch.Batch, conversions *TypedParamConversions) {
	proc.setTypedPrepareParamsWithConversions(params, false, conversions)
}

// GetTypedPrepareParamAsInteger returns parameter i as a one-row constant of
// integer type typ, converted once per execution and owned by the conversion
// store. It returns nil when parameter i has no typed integer value, when the
// value does not fit typ (the caller then runs the CAST, which reports the
// error), or when this process has no conversion store. Callers must treat the
// vector as read-only.
func (proc *Process) GetTypedPrepareParamAsInteger(i int, typ types.Type) (*vector.Vector, error) {
	src := proc.GetTypedPrepareParam(i)
	conversions := proc.Base.typedPrepareParamConversions
	if src == nil || conversions == nil {
		return nil, nil
	}
	return conversions.get(i, typ, src)
}

// TypedIntegerParamBits reads a typed integer parameter for an integer target
// type. ok is false unless both types are integers and the value is in the
// target's range; the value is then returned as its two's-complement bits.
// Within range, CAST between integer types is exact and diagnostic-free.
func TypedIntegerParamBits(src *vector.Vector, typ types.Type) (bits uint64, ok bool) {
	from, to := src.GetType().Oid, typ.Oid
	if !from.IsInteger() || !to.IsInteger() || src.IsNull(0) {
		return 0, false
	}
	var signed int64
	var unsigned uint64
	switch from {
	case types.T_int8:
		signed = int64(vector.GetFixedAtNoTypeCheck[int8](src, 0))
	case types.T_int16:
		signed = int64(vector.GetFixedAtNoTypeCheck[int16](src, 0))
	case types.T_int32:
		signed = int64(vector.GetFixedAtNoTypeCheck[int32](src, 0))
	case types.T_int64:
		signed = vector.GetFixedAtNoTypeCheck[int64](src, 0)
	case types.T_uint8:
		unsigned = uint64(vector.GetFixedAtNoTypeCheck[uint8](src, 0))
	case types.T_uint16:
		unsigned = uint64(vector.GetFixedAtNoTypeCheck[uint16](src, 0))
	case types.T_uint32:
		unsigned = uint64(vector.GetFixedAtNoTypeCheck[uint32](src, 0))
	case types.T_uint64:
		unsigned = vector.GetFixedAtNoTypeCheck[uint64](src, 0)
	}
	negative := false
	if from.IsSignedInt() {
		negative = signed < 0
		unsigned = uint64(signed)
	}
	width := uint(to.TypeLen() * 8)
	if to.IsSignedInt() {
		minValue, maxValue := int64(-1)<<(width-1), int64(1)<<(width-1)-1
		if negative {
			return unsigned, signed >= minValue
		}
		return unsigned, unsigned <= uint64(maxValue)
	}
	if negative {
		return 0, false
	}
	return unsigned, width == 64 || unsigned < uint64(1)<<width
}

// NewIntegerParamConst builds a one-row constant of integer type typ from the
// two's-complement bits of an in-range value.
func NewIntegerParamConst(typ types.Type, bits uint64, mp *mpool.MPool) (*vector.Vector, error) {
	switch typ.Oid {
	case types.T_int8:
		return vector.NewConstFixed(typ, int8(bits), 1, mp)
	case types.T_int16:
		return vector.NewConstFixed(typ, int16(bits), 1, mp)
	case types.T_int32:
		return vector.NewConstFixed(typ, int32(bits), 1, mp)
	case types.T_int64:
		return vector.NewConstFixed(typ, int64(bits), 1, mp)
	case types.T_uint8:
		return vector.NewConstFixed(typ, uint8(bits), 1, mp)
	case types.T_uint16:
		return vector.NewConstFixed(typ, uint16(bits), 1, mp)
	case types.T_uint32:
		return vector.NewConstFixed(typ, uint32(bits), 1, mp)
	case types.T_uint64:
		return vector.NewConstFixed(typ, bits, 1, mp)
	}
	return nil, moerr.NewInternalErrorNoCtxf("not an integer parameter type %s", typ.String())
}

// SetIntegerParamConst overwrites the value of a one-row integer constant made
// by NewIntegerParamConst; it reuses the vector's memory.
func SetIntegerParamConst(vec *vector.Vector, bits uint64, mp *mpool.MPool) error {
	// Empty it first so the value is written into its one-row buffer.
	vec.CleanOnlyData()
	switch vec.GetType().Oid {
	case types.T_int8:
		return vector.SetConstFixed(vec, int8(bits), 1, mp)
	case types.T_int16:
		return vector.SetConstFixed(vec, int16(bits), 1, mp)
	case types.T_int32:
		return vector.SetConstFixed(vec, int32(bits), 1, mp)
	case types.T_int64:
		return vector.SetConstFixed(vec, int64(bits), 1, mp)
	case types.T_uint8:
		return vector.SetConstFixed(vec, uint8(bits), 1, mp)
	case types.T_uint16:
		return vector.SetConstFixed(vec, uint16(bits), 1, mp)
	case types.T_uint32:
		return vector.SetConstFixed(vec, uint32(bits), 1, mp)
	case types.T_uint64:
		return vector.SetConstFixed(vec, bits, 1, mp)
	}
	return moerr.NewInternalErrorNoCtxf("not an integer parameter type %s", vec.GetType().String())
}
