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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// If operator supported format like that
//
//	If(<boolean operator>, <value operator>, <value operator>)
var (
	IfBool = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[bool](vs, proc, types.Type{Oid: types.T_bool})
	}

	IfUint8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[uint8](vs, proc, types.Type{Oid: types.T_uint8})
	}

	IfUint16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[uint16](vs, proc, types.Type{Oid: types.T_uint16})
	}

	IfUint32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[uint32](vs, proc, types.Type{Oid: types.T_uint32})
	}

	IfUint64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[uint64](vs, proc, types.Type{Oid: types.T_uint64})
	}

	IfInt8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[int8](vs, proc, types.Type{Oid: types.T_int8})
	}

	IfInt16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[int16](vs, proc, types.Type{Oid: types.T_int16})
	}

	IfInt32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[int32](vs, proc, types.Type{Oid: types.T_int32})
	}

	IfInt64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[int64](vs, proc, types.Type{Oid: types.T_int64})
	}

	IfFloat32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[float32](vs, proc, types.Type{Oid: types.T_float32})
	}

	IfFloat64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[float64](vs, proc, types.Type{Oid: types.T_float64})
	}

	IfDecimal64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Decimal64](vs, proc, types.Type{Oid: types.T_decimal64})
	}

	IfDecimal128 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Decimal128](vs, proc, types.Type{Oid: types.T_decimal128})
	}

	IfDate = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Date](vs, proc, types.Type{Oid: types.T_date})
	}

	IfTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Time](vs, proc, types.Type{Oid: types.T_time})
	}

	IfDateTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Datetime](vs, proc, types.Type{Oid: types.T_datetime})
	}

	IfVarchar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.Type{Oid: types.T_varchar})
	}

	IfChar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.Type{Oid: types.T_char})
	}

	IfTimestamp = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Timestamp](vs, proc, types.Type{Oid: types.T_timestamp})
	}

	IfBlob = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.Type{Oid: types.T_blob})
	}

	IfText = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.Type{Oid: types.T_text})
	}

	IfJson = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.Type{Oid: types.T_json})
	}

	IfUuid = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.Type{Oid: types.T_uuid})
	}
)

func IfTypeCheckFn(inputTypes []types.T, _ []types.T, ret types.T) bool {
	if len(inputTypes) == 3 && inputTypes[0] == types.T_bool {
		if inputTypes[1] != ret && inputTypes[1] != types.T_any {
			return false
		}
		if inputTypes[2] != ret && inputTypes[2] != types.T_any {
			return false
		}
		return true
	}
	return false
}

type IfRet interface {
	constraints.Integer | constraints.Float | bool | types.Date | types.Datetime |
		types.Decimal64 | types.Decimal128 | types.Timestamp
}

func ifGeneral[T IfRet](vs []*vector.Vector, proc *process.Process, ret types.Type) (*vector.Vector, error) {
	return cwGeneral[T](vs, proc, ret)
}

func ifForString(vs []*vector.Vector, proc *process.Process, typ types.Type) (*vector.Vector, error) {
	return cwString(vs, proc, typ)
}
