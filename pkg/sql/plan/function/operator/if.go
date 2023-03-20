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
		return ifGeneral[bool](vs, proc, types.T_bool.ToType())
	}

	IfUint8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[uint8](vs, proc, types.T_uint8.ToType())
	}

	IfUint16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[uint16](vs, proc, types.T_uint16.ToType())
	}

	IfUint32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[uint32](vs, proc, types.T_uint32.ToType())
	}

	IfUint64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[uint64](vs, proc, types.T_uint64.ToType())
	}

	IfInt8 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[int8](vs, proc, types.T_int8.ToType())
	}

	IfInt16 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[int16](vs, proc, types.T_int16.ToType())
	}

	IfInt32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[int32](vs, proc, types.T_int32.ToType())
	}

	IfInt64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[int64](vs, proc, types.T_int64.ToType())
	}

	IfFloat32 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[float32](vs, proc, types.T_float32.ToType())
	}

	IfFloat64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[float64](vs, proc, types.T_float64.ToType())
	}

	IfDecimal64 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Decimal64](vs, proc, types.T_decimal64.ToType())
	}

	IfDecimal128 = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Decimal128](vs, proc, types.T_decimal128.ToType())
	}

	IfDate = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Date](vs, proc, types.T_date.ToType())
	}

	IfTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Time](vs, proc, types.T_time.ToType())
	}

	IfDateTime = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Datetime](vs, proc, types.T_datetime.ToType())
	}

	IfVarchar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.T_varchar.ToType())
	}

	IfChar = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.T_char.ToType())
	}

	IfTimestamp = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifGeneral[types.Timestamp](vs, proc, types.T_timestamp.ToType())
	}

	IfBlob = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.T_blob.ToType())
	}

	IfText = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.T_text.ToType())
	}

	IfJson = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.T_json.ToType())
	}

	IfUuid = func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
		return ifForString(vs, proc, types.T_uuid.ToType())
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
