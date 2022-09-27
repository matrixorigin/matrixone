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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/external"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ParseBool(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[bool](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseBool(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseInt8(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[int8](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseInt8(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseInt16(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[int16](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseInt16(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseInt32(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[int32](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseInt32(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[int64](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseInt64(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseUint8(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[uint8](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseUint8(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseUint16(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[uint16](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseUint16(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseUint32(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[uint32](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseUint32(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseUint64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[uint64](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseUint64(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseFloat32(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[float32](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseFloat32(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseFloat64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[float64](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseFloat64(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseString(vectors []*vector.Vector, proc *process.Process) *vector.Vector {
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustStrCols(outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := external.ParseNullFlagStrings(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	copy(rs, xs)
	vector.SetCol(outputVec, rs)
	return outputVec
}

func ParseJson(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustBytesCols(outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseJson(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Date](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseDate(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseDateTime(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Datetime](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseDateTime(xs, outputVec.Nsp, outputVec.Typ.Precision, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseDecimal64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Decimal64](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseDecimal64(xs, outputVec.Nsp, outputVec.Typ.Width, outputVec.Typ.Scale, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseDecimal128(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Decimal128](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseDecimal128(xs, outputVec.Nsp, outputVec.Typ.Width, outputVec.Typ.Scale, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseTimeStamp(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Timestamp](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseTimeStamp(xs, outputVec.Nsp, outputVec.Typ.Precision, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}
