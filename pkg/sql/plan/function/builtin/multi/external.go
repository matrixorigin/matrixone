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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/external"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type tpNumber interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64 | bool | types.Date
}
type tpTime interface {
	types.Timestamp | types.Datetime
}
type tpDecimal interface {
	types.Decimal64 | types.Decimal128
}

func ParseNumber[T tpNumber](vectors []*vector.Vector, proc *process.Process, cb func(xs []string, nsp *nulls.Nulls, rs []T) ([]T, error)) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[T](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = cb(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseTime[T tpTime](vectors []*vector.Vector, proc *process.Process, cb func(xs []string, nsp *nulls.Nulls, precision int32, rs []T) ([]T, error)) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[T](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = cb(xs, outputVec.Nsp, outputVec.Typ.Precision, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseDecimal[T tpDecimal](vectors []*vector.Vector, proc *process.Process, cb func(xs []string, nsp *nulls.Nulls, width int32, scale int32, rs []T) ([]T, error)) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[T](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := external.ParseNullFlagNormal(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = cb(xs, outputVec.Nsp, outputVec.Typ.Width, outputVec.Typ.Scale, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}
func ParseString(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := external.ParseNullFlagStrings(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	for i, r := range xs {
		err := vector.SetStringAt(outputVec, i, r, proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return outputVec, nil
}

func ParseJson(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustBytesCols(outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = external.TrimSpace(xs)
	nullFlags := external.ParseNullFlagStrings(xs, nullList)
	external.InsertNsp(nullFlags, outputVec.Nsp)
	rs, err = external.ParseJson(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	for i, r := range rs {
		err = vector.SetBytesAt(outputVec, i, r, proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return outputVec, nil
}
