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

//go:build arm64
// +build arm64

package add

func init() {
	Int8Add = int8Add
	Int8AddScalar = int8AddScalar
	Int16Add = int16Add
	Int16AddScalar = int16AddScalar
	Int32Add = int32Add
	Int32AddScalar = int32AddScalar
	Int64Add = int64Add
	Int64AddScalar = int64AddScalar
	Uint8Add = uint8Add
	Uint8AddScalar = uint8AddScalar
	Uint16Add = uint16Add
	Uint16AddScalar = uint16AddScalar
	Uint32Add = uint32Add
	Uint32AddScalar = uint32AddScalar
	Uint64Add = uint64Add
	Uint64AddScalar = uint64AddScalar
	Float32Add = float32Add
	Float32AddScalar = float32AddScalar
	Float64Add = float64Add
	Float64AddScalar = float64AddScalar
	Int8AddSels = int8AddSels
	Int8AddScalarSels = int8AddScalarSels
	Int16AddSels = int16AddSels
	Int16AddScalarSels = int16AddScalarSels
	Int32AddSels = int32AddSels
	Int32AddScalarSels = int32AddScalarSels
	Int64AddSels = int64AddSels
	Int64AddScalarSels = int64AddScalarSels
	Uint8AddSels = uint8AddSels
	Uint8AddScalarSels = uint8AddScalarSels
	Uint16AddSels = uint16AddSels
	Uint16AddScalarSels = uint16AddScalarSels
	Uint32AddSels = uint32AddSels
	Uint32AddScalarSels = uint32AddScalarSels
	Uint64AddSels = uint64AddSels
	Uint64AddScalarSels = uint64AddScalarSels
	Float32AddSels = float32AddSels
	Float32AddScalarSels = float32AddScalarSels
	Float64AddSels = float64AddSels
	Float64AddScalarSels = float64AddScalarSels

	Int32Int64Add = int32Int64Add
	Int32Int64AddScalar = int32Int64AddScalar
	Int32Int64AddSels = int32Int64AddSels
	Int32Int64AddScalarSels = int32Int64AddScalarSels
	Int16Int64Add = int16Int64Add
	Int16Int64AddScalar = int16Int64AddScalar
	Int16Int64AddSels = int16Int64AddSels
	Int16Int64AddScalarSels = int16Int64AddScalarSels
	Int8Int64Add = int8Int64Add
	Int8Int64AddScalar = int8Int64AddScalar
	Int8Int64AddSels = int8Int64AddSels
	Int8Int64AddScalarSels = int8Int64AddScalarSels
	Int16Int32Add = int16Int32Add
	Int16Int32AddScalar = int16Int32AddScalar
	Int16Int32AddSels = int16Int32AddSels
	Int16Int32AddScalarSels = int16Int32AddScalarSels
	Int8Int32Add = int8Int32Add
	Int8Int32AddScalar = int8Int32AddScalar
	Int8Int32AddSels = int8Int32AddSels
	Int8Int32AddScalarSels = int8Int32AddScalarSels
	Int8Int16Add = int8Int16Add
	Int8Int16AddScalar = int8Int16AddScalar
	Int8Int16AddSels = int8Int16AddSels
	Int8Int16AddScalarSels = int8Int16AddScalarSels
	Float32Float64Add = float32Float64Add
	Float32Float64AddScalar = float32Float64AddScalar
	Float32Float64AddSels = float32Float64AddSels
	Float32Float64AddScalarSels = float32Float64AddScalarSels
	Uint32Uint64Add = uint32Uint64Add
	Uint32Uint64AddScalar = uint32Uint64AddScalar
	Uint32Uint64AddSels = uint32Uint64AddSels
	Uint32Uint64AddScalarSels = uint32Uint64AddScalarSels
	Uint16Uint64Add = uint16Uint64Add
	Uint16Uint64AddScalar = uint16Uint64AddScalar
	Uint16Uint64AddSels = uint16Uint64AddSels
	Uint16Uint64AddScalarSels = uint16Uint64AddScalarSels
	Uint8Uint64Add = uint8Uint64Add
	Uint8Uint64AddScalar = uint8Uint64AddScalar
	Uint8Uint64AddSels = uint8Uint64AddSels
	Uint8Uint64AddScalarSels = uint8Uint64AddScalarSels
	Uint16Uint32Add = uint16Uint32Add
	Uint16Uint32AddScalar = uint16Uint32AddScalar
	Uint16Uint32AddSels = uint16Uint32AddSels
	Uint16Uint32AddScalarSels = uint16Uint32AddScalarSels
	Uint8Uint32Add = uint8Uint32Add
	Uint8Uint32AddScalar = uint8Uint32AddScalar
	Uint8Uint32AddSels = uint8Uint32AddSels
	Uint8Uint32AddScalarSels = uint8Uint32AddScalarSels
	Uint8Uint16Add = uint8Uint16Add
	Uint8Uint16AddScalar = uint8Uint16AddScalar
	Uint8Uint16AddSels = uint8Uint16AddSels
	Uint8Uint16AddScalarSels = uint8Uint16AddScalarSels
}