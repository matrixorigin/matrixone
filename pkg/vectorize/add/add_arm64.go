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
}
