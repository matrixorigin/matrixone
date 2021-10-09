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
	Int8Add = addGeneric[int8]
	Int8AddScalar = addScalarGeneric[int8]
	Int16Add = addGeneric[int16]
	Int16AddScalar = addScalarGeneric[int16]
	Int32Add = addGeneric[int32]
	Int32AddScalar = addScalarGeneric[int32]
	Int64Add = addGeneric[int64]
	Int64AddScalar = addScalarGeneric[int64]
	Uint8Add = addGeneric[uint8]
	Uint8AddScalar = addScalarGeneric[uint8]
	Uint16Add = addGeneric[uint16]
	Uint16AddScalar = addScalarGeneric[uint16]
	Uint32Add = addGeneric[uint32]
	Uint32AddScalar = addScalarGeneric[uint32]
	Uint64Add = addGeneric[uint64]
	Uint64AddScalar = addScalarGeneric[uint64]
	Float32Add = addGeneric[float32]
	Float32AddScalar = addScalarGeneric[float32]
	Float64Add = addGeneric[float64]
	Float64AddScalar = addScalarGeneric[float64]
	Int8AddSels = addSelsGeneric[int8]
	Int8AddScalarSels = addScalarSelsGeneric[int8]
	Int16AddSels = addSelsGeneric[int16]
	Int16AddScalarSels = addScalarSelsGeneric[int16]
	Int32AddSels = addSelsGeneric[int32]
	Int32AddScalarSels = addScalarSelsGeneric[int32]
	Int64AddSels = addSelsGeneric[int64]
	Int64AddScalarSels = addScalarSelsGeneric[int64]
	Uint8AddSels = addSelsGeneric[uint8]
	Uint8AddScalarSels = addScalarSelsGeneric[uint8]
	Uint16AddSels = addSelsGeneric[uint16]
	Uint16AddScalarSels = addScalarSelsGeneric[uint16]
	Uint32AddSels = addSelsGeneric[uint32]
	Uint32AddScalarSels = addScalarSelsGeneric[uint32]
	Uint64AddSels = addSelsGeneric[uint64]
	Uint64AddScalarSels = addScalarSelsGeneric[uint64]
	Float32AddSels = addSelsGeneric[float32]
	Float32AddScalarSels = addScalarSelsGeneric[float32]
	Float64AddSels = addSelsGeneric[float64]
	Float64AddScalarSels = addScalarSelsGeneric[float64]
}
