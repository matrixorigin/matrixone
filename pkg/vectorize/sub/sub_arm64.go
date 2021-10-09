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

package sub

func init() {
	Int8Sub = subGeneric[int8]
	Int8SubScalar = subScalarGeneric[int8]
	Int8SubByScalar = subByScalarGeneric[int8]
	Int16Sub = subGeneric[int16]
	Int16SubScalar = subScalarGeneric[int16]
	Int16SubByScalar = subByScalarGeneric[int16]
	Int32Sub = subGeneric[int32]
	Int32SubScalar = subScalarGeneric[int32]
	Int32SubByScalar = subByScalarGeneric[int32]
	Int64Sub = subGeneric[int64]
	Int64SubScalar = subScalarGeneric[int64]
	Int64SubByScalar = subByScalarGeneric[int64]
	Uint8Sub = subGeneric[uint8]
	Uint8SubScalar = subScalarGeneric[uint8]
	Uint8SubByScalar = subByScalarGeneric[uint8]
	Uint16Sub = subGeneric[uint16]
	Uint16SubScalar = subScalarGeneric[uint16]
	Uint16SubByScalar = subByScalarGeneric[uint16]
	Uint32Sub = subGeneric[uint32]
	Uint32SubScalar = subScalarGeneric[uint32]
	Uint32SubByScalar = subByScalarGeneric[uint32]
	Uint64Sub = subGeneric[uint64]
	Uint64SubScalar = subScalarGeneric[uint64]
	Uint64SubByScalar = subByScalarGeneric[uint64]
	Float32Sub = subGeneric[float32]
	Float32SubScalar = subScalarGeneric[float32]
	Float32SubByScalar = subByScalarGeneric[float32]
	Float64Sub = subGeneric[float64]
	Float64SubScalar = subScalarGeneric[float64]
	Float64SubByScalar = subByScalarGeneric[float64]
	Int8SubSels = subSelsGeneric[int8]
	Int8SubScalarSels = subScalarSelsGeneric[int8]
	Int8SubByScalarSels = subByScalarSelsGeneric[int8]
	Int16SubSels = subSelsGeneric[int16]
	Int16SubScalarSels = subScalarSelsGeneric[int16]
	Int16SubByScalarSels = subByScalarSelsGeneric[int16]
	Int32SubSels = subSelsGeneric[int32]
	Int32SubScalarSels = subScalarSelsGeneric[int32]
	Int32SubByScalarSels = subByScalarSelsGeneric[int32]
	Int64SubSels = subSelsGeneric[int64]
	Int64SubScalarSels = subScalarSelsGeneric[int64]
	Int64SubByScalarSels = subByScalarSelsGeneric[int64]
	Uint8SubSels = subSelsGeneric[uint8]
	Uint8SubScalarSels = subScalarSelsGeneric[uint8]
	Uint8SubByScalarSels = subByScalarSelsGeneric[uint8]
	Uint16SubSels = subSelsGeneric[uint16]
	Uint16SubScalarSels = subScalarSelsGeneric[uint16]
	Uint16SubByScalarSels = subByScalarSelsGeneric[uint16]
	Uint32SubSels = subSelsGeneric[uint32]
	Uint32SubScalarSels = subScalarSelsGeneric[uint32]
	Uint32SubByScalarSels = subByScalarSelsGeneric[uint32]
	Uint64SubSels = subSelsGeneric[uint64]
	Uint64SubScalarSels = subScalarSelsGeneric[uint64]
	Uint64SubByScalarSels = subByScalarSelsGeneric[uint64]
	Float32SubSels = subSelsGeneric[float32]
	Float32SubScalarSels = subScalarSelsGeneric[float32]
	Float32SubByScalarSels = subByScalarSelsGeneric[float32]
	Float64SubSels = subSelsGeneric[float64]
	Float64SubScalarSels = subScalarSelsGeneric[float64]
	Float64SubByScalarSels = subByScalarSelsGeneric[float64]
}
