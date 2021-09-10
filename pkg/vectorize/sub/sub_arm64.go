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
	Int8Sub = int8Sub
	Int8SubScalar = int8SubScalar
	Int8SubByScalar = int8SubByScalar
	Int16Sub = int16Sub
	Int16SubScalar = int16SubScalar
	Int16SubByScalar = int16SubByScalar
	Int32Sub = int32Sub
	Int32SubScalar = int32SubScalar
	Int32SubByScalar = int32SubByScalar
	Int64Sub = int64Sub
	Int64SubScalar = int64SubScalar
	Int64SubByScalar = int64SubByScalar
	Uint8Sub = uint8Sub
	Uint8SubScalar = uint8SubScalar
	Uint8SubByScalar = uint8SubByScalar
	Uint16Sub = uint16Sub
	Uint16SubScalar = uint16SubScalar
	Uint16SubByScalar = uint16SubByScalar
	Uint32Sub = uint32Sub
	Uint32SubScalar = uint32SubScalar
	Uint32SubByScalar = uint32SubByScalar
	Uint64Sub = uint64Sub
	Uint64SubScalar = uint64SubScalar
	Uint64SubByScalar = uint64SubByScalar
	Float32Sub = float32Sub
	Float32SubScalar = float32SubScalar
	Float32SubByScalar = float32SubByScalar
	Float64Sub = float64Sub
	Float64SubScalar = float64SubScalar
	Float64SubByScalar = float64SubByScalar
	Int8SubSels = int8SubSels
	Int8SubScalarSels = int8SubScalarSels
	Int8SubByScalarSels = int8SubByScalarSels
	Int16SubSels = int16SubSels
	Int16SubScalarSels = int16SubScalarSels
	Int16SubByScalarSels = int16SubByScalarSels
	Int32SubSels = int32SubSels
	Int32SubScalarSels = int32SubScalarSels
	Int32SubByScalarSels = int32SubByScalarSels
	Int64SubSels = int64SubSels
	Int64SubScalarSels = int64SubScalarSels
	Int64SubByScalarSels = int64SubByScalarSels
	Uint8SubSels = uint8SubSels
	Uint8SubScalarSels = uint8SubScalarSels
	Uint8SubByScalarSels = uint8SubByScalarSels
	Uint16SubSels = uint16SubSels
	Uint16SubScalarSels = uint16SubScalarSels
	Uint16SubByScalarSels = uint16SubByScalarSels
	Uint32SubSels = uint32SubSels
	Uint32SubScalarSels = uint32SubScalarSels
	Uint32SubByScalarSels = uint32SubByScalarSels
	Uint64SubSels = uint64SubSels
	Uint64SubScalarSels = uint64SubScalarSels
	Uint64SubByScalarSels = uint64SubByScalarSels
	Float32SubSels = float32SubSels
	Float32SubScalarSels = float32SubScalarSels
	Float32SubByScalarSels = float32SubByScalarSels
	Float64SubSels = float64SubSels
	Float64SubScalarSels = float64SubScalarSels
	Float64SubByScalarSels = float64SubByScalarSels
}
