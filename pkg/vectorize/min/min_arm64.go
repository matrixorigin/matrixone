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

package min

func init() {
	BoolMin = boolMin
	Int8Min = minGeneric[int8]
	Int16Min = minGeneric[int16]
	Int32Min = minGeneric[int32]
	Int64Min = minGeneric[int64]
	Uint8Min = minGeneric[uint8]
	Uint16Min = minGeneric[uint16]
	Uint32Min = minGeneric[uint32]
	Uint64Min = minGeneric[uint64]
	Float32Min = minGeneric[float32]
	Float64Min = minGeneric[float64]
	StrMin = strMin

	BoolMinSels = boolMinSels
	Int8MinSels = minSelsGeneric[int8]
	Int16MinSels = minSelsGeneric[int16]
	Int32MinSels = minSelsGeneric[int32]
	Int64MinSels = minSelsGeneric[int64]
	Uint8MinSels = minSelsGeneric[uint8]
	Uint16MinSels = minSelsGeneric[uint16]
	Uint32MinSels = minSelsGeneric[uint32]
	Uint64MinSels = minSelsGeneric[uint64]
	Float32MinSels = minSelsGeneric[float32]
	Float64MinSels = minSelsGeneric[float64]
	StrMinSels = strMinSels
}
