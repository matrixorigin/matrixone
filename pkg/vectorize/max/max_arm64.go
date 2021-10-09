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

package max

func init() {
	BoolMax = boolMax
	Int8Max = maxGeneric[int8]
	Int16Max = maxGeneric[int16]
	Int32Max = maxGeneric[int32]
	Int64Max = maxGeneric[int64]
	Uint8Max = maxGeneric[uint8]
	Uint16Max = maxGeneric[uint16]
	Uint32Max = maxGeneric[uint32]
	Uint64Max = maxGeneric[uint64]
	Float32Max = maxGeneric[float32]
	Float64Max = maxGeneric[float64]
	StrMax = strMax

	BoolMaxSels = boolMaxSels
	Int8MaxSels = maxSelsGeneric[int8]
	Int16MaxSels = maxSelsGeneric[int16]
	Int32MaxSels = maxSelsGeneric[int32]
	Int64MaxSels = maxSelsGeneric[int64]
	Uint8MaxSels = maxSelsGeneric[uint8]
	Uint16MaxSels = maxSelsGeneric[uint16]
	Uint32MaxSels = maxSelsGeneric[uint32]
	Uint64MaxSels = maxSelsGeneric[uint64]
	Float32MaxSels = maxSelsGeneric[float32]
	Float64MaxSels = maxSelsGeneric[float64]
	StrMaxSels = strMaxSels
}
