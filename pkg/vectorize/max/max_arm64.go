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
	Int8Max = int8Max
	Int16Max = int16Max
	Int32Max = int32Max
	Int64Max = int64Max
	Uint8Max = uint8Max
	Uint16Max = uint16Max
	Uint32Max = uint32Max
	Uint64Max = uint64Max
	Float32Max = float32Max
	Float64Max = float64Max
	StrMax = strMax

	BoolMaxSels = boolMaxSels
	Int8MaxSels = int8MaxSels
	Int16MaxSels = int16MaxSels
	Int32MaxSels = int32MaxSels
	Int64MaxSels = int64MaxSels
	Uint8MaxSels = uint8MaxSels
	Uint16MaxSels = uint16MaxSels
	Uint32MaxSels = uint32MaxSels
	Uint64MaxSels = uint64MaxSels
	Float32MaxSels = float32MaxSels
	Float64MaxSels = float64MaxSels
	StrMaxSels = strMaxSels
}
