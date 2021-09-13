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
	Int8Min = int8Min
	Int16Min = int16Min
	Int32Min = int32Min
	Int64Min = int64Min
	Uint8Min = uint8Min
	Uint16Min = uint16Min
	Uint32Min = uint32Min
	Uint64Min = uint64Min
	Float32Min = float32Min
	Float64Min = float64Min
	StrMin = strMin

	BoolMinSels = boolMinSels
	Int8MinSels = int8MinSels
	Int16MinSels = int16MinSels
	Int32MinSels = int32MinSels
	Int64MinSels = int64MinSels
	Uint8MinSels = uint8MinSels
	Uint16MinSels = uint16MinSels
	Uint32MinSels = uint32MinSels
	Uint64MinSels = uint64MinSels
	Float32MinSels = float32MinSels
	Float64MinSels = float64MinSels
	StrMinSels = strMinSels
}
