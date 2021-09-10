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

package sum

func init() {
	Int8Sum = int8Sum
	Int16Sum = int16Sum
	Int32Sum = int32Sum
	Int64Sum = int64Sum
	Uint8Sum = uint8Sum
	Uint16Sum = uint16Sum
	Uint32Sum = uint32Sum
	Uint64Sum = uint64Sum
	Float32Sum = float32Sum
	Float64Sum = float64Sum
	Int8SumSels = int8SumSels
	Int16SumSels = int16SumSels
	Int32SumSels = int32SumSels
	Int64SumSels = int64SumSels
	Uint8SumSels = uint8SumSels
	Uint16SumSels = uint16SumSels
	Uint32SumSels = uint32SumSels
	Uint64SumSels = uint64SumSels
	Float32SumSels = float32SumSels
	Float64SumSels = float64SumSels
}
