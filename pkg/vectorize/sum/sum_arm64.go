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
	Int8Sum = sumSignedGeneric[int8]
	Int16Sum = sumSignedGeneric[int16]
	Int32Sum = sumSignedGeneric[int32]
	Int64Sum = sumSignedGeneric[int64]
	Uint8Sum = sumUnsignedGeneric[uint8]
	Uint16Sum = sumUnsignedGeneric[uint16]
	Uint32Sum = sumUnsignedGeneric[uint32]
	Uint64Sum = sumUnsignedGeneric[uint64]
	Float32Sum = sumFloatGeneric[float32]
	Float64Sum = sumFloatGeneric[float64]
	Int8SumSels = sumSignedSelsGeneric[int8]
	Int16SumSels = sumSignedSelsGeneric[int16]
	Int32SumSels = sumSignedSelsGeneric[int32]
	Int64SumSels = sumSignedSelsGeneric[int64]
	Uint8SumSels = sumUnsignedSelsGeneric[uint8]
	Uint16SumSels = sumUnsignedSelsGeneric[uint16]
	Uint32SumSels = sumUnsignedSelsGeneric[uint32]
	Uint64SumSels = sumUnsignedSelsGeneric[uint64]
	Float32SumSels = sumFloatSelsGeneric[float32]
	Float64SumSels = sumFloatSelsGeneric[float64]
}
