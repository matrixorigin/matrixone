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

package types

func (_ Bool) Size() int {
	return 1
}

func (_ Int8) Size() int {
	return 1
}

func (_ Int16) Size() int {
	return 2
}

func (_ Int32) Size() int {
	return 4
}

func (_ Int64) Size() int {
	return 8
}

func (_ UInt8) Size() int {
	return 1
}

func (_ UInt16) Size() int {
	return 2
}

func (_ UInt32) Size() int {
	return 4
}

func (_ UInt64) Size() int {
	return 8
}

func (_ Float32) Size() int {
	return 4
}

func (_ Float64) Size() int {
	return 8
}

func (_ Date) Size() int {
	return 4
}

func (_ Datetime) Size() int {
	return 8
}

func (_ Timestamp) Size() int {
	return 8
}

func (_ Decimal64) Size() int {
	return 8
}

func (_ Decimal128) Size() int {
	return 16
}

func (s String) Size() int {
	return len(s)
}
