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

package functionUtil

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func ConvertD64ToD128(v types.Decimal64) types.Decimal128 {
	x := types.Decimal128{B0_63: uint64(v), B64_127: 0}
	if v>>63 != 0 {
		x.B64_127 = ^x.B64_127
	}
	return x
}

func QuickStrToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func QuickBytesToStr(data []byte) string {
	if data == nil {
		return ""
	}
	return *(*string)(unsafe.Pointer(&data))
}
