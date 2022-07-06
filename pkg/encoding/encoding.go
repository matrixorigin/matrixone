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

package encoding

import (
	"unsafe"
)

func EncodeSlice[T any](v []T, sz int) (ret []byte) {
	if len(v) > 0 {
		ret = unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), cap(v)*sz)[:len(v)*sz]
	}
	return
}

func DecodeSlice[T any](v []byte, sz int) (ret []T) {
	if len(v) > 0 {
		ret = unsafe.Slice((*T)(unsafe.Pointer(&v[0])), cap(v)/sz)[:len(v)/sz]
	}
	return
}
