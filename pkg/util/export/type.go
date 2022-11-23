// Copyright 2022 Matrix Origin
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

package export

import (
	"io"
	"reflect"
	"unsafe"
)

// stringWriter same as io.stringWriter
type stringWriter interface {
	io.Writer
	io.StringWriter
	//WriteRune(rune) (int, error)
}

func String2Bytes(s string) (ret []byte) {
	sliceHead := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	strHead := (*reflect.StringHeader)(unsafe.Pointer(&s))

	sliceHead.Data = strHead.Data
	sliceHead.Len = strHead.Len
	sliceHead.Cap = strHead.Len
	return
}
