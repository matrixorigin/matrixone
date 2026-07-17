// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mo_cgo_native

/*
#include "mo.h"
*/
import "C"

// BitmapCount references libmo without declaring its native link dependencies.
// The external test wrapper owns that link closure for its transitive caller.
func BitmapCount(value uint64) uint64 {
	word := C.uint64_t(value)
	return uint64(C.Bitmap_Count(&word, 64))
}
