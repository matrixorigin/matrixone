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

package util

import (
	_ "unsafe"
)

// fastrand32 returns a lock free uint32 value. Compared to rand.Uint32, this
// implementation scales. We're using the go runtime's implementation through a
// linker trick.
//
//go:linkname fastrand32 runtime.fastrand
func fastrand32() uint32

// Fastrand64 returns a lock free uint64 value.
// Compared to rand.Int63(), this implementation scales.
func Fastrand64() uint64 {
	x, y := fastrand32(), fastrand32() // 32-bit halves
	return uint64(x)<<32 ^ uint64(y)
}
