// Copyright 2021 Matrix Origin
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

//go:build arm64
// +build arm64

package hashtable

import (
	"golang.org/x/sys/cpu"
)

func crc32HashAsm(data []byte, tail uint64) uint64

func init() {
	if cpu.ARM64.HasCRC32 {
		BytesHash = crc32Hash
	} else {
		BytesHash = wyhash
	}
}

func crc32Hash(data []byte) uint64 {
	l := len(data)
	if l < 8 {
		return wyhash(data)
	}
	var rem uint64
	if l&7 != 0 {
		rem = r8(data, l-8)
	}
	return crc32HashAsm(data, rem)
}
