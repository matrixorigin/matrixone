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

package hashtable

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

func Crc32BytesHashAsm(data unsafe.Pointer, length int) uint64
func Crc32IntHashAsm(data uint64) uint64
func Crc32IntSliceHashAsm(data *uint64, length int) uint64

func Crc32Int8BatchHashAsm(data *uint8, hashes *uint64, length int)
func Crc32Int16BatchHashAsm(data *uint16, hashes *uint64, length int)
func Crc32Int32BatchHashAsm(data *uint32, hashes *uint64, length int)
func Crc32Int64BatchHashAsm(data *uint64, hashes *uint64, length int)
func Crc32Int128BatchHashAsm(data *[2]uint64, hashes *uint64, length int)
func Crc32Int192BatchHashAsm(data *[3]uint64, hashes *uint64, length int)
func Crc32Int256BatchHashAsm(data *[4]uint64, hashes *uint64, length int)
func Crc32Int320BatchHashAsm(data *[5]uint64, hashes *uint64, length int)

func init() {
	if cpu.X86.HasSSE42 {
		BytesHash = Crc32BytesHashAsm
		IntHash = Crc32IntHashAsm
	} else {
		BytesHash = wyhash
		IntHash = intHash64
	}
}
