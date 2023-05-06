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
	"runtime"
	"unsafe"

	"golang.org/x/sys/cpu"
)

func crc32Int64BatchHash(data unsafe.Pointer, hashes *uint64, length int)
func crc32Int64CellBatchHash(data unsafe.Pointer, hashes *uint64, length int)

func aesBytesBatchGenHashStates(data *[]byte, states *[3]uint64, length int)
func aesInt192BatchGenHashStates(data *[3]uint64, states *[3]uint64, length int)
func aesInt256BatchGenHashStates(data *[4]uint64, states *[3]uint64, length int)
func aesInt320BatchGenHashStates(data *[5]uint64, states *[3]uint64, length int)

func init() {
	if cpu.ARM64.HasCRC32 || (runtime.GOARCH == "arm64" && runtime.GOOS == "darwin") {
		Int64BatchHash = crc32Int64BatchHash
		Int64CellBatchHash = crc32Int64CellBatchHash
	}

	if cpu.ARM64.HasAES || (runtime.GOARCH == "arm64" && runtime.GOOS == "darwin") {
		BytesBatchGenHashStates = aesBytesBatchGenHashStates
		Int192BatchGenHashStates = aesInt192BatchGenHashStates
		Int256BatchGenHashStates = aesInt256BatchGenHashStates
		Int320BatchGenHashStates = aesInt320BatchGenHashStates
	}
}
