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

//go:build !(amd64 || arm64)

package hashtable

import (
	"unsafe"
)

func Crc32Int64BatchHash(data unsafe.Pointer, hashes *uint64, length int)     {}
func Crc32Int64CellBatchHash(data unsafe.Pointer, hashes *uint64, length int) {}

func AesBytesBatchGenHashStates(data *[]byte, states *[3]uint64, length int)     {}
func AesInt192BatchGenHashStates(data *[3]uint64, states *[3]uint64, length int) {}
func AesInt256BatchGenHashStates(data *[4]uint64, states *[3]uint64, length int) {}
func AesInt320BatchGenHashStates(data *[5]uint64, states *[3]uint64, length int) {}
