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

//go:build amd64 || arm64

package hashtable

import (
	"unsafe"
)

func Crc32BytesHash(data unsafe.Pointer, length int) uint64

func Crc32Int64Hash(data uint64) uint64
func Crc32Int64BatchHash(data unsafe.Pointer, hashes *uint64, length int)
func Crc32Int64CellBatchHash(data unsafe.Pointer, hashes *uint64, length int)

func Crc32Int192Hash(data *[3]uint64) uint64
func Crc32Int192BatchHash(data *[3]uint64, hashes *uint64, length int)

func Crc32Int256Hash(data *[4]uint64) uint64
func Crc32Int256BatchHash(data *[4]uint64, hashes *uint64, length int)

func Crc32Int320Hash(data *[5]uint64) uint64
func Crc32Int320BatchHash(data *[5]uint64, hashes *uint64, length int)

func AesBytesHash(data unsafe.Pointer, length int) [2]uint64

func AesInt192BatchHash(data *[3]uint64, hashes *uint64, length int)
func AesInt192BatchGenKey(data *[3]uint64, hashes *[2]uint64, length int)

func AesInt256BatchHash(data *[4]uint64, hashes *uint64, length int)
func AesInt256BatchGenKey(data *[4]uint64, hashes *[2]uint64, length int)

func AesInt320BatchHash(data *[5]uint64, hashes *uint64, length int)
func AesInt320BatchGenKey(data *[5]uint64, hashes *[2]uint64, length int)
