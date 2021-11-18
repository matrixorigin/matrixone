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

#include "textflag.h"

// func Crc32BytesHashAsm(data unsafe.Pointer, length int) uint64
// Requires: CRC32
TEXT ·Crc32BytesHashAsm(SB), NOSPLIT, $0-24
	MOVD    data+0(FP), R0
	MOVD    length+8(FP), R1
	MOVD    $-1, R2
	CRC32CX R1, R2
	ADD     R0, R1
	SUB     $8, R1

loop:
	CMP     R0, R1
	BLE     done
	MOVD.P  8(R0), R3
	CRC32CX R3, R2
	JMP     loop

done:
	MOVD    (R1), R3
	CRC32CX R3, R2
	MOVD    R2, ret+16(FP)
	RET

// func Crc32IntHashAsm(data uint64) uint64
// Requires: CRC32
TEXT ·Crc32IntHashAsm(SB), NOSPLIT, $0-16
	MOVD    data_base+0(FP), R0
	MOVD    $-1, R1
	CRC32CX R0, R1
	MOVD    R1, ret+8(FP)
	RET
