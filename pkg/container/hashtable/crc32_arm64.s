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

//go:build arm64
// +build arm64

#include "textflag.h"

// func crc32BytesHashAsm(data []byte) uint64
// Requires: CRC32
TEXT ·crc32HashAsm(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R9
	MOVD data_len+8(FP), R11
	MOVD R9, R13
	ADD  R11, R13
	MOVD $-1, R11

loop:
	CMPQ    R9, R13
	BGE     looptail
	MOVD.P  8(R9), R10
	CRC32CX R10, R11
	JMP     loop

looptail:
	BE      done
	MOVD    -8(R13), R10
	CRC32CX R10, R11

done:
	MOVD    R11, ret+24(FP)
	RET

// func crc32IntHashAsm(data uint64) uint64
// Requires: SSE4.2
TEXT ·crc32IntHashAsm(SB), NOSPLIT, $0-16
	MOVD    data_base+0(FP), R9
	CRC32CX R9, R13
	MOVD    R13, ret+8(FP)
	RET
