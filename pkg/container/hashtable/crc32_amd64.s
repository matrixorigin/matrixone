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

//go:build amd64
// +build amd64

#include "textflag.h"

// func crc32BytesHashAsm(data []byte) uint64
// Requires: SSE4.2
TEXT ·crc32BytesHashAsm(SB), NOSPLIT, $0-32
	MOVQ data_base+0(FP), AX
	MOVQ AX, DX
	ADDQ data_len+8(FP), DX
	MOVQ $-1, CX

loop:
	CMPQ   AX, DX
	JGE    looptail
	CRC32Q (AX), CX
	ADDQ   $8, AX
	JMP    loop

looptail:
	JE     done
	CRC32Q -8(DX), CX

done:
	MOVQ CX, ret+24(FP)
	RET

// func crc32IntHashAsm(data uint64) uint64
// Requires: SSE4.2
TEXT ·crc32IntHashAsm(SB), NOSPLIT, $0-16
	CRC32Q data+0(FP), AX
	MOVQ   AX, ret+8(FP)
	RET
