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

DATA aesIV<>+0x00(SB)/8, $0x5A8279996ED9EBA1
DATA aesIV<>+0x08(SB)/8, $0x8F1BBCDCCA62C1D6
DATA aesIV<>+0x10(SB)/8, $0x5A8279996ED9EBA1
DATA aesIV<>+0x18(SB)/8, $0x8F1BBCDCCA62C1D6
DATA aesIV<>+0x20(SB)/8, $0x5A8279996ED9EBA1
DATA aesIV<>+0x28(SB)/8, $0x8F1BBCDCCA62C1D6
DATA aesIV<>+0x30(SB)/8, $0x5A8279996ED9EBA1
DATA aesIV<>+0x38(SB)/8, $0x8F1BBCDCCA62C1D6
GLOBL aesIV<>(SB), (NOPTR+RODATA), $64

// func crc32BytesHashAsm(data unsafe.Pointer, length int) uint64
// Requires: SSE4.2
TEXT ·crc32BytesHashAsm(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), AX
	MOVQ length+8(FP), CX
	MOVQ CX, BX
	MOVQ $-1, DX
	ADDQ AX, CX
	SUBQ $8, CX

loop:
	CMPQ   AX, CX
	JGE    done
	CRC32Q (AX), DX
	ADDQ   $8, AX
	JMP    loop

done:
	CRC32Q (CX), DX
	MOVD   DX, ret+16(FP)
	MOVD   BX, ret+20(FP)
	RET

// func crc32Int64HashAsm(data uint64) uint64
// Requires: SSE4.2
TEXT ·crc32Int64HashAsm(SB), NOSPLIT, $0-16
	MOVQ   $-1, AX
	CRC32Q data+0(FP), AX
	MOVQ   AX, ret+8(FP)
	RET

// func crc32Int64BatchHashAsm(data *uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int64BatchHashAsm(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), AX
	MOVQ hashes+8(FP), DX
	MOVQ length+16(FP), CX

loop:
	SUBQ   $8, CX
	JL     tail
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	MOVQ   R8, (DX)
	MOVQ   $-1, R9
	CRC32Q 8(AX), R9
	MOVQ   R9, 8(DX)
	MOVQ   $-1, R10
	CRC32Q 16(AX), R10
	MOVQ   R10, 16(DX)
	MOVQ   $-1, R11
	CRC32Q 24(AX), R11
	MOVQ   R11, 24(DX)
	MOVQ   $-1, R12
	CRC32Q 32(AX), R12
	MOVQ   R12, 32(DX)
	MOVQ   $-1, R13
	CRC32Q 40(AX), R13
	MOVQ   R13, 40(DX)
	MOVQ   $-1, R14
	CRC32Q 48(AX), R14
	MOVQ   R14, 48(DX)
	MOVQ   $-1, R15
	CRC32Q 56(AX), R15
	MOVQ   R15, 56(DX)
	ADDQ   $64, AX
	ADDQ   $64, DX
	JMP    loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	MOVQ   R8, (DX)
	ADDQ   $8, AX
	ADDQ   $8, DX
	LOOP   tailLoop

done:
	RET

// func crc32Int64CellBatchHashAsm(data *uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int64CellBatchHashAsm(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), AX
	MOVQ hashes+8(FP), DX
	MOVQ length+16(FP), CX

loop:
	SUBQ   $8, CX
	JL     tail
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	MOVQ   R8, (DX)
	MOVQ   $-1, R9
	CRC32Q 16(AX), R9
	MOVQ   R9, 8(DX)
	MOVQ   $-1, R10
	CRC32Q 32(AX), R10
	MOVQ   R10, 16(DX)
	MOVQ   $-1, R11
	CRC32Q 48(AX), R11
	MOVQ   R11, 24(DX)
	MOVQ   $-1, R12
	CRC32Q 64(AX), R12
	MOVQ   R12, 32(DX)
	MOVQ   $-1, R13
	CRC32Q 80(AX), R13
	MOVQ   R13, 40(DX)
	MOVQ   $-1, R14
	CRC32Q 96(AX), R14
	MOVQ   R14, 48(DX)
	MOVQ   $-1, R15
	CRC32Q 112(AX), R15
	MOVQ   R15, 56(DX)
	ADDQ   $128, AX
	ADDQ   $64, DX
	JMP    loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	MOVQ   R8, (DX)
	ADDQ   $16, AX
	ADDQ   $8, DX
	LOOP   tailLoop

done:
	RET

// func aesBytesHashAsm(data unsafe.Pointer, length int) [2]uint64
// Requires: AES
TEXT ·aesBytesHashAsm(SB), NOSPLIT, $0-32
	MOVQ data+0(FP), AX
	MOVQ length+8(FP), CX
	ADDQ AX, CX
	SUBQ $64, CX

	VMOVDQU aesIV<>+0(SB), X0
	VMOVDQU aesIV<>+16(SB), X1
	VMOVDQU aesIV<>+32(SB), X2
	VMOVDQU aesIV<>+48(SB), X3

loop:
	CMPQ AX, CX
	JGE  tail0

	VAESENC (AX), X0, X0
	VAESENC 16(AX), X1, X1
	VAESENC 32(AX), X2, X2
	VAESENC 48(AX), X3, X3

	ADDQ $64, AX
	JMP  loop

tail0:
	ADDQ $48, CX

	CMPQ AX, CX
	JGE  tail1

	VAESENC (AX), X0, X0
	ADDQ    $16, AX

tail1:
	CMPQ AX, CX
	JGE  tail2

	VAESENC (AX), X1, X1
	ADDQ    $16, AX

tail2:
	CMPQ AX, CX
	JGE  tail3

	VAESENC (AX), X2, X2
	ADDQ    $16, AX

tail3:
	VAESENC (CX), X3, X3

	VAESENC X1, X0, X0
	VAESENC X3, X2, X1
	VAESENC X1, X0, X0

	VAESENC X0, X0, X0
	VAESENC X0, X0, X0
	VAESENC X0, X0, X0

	VMOVDQU X0, ret+16(FP)

	RET

////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////

// func crc32Int192HashAsm(data *[3]uint64) uint64
// Requires: SSE4.2
TEXT ·crc32Int192HashAsm(SB), NOSPLIT, $0-16
	MOVQ data+0(FP), AX
	MOVQ   $-1, DX
	CRC32Q (AX), DX
	CRC32Q 8(AX), DX
	CRC32Q 16(AX), DX
	MOVQ   DX, ret+8(FP)
	RET

// func crc32Int256HashAsm(data *[4]uint64) uint64
// Requires: SSE4.2
TEXT ·crc32Int256HashAsm(SB), NOSPLIT, $0-16
	MOVQ data+0(FP), AX
	MOVQ   $-1, DX
	CRC32Q (AX), DX
	CRC32Q 8(AX), DX
	CRC32Q 16(AX), DX
	CRC32Q 24(AX), DX
	MOVQ   DX, ret+8(FP)
	RET

// func crc32Int320HashAsm(data *[4]uint64) uint64
// Requires: SSE4.2
TEXT ·crc32Int320HashAsm(SB), NOSPLIT, $0-16
	MOVQ data+0(FP), AX
	MOVQ   $-1, DX
	CRC32Q (AX), DX
	CRC32Q 8(AX), DX
	CRC32Q 16(AX), DX
	CRC32Q 24(AX), DX
	CRC32Q 32(AX), DX
	MOVQ   DX, ret+8(FP)
	RET

// func crc32Int192BatchHashAsm(data *[3]uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int192BatchHashAsm(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), AX
	MOVQ hashes+8(FP), DX
	MOVQ length+16(FP), CX

loop:
	SUBQ   $8, CX
	JL     tail
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	CRC32Q 8(AX), R8
	CRC32Q 16(AX), R8
	MOVQ   R8, (DX)
	MOVQ   $-1, R9
	CRC32Q 24(AX), R9
	CRC32Q 32(AX), R9
	CRC32Q 40(AX), R9
	MOVQ   R9, 8(DX)
	MOVQ   $-1, R10
	CRC32Q 48(AX), R10
	CRC32Q 56(AX), R10
	CRC32Q 64(AX), R10
	MOVQ   R10, 16(DX)
	MOVQ   $-1, R11
	CRC32Q 72(AX), R11
	CRC32Q 80(AX), R11
	CRC32Q 88(AX), R11
	MOVQ   R11, 24(DX)
	MOVQ   $-1, R12
	CRC32Q 96(AX), R12
	CRC32Q 104(AX), R12
	CRC32Q 112(AX), R12
	MOVQ   R12, 32(DX)
	MOVQ   $-1, R13
	CRC32Q 120(AX), R13
	CRC32Q 128(AX), R13
	CRC32Q 136(AX), R13
	MOVQ   R13, 40(DX)
	MOVQ   $-1, R14
	CRC32Q 144(AX), R14
	CRC32Q 152(AX), R14
	CRC32Q 160(AX), R14
	MOVQ   R14, 48(DX)
	MOVQ   $-1, R15
	CRC32Q 168(AX), R15
	CRC32Q 176(AX), R15
	CRC32Q 184(AX), R15
	MOVQ   R15, 56(DX)
	ADDQ   $192, AX
	ADDQ   $64, DX
	JMP    loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	CRC32Q 8(AX), R8
	CRC32Q 16(AX), R8
	MOVQ   R8, (DX)
	ADDQ   $24, AX
	ADDQ   $8, DX
	LOOP   tailLoop

done:
	RET

// func crc32Int256BatchHashAsm(data *[4]uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int256BatchHashAsm(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), AX
	MOVQ hashes+8(FP), DX
	MOVQ length+16(FP), CX

loop:
	SUBQ   $8, CX
	JL     tail
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	CRC32Q 8(AX), R8
	CRC32Q 16(AX), R8
	CRC32Q 24(AX), R8
	MOVQ   R8, (DX)
	MOVQ   $-1, R9
	CRC32Q 32(AX), R9
	CRC32Q 40(AX), R9
	CRC32Q 48(AX), R9
	CRC32Q 56(AX), R9
	MOVQ   R9, 8(DX)
	MOVQ   $-1, R10
	CRC32Q 64(AX), R10
	CRC32Q 72(AX), R10
	CRC32Q 80(AX), R10
	CRC32Q 88(AX), R10
	MOVQ   R10, 16(DX)
	MOVQ   $-1, R11
	CRC32Q 96(AX), R11
	CRC32Q 104(AX), R11
	CRC32Q 112(AX), R11
	CRC32Q 120(AX), R11
	MOVQ   R11, 24(DX)
	MOVQ   $-1, R12
	CRC32Q 128(AX), R12
	CRC32Q 136(AX), R12
	CRC32Q 144(AX), R12
	CRC32Q 152(AX), R12
	MOVQ   R12, 32(DX)
	MOVQ   $-1, R13
	CRC32Q 160(AX), R13
	CRC32Q 168(AX), R13
	CRC32Q 176(AX), R13
	CRC32Q 184(AX), R13
	MOVQ   R13, 40(DX)
	MOVQ   $-1, R14
	CRC32Q 192(AX), R14
	CRC32Q 200(AX), R14
	CRC32Q 208(AX), R14
	CRC32Q 216(AX), R14
	MOVQ   R14, 48(DX)
	MOVQ   $-1, R15
	CRC32Q 224(AX), R15
	CRC32Q 232(AX), R15
	CRC32Q 240(AX), R15
	CRC32Q 248(AX), R15
	MOVQ   R15, 56(DX)
	ADDQ   $256, AX
	ADDQ   $64, DX
	JMP    loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	CRC32Q 8(AX), R8
	CRC32Q 16(AX), R8
	CRC32Q 24(AX), R8
	MOVQ   R8, (DX)
	ADDQ   $32, AX
	ADDQ   $8, DX
	LOOP   tailLoop

done:
	RET

// func crc32Int320BatchHashAsm(data *[5]uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int320BatchHashAsm(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), AX
	MOVQ hashes+8(FP), DX
	MOVQ length+16(FP), CX

loop:
	SUBQ   $8, CX
	JL     tail
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	CRC32Q 8(AX), R8
	CRC32Q 16(AX), R8
	CRC32Q 24(AX), R8
	CRC32Q 32(AX), R8
	MOVQ   R8, (DX)
	MOVQ   $-1, R9
	CRC32Q 40(AX), R9
	CRC32Q 48(AX), R9
	CRC32Q 56(AX), R9
	CRC32Q 64(AX), R9
	CRC32Q 72(AX), R9
	MOVQ   R9, 8(DX)
	MOVQ   $-1, R10
	CRC32Q 80(AX), R10
	CRC32Q 88(AX), R10
	CRC32Q 96(AX), R10
	CRC32Q 104(AX), R10
	CRC32Q 112(AX), R10
	MOVQ   R10, 16(DX)
	MOVQ   $-1, R11
	CRC32Q 120(AX), R11
	CRC32Q 128(AX), R11
	CRC32Q 136(AX), R11
	CRC32Q 144(AX), R11
	CRC32Q 152(AX), R11
	MOVQ   R11, 24(DX)
	MOVQ   $-1, R12
	CRC32Q 160(AX), R12
	CRC32Q 168(AX), R12
	CRC32Q 176(AX), R12
	CRC32Q 184(AX), R12
	CRC32Q 192(AX), R12
	MOVQ   R12, 32(DX)
	MOVQ   $-1, R13
	CRC32Q 200(AX), R13
	CRC32Q 208(AX), R13
	CRC32Q 216(AX), R13
	CRC32Q 224(AX), R13
	CRC32Q 232(AX), R13
	MOVQ   R13, 40(DX)
	MOVQ   $-1, R14
	CRC32Q 240(AX), R14
	CRC32Q 248(AX), R14
	CRC32Q 256(AX), R14
	CRC32Q 264(AX), R14
	CRC32Q 272(AX), R14
	MOVQ   R14, 48(DX)
	MOVQ   $-1, R15
	CRC32Q 280(AX), R15
	CRC32Q 288(AX), R15
	CRC32Q 296(AX), R15
	CRC32Q 304(AX), R15
	CRC32Q 312(AX), R15
	MOVQ   R15, 56(DX)
	ADDQ   $320, AX
	ADDQ   $64, DX
	JMP    loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ   $-1, R8
	CRC32Q (AX), R8
	CRC32Q 8(AX), R8
	CRC32Q 16(AX), R8
	CRC32Q 24(AX), R8
	CRC32Q 32(AX), R8
	MOVQ   R8, (DX)
	ADDQ   $40, AX
	ADDQ   $8, DX
	LOOP   tailLoop

done:
	RET
