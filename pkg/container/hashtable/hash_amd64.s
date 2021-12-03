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

// func crc32BytesHash(data unsafe.Pointer, length int) uint64
// Requires: SSE4.2
TEXT ·crc32BytesHash(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), SI
	MOVQ length+8(FP), CX
	MOVQ CX, BX
	MOVQ $-1, DI
	ADDQ SI, CX
	SUBQ $8, CX

loop:
	CMPQ   SI, CX
	JGE    done
	CRC32Q (SI), DI
	ADDQ   $8, SI
	JMP    loop

done:
	CRC32Q (CX), DI
	MOVL   DI, ret+16(FP)
	MOVL   BX, ret+20(FP)
	RET

// func crc32Int64Hash(data uint64) uint64
// Requires: SSE4.2
TEXT ·crc32Int64Hash(SB), NOSPLIT, $0-16
	MOVQ   $-1, SI
	CRC32Q data+0(FP), SI
	MOVQ   SI, ret+8(FP)
	RET

// func crc32Int64BatchHash(data *uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int64BatchHash(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), SI
	MOVQ hashes+8(FP), DI
	MOVQ length+16(FP), CX

loop:
	SUBQ $8, CX
	JL   tail

	MOVQ $-1, R8
	MOVQ $-1, R9
	MOVQ $-1, R10
	MOVQ $-1, R11
	MOVQ $-1, R12
	MOVQ $-1, R13
	MOVQ $-1, R14
	MOVQ $-1, R15

	CRC32Q (SI), R8
	CRC32Q 8(SI), R9
	CRC32Q 16(SI), R10
	CRC32Q 24(SI), R11
	CRC32Q 32(SI), R12
	CRC32Q 40(SI), R13
	CRC32Q 48(SI), R14
	CRC32Q 56(SI), R15

	MOVQ R8, (DI)
	MOVQ R9, 8(DI)
	MOVQ R10, 16(DI)
	MOVQ R11, 24(DI)
	MOVQ R12, 32(DI)
	MOVQ R13, 40(DI)
	MOVQ R14, 48(DI)
	MOVQ R15, 56(DI)

	ADDQ $64, SI
	ADDQ $64, DI
	JMP  loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ   $-1, R8
	CRC32Q (SI), R8
	MOVQ   R8, (DI)

	ADDQ $8, SI
	ADDQ $8, DI
	LOOP tailLoop

done:
	RET

// func crc32Int64CellBatchHash(data *uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int64CellBatchHash(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), SI
	MOVQ hashes+8(FP), DI
	MOVQ length+16(FP), CX

loop:
	SUBQ $8, CX
	JL   tail

	MOVQ $-1, R8
	MOVQ $-1, R9
	MOVQ $-1, R10
	MOVQ $-1, R11
	MOVQ $-1, R12
	MOVQ $-1, R13
	MOVQ $-1, R14
	MOVQ $-1, R15

	CRC32Q (SI), R8
	CRC32Q 16(SI), R9
	CRC32Q 32(SI), R10
	CRC32Q 48(SI), R11
	CRC32Q 64(SI), R12
	CRC32Q 80(SI), R13
	CRC32Q 96(SI), R14
	CRC32Q 112(SI), R15

	MOVQ R8, (DI)
	MOVQ R9, 8(DI)
	MOVQ R10, 16(DI)
	MOVQ R11, 24(DI)
	MOVQ R12, 32(DI)
	MOVQ R13, 40(DI)
	MOVQ R14, 48(DI)
	MOVQ R15, 56(DI)

	ADDQ $128, SI
	ADDQ $64, DI
	JMP  loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ   $-1, R8
	CRC32Q (SI), R8
	MOVQ   R8, (DI)

	ADDQ $16, SI
	ADDQ $8, DI
	LOOP tailLoop

done:
	RET

DATA aesIV<>+0x00(SB)/8, $0x5A8279996ED9EBA1
DATA aesIV<>+0x08(SB)/8, $0x8F1BBCDCCA62C1D6
DATA aesIV<>+0x10(SB)/8, $0x5A8279996ED9EBA1
DATA aesIV<>+0x18(SB)/8, $0x8F1BBCDCCA62C1D6
DATA aesIV<>+0x20(SB)/8, $0x5A8279996ED9EBA1
DATA aesIV<>+0x28(SB)/8, $0x8F1BBCDCCA62C1D6
DATA aesIV<>+0x30(SB)/8, $0x5A8279996ED9EBA1
DATA aesIV<>+0x38(SB)/8, $0x8F1BBCDCCA62C1D6
GLOBL aesIV<>(SB), (NOPTR+RODATA), $64

// func aesBytesHash(data unsafe.Pointer, length int) [2]uint64
// Requires: AES
TEXT ·aesBytesHash(SB), NOSPLIT, $0-32
	MOVQ data+0(FP), SI
	MOVQ length+8(FP), CX
	ADDQ SI, CX
	SUBQ $64, CX

	VMOVDQU aesIV<>+0(SB), X0
	VMOVDQU aesIV<>+16(SB), X1
	VMOVDQU aesIV<>+32(SB), X2
	VMOVDQU aesIV<>+48(SB), X3

loop:
	CMPQ SI, CX
	JGE  tail0

	VAESENC (SI), X0, X0
	VAESENC 16(SI), X1, X1
	VAESENC 32(SI), X2, X2
	VAESENC 48(SI), X3, X3

	ADDQ $64, SI
	JMP  loop

tail0:
	ADDQ $48, CX

	CMPQ SI, CX
	JGE  tail1

	VAESENC (SI), X0, X0
	ADDQ    $16, SI

tail1:
	CMPQ SI, CX
	JGE  tail2

	VAESENC (SI), X1, X1
	ADDQ    $16, SI

tail2:
	CMPQ SI, CX
	JGE  tail3

	VAESENC (SI), X2, X2
	ADDQ    $16, SI

tail3:
	VAESENC (CX), X3, X3

	VAESENC X1, X0, X0
	VAESENC X3, X2, X2
	VAESENC X2, X0, X0

	VAESENC X0, X0, X0
	VAESENC X0, X0, X0
	VAESENC X0, X0, X0

	VMOVDQU X0, ret+16(FP)

	RET

////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////

// func crc32Int192Hash(data *[3]uint64) uint64
// Requires: SSE4.2
TEXT ·crc32Int192Hash(SB), NOSPLIT, $0-16
	MOVQ   data+0(FP), SI
	MOVQ   $-1, DI
	CRC32Q (SI), DI
	CRC32Q 8(SI), DI
	CRC32Q 16(SI), DI
	MOVQ   DI, ret+8(FP)
	RET

// func crc32Int256Hash(data *[4]uint64) uint64
// Requires: SSE4.2
TEXT ·crc32Int256Hash(SB), NOSPLIT, $0-16
	MOVQ   ata+0(FP), SI
	MOVQ   $-1, DI
	CRC32Q (SI), DI
	CRC32Q 8(SI), DI
	CRC32Q 16(SI), DI
	CRC32Q 24(SI), DI
	MOVQ   DI, ret+8(FP)
	RET

// func crc32Int320Hash(data *[4]uint64) uint64
// Requires: SSE4.2
TEXT ·crc32Int320Hash(SB), NOSPLIT, $0-16
	MOVQ   data+0(FP), SI
	MOVQ   $-1, DI
	CRC32Q (SI), DI
	CRC32Q 8(SI), DI
	CRC32Q 16(SI), DI
	CRC32Q 24(SI), DI
	CRC32Q 32(SI), DI
	MOVQ   DI, ret+8(FP)
	RET

// func crc32Int192BatchHash(data *[3]uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int192BatchHash(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), SI
	MOVQ hashes+8(FP), DI
	MOVQ length+16(FP), CX

loop:
	SUBQ $8, CX
	JL   tail

	MOVQ $-1, R8
	MOVQ $-1, R9
	MOVQ $-1, R10
	MOVQ $-1, R11
	MOVQ $-1, R12
	MOVQ $-1, R13
	MOVQ $-1, R14
	MOVQ $-1, R15

	CRC32Q (SI), R8
	CRC32Q 8(SI), R8
	CRC32Q 16(SI), R8
	CRC32Q 24(SI), R9
	CRC32Q 32(SI), R9
	CRC32Q 40(SI), R9
	CRC32Q 48(SI), R10
	CRC32Q 56(SI), R10
	CRC32Q 64(SI), R10
	CRC32Q 72(SI), R11
	CRC32Q 80(SI), R11
	CRC32Q 88(SI), R11
	CRC32Q 96(SI), R12
	CRC32Q 104(SI), R12
	CRC32Q 112(SI), R12
	CRC32Q 120(SI), R13
	CRC32Q 128(SI), R13
	CRC32Q 136(SI), R13
	CRC32Q 144(SI), R14
	CRC32Q 152(SI), R14
	CRC32Q 160(SI), R14
	CRC32Q 168(SI), R15
	CRC32Q 176(SI), R15
	CRC32Q 184(SI), R15

	MOVQ R8, (DI)
	MOVQ R9, 8(DI)
	MOVQ R10, 16(DI)
	MOVQ R11, 24(DI)
	MOVQ R12, 32(DI)
	MOVQ R13, 40(DI)
	MOVQ R14, 48(DI)
	MOVQ R15, 56(DI)

	ADDQ $192, SI
	ADDQ $64, DI
	JMP  loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ $-1, R8

	CRC32Q (SI), R8
	CRC32Q 8(SI), R8
	CRC32Q 16(SI), R8

	MOVQ R8, (DI)

	ADDQ $24, SI
	ADDQ $8, DI
	LOOP tailLoop

done:
	RET

// func crc32Int256BatchHash(data *[4]uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int256BatchHash(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), SI
	MOVQ hashes+8(FP), DI
	MOVQ length+16(FP), CX

loop:
	SUBQ $8, CX
	JL   tail

	MOVQ $-1, R8
	MOVQ $-1, R9
	MOVQ $-1, R10
	MOVQ $-1, R11
	MOVQ $-1, R12
	MOVQ $-1, R13
	MOVQ $-1, R14
	MOVQ $-1, R15

	CRC32Q (SI), R8
	CRC32Q 8(SI), R8
	CRC32Q 16(SI), R8
	CRC32Q 24(SI), R8
	CRC32Q 32(SI), R9
	CRC32Q 40(SI), R9
	CRC32Q 48(SI), R9
	CRC32Q 56(SI), R9
	CRC32Q 64(SI), R10
	CRC32Q 72(SI), R10
	CRC32Q 80(SI), R10
	CRC32Q 88(SI), R10
	CRC32Q 96(SI), R11
	CRC32Q 104(SI), R11
	CRC32Q 112(SI), R11
	CRC32Q 120(SI), R11
	CRC32Q 128(SI), R12
	CRC32Q 136(SI), R12
	CRC32Q 144(SI), R12
	CRC32Q 152(SI), R12
	CRC32Q 160(SI), R13
	CRC32Q 168(SI), R13
	CRC32Q 176(SI), R13
	CRC32Q 184(SI), R13
	CRC32Q 192(SI), R14
	CRC32Q 200(SI), R14
	CRC32Q 208(SI), R14
	CRC32Q 216(SI), R14
	CRC32Q 224(SI), R15
	CRC32Q 232(SI), R15
	CRC32Q 240(SI), R15
	CRC32Q 248(SI), R15

	MOVQ R8, (DI)
	MOVQ R9, 8(DI)
	MOVQ R10, 16(DI)
	MOVQ R11, 24(DI)
	MOVQ R12, 32(DI)
	MOVQ R13, 40(DI)
	MOVQ R14, 48(DI)
	MOVQ R15, 56(DI)

	ADDQ $256, SI
	ADDQ $64, DI
	JMP  loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ $-1, R8

	CRC32Q (SI), R8
	CRC32Q 8(SI), R8
	CRC32Q 16(SI), R8
	CRC32Q 24(SI), R8

	MOVQ R8, (DI)

	ADDQ $32, SI
	ADDQ $8, DI
	LOOP tailLoop

done:
	RET

// func crc32Int320BatchHash(data *[5]uint64, hashes *uint64, length int)
// Requires: SSE4.2
TEXT ·crc32Int320BatchHash(SB), NOSPLIT, $0-24
	MOVQ data+0(FP), SI
	MOVQ hashes+8(FP), DI
	MOVQ length+16(FP), CX

loop:
	SUBQ $8, CX
	JL   tail

	MOVQ $-1, R8
	MOVQ $-1, R9
	MOVQ $-1, R10
	MOVQ $-1, R11
	MOVQ $-1, R12
	MOVQ $-1, R13
	MOVQ $-1, R14
	MOVQ $-1, R15

	CRC32Q (SI), R8
	CRC32Q 8(SI), R8
	CRC32Q 16(SI), R8
	CRC32Q 24(SI), R8
	CRC32Q 32(SI), R8
	CRC32Q 40(SI), R9
	CRC32Q 48(SI), R9
	CRC32Q 56(SI), R9
	CRC32Q 64(SI), R9
	CRC32Q 72(SI), R9
	CRC32Q 80(SI), R10
	CRC32Q 88(SI), R10
	CRC32Q 96(SI), R10
	CRC32Q 104(SI), R10
	CRC32Q 112(SI), R10
	CRC32Q 120(SI), R11
	CRC32Q 128(SI), R11
	CRC32Q 136(SI), R11
	CRC32Q 144(SI), R11
	CRC32Q 152(SI), R11
	CRC32Q 160(SI), R12
	CRC32Q 168(SI), R12
	CRC32Q 176(SI), R12
	CRC32Q 184(SI), R12
	CRC32Q 192(SI), R12
	CRC32Q 200(SI), R13
	CRC32Q 208(SI), R13
	CRC32Q 216(SI), R13
	CRC32Q 224(SI), R13
	CRC32Q 232(SI), R13
	CRC32Q 240(SI), R14
	CRC32Q 248(SI), R14
	CRC32Q 256(SI), R14
	CRC32Q 264(SI), R14
	CRC32Q 272(SI), R14
	CRC32Q 280(SI), R15
	CRC32Q 288(SI), R15
	CRC32Q 296(SI), R15
	CRC32Q 304(SI), R15
	CRC32Q 312(SI), R15

	MOVQ R8, (DI)
	MOVQ R9, 8(DI)
	MOVQ R10, 16(DI)
	MOVQ R11, 24(DI)
	MOVQ R12, 32(DI)
	MOVQ R13, 40(DI)
	MOVQ R14, 48(DI)
	MOVQ R15, 56(DI)

	ADDQ $320, SI
	ADDQ $64, DI
	JMP  loop

tail:
	ADDQ $8, CX
	JE   done

tailLoop:
	MOVQ $-1, R8

	CRC32Q (SI), R8
	CRC32Q 8(SI), R8
	CRC32Q 16(SI), R8
	CRC32Q 24(SI), R8
	CRC32Q 32(SI), R8

	MOVQ R8, (DI)

	ADDQ $40, SI
	ADDQ $8, DI
	LOOP tailLoop

done:
	RET
