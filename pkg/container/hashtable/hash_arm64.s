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

// func Crc32BytesHash(data unsafe.Pointer, length int) uint64
// Requires: CRC32
TEXT ·Crc32BytesHash(SB), NOSPLIT, $0-24
	MOVD data+0(FP), R0
	MOVD length+8(FP), R1
	MOVD R1, R4
	MOVD $-1, R2
	ADD  R0, R1
	SUB  $8, R1

loop:
	CMP     R0, R1
	BLE     done
	MOVD.P  8(R0), R3
	CRC32CX R3, R2
	JMP     loop

done:
	MOVD    (R1), R3
	CRC32CX R3, R2
	MOVW    R2, ret+16(FP)
	MOVW    R4, ret+20(FP)
	RET

// func Crc32Int64Hash(data uint64) uint64
// Requires: CRC32
TEXT ·Crc32Int64Hash(SB), NOSPLIT, $0-16
	MOVD    data+0(FP), R0
	MOVD    $-1, R1
	CRC32CX R0, R1
	MOVD    R1, ret+8(FP)
	RET

// func Crc32Int64BatchHash(data *uint64, hashes *uint64, length int)
// Requires: CRC32
TEXT ·Crc32Int64BatchHash(SB), NOSPLIT, $0-24
	MOVD data+0(FP), R0
	MOVD hashes+8(FP), R1
	MOVD length+16(FP), R2

loop:
	SUBS $8, R2
	BLT  tail

	MOVD $-1, R3
	MOVD $-1, R4
	MOVD $-1, R5
	MOVD $-1, R6
	MOVD $-1, R7
	MOVD $-1, R8
	MOVD $-1, R9
	MOVD $-1, R10

	LDP.P 16(R0), (R11, R12)
	LDP.P 16(R0), (R13, R14)
	LDP.P 16(R0), (R15, R16)
	LDP.P 16(R0), (R17, R19)

	CRC32CX R11, R3
	CRC32CX R12, R4
	CRC32CX R13, R5
	CRC32CX R14, R6
	CRC32CX R15, R7
	CRC32CX R16, R8
	CRC32CX R17, R9
	CRC32CX R19, R10

	STP.P (R3, R4), 16(R1)
	STP.P (R5, R6), 16(R1)
	STP.P (R7, R8), 16(R1)
	STP.P (R9, R10), 16(R1)

	JMP loop

tail:
	ADDS $8, R2
	BEQ  done

tailLoop:
	MOVD    $-1, R3
	MOVD.P  8(R0), R4
	CRC32CX R4, R3
	MOVD.P  R3, 8(R1)

	SUBS $1, R2
	BNE  tailLoop

done:
	RET

// func Crc32Int64CellBatchHash(data *uint64, hashes *uint64, length int)
// Requires: CRC32
TEXT ·Crc32Int64CellBatchHash(SB), NOSPLIT, $0-24
	MOVD data+0(FP), R0
	MOVD hashes+8(FP), R1
	MOVD length+16(FP), R2

loop:
	SUBS $8, R2
	BLT  tail

	MOVD $-1, R3
	MOVD $-1, R4
	MOVD $-1, R5
	MOVD $-1, R6
	MOVD $-1, R7
	MOVD $-1, R8
	MOVD $-1, R9
	MOVD $-1, R10

	MOVD.P 16(R0), R11
	MOVD.P 16(R0), R12
	MOVD.P 16(R0), R13
	MOVD.P 16(R0), R14
	MOVD.P 16(R0), R15
	MOVD.P 16(R0), R16
	MOVD.P 16(R0), R17
	MOVD.P 16(R0), R19

	CRC32CX R11, R3
	CRC32CX R12, R4
	CRC32CX R13, R5
	CRC32CX R14, R6
	CRC32CX R15, R7
	CRC32CX R16, R8
	CRC32CX R17, R9
	CRC32CX R19, R10

	STP.P (R3, R4), 16(R1)
	STP.P (R5, R6), 16(R1)
	STP.P (R7, R8), 16(R1)
	STP.P (R9, R10), 16(R1)

	JMP loop

tail:
	ADDS $8, R2
	BEQ  done

tailLoop:
	MOVD    $-1, R4
	MOVD.P  16(R0), R3
	CRC32CX R3, R4
	MOVD.P  R4, 8(R1)

	SUBS $1, R2
	BNE  tailLoop

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

// func AesBytesHash(data unsafe.Pointer, length int) [2]uint64
// Requires: AES
TEXT ·AesBytesHash(SB), NOSPLIT, $0-32
	MOVD data+0(FP), R0
	MOVD length+8(FP), R1
	MOVD $ret+16(FP), R2
	ADD  R0, R1
	SUB  $64, R1

	MOVD $aesIV<>+0(SB), R3
	VLD1 (R3), [V0.B16, V1.B16, V2.B16, V3.B16]
	VEOR V10.B16, V10.B16, V10.B16

loop:
	CMP R0, R1
	BLE tail0

	VLD1.P 64(R0), [V4.B16, V5.B16, V6.B16, V7.B16]

	AESE	V10.B16, V0.B16
	AESMC	V0.B16, V0.B16
	VEOR	V4.B16, V0.B16, V0.B16

	AESE	V10.B16, V1.B16
	AESMC	V1.B16, V1.B16
	VEOR	V5.B16, V1.B16, V1.B16

	AESE	V10.B16, V2.B16
	AESMC	V2.B16, V2.B16
	VEOR	V6.B16, V2.B16, V2.B16

	AESE	V10.B16, V3.B16
	AESMC	V3.B16, V3.B16
	VEOR	V7.B16, V3.B16, V3.B16

	JMP loop

tail0:
	ADD $48, R1

	CMP R0, R1
	BLE tail1

	VLD1.P 16(R0), [V4.B16]
	AESE   V10.B16, V0.B16
	AESMC  V0.B16, V0.B16
	VEOR   V4.B16, V0.B16, V0.B16

tail1:
	CMP R0, R1
	BLE tail2

	VLD1.P 16(R0), [V5.B16]
	AESE   V10.B16, V1.B16
	AESMC  V1.B16, V1.B16
	VEOR   V5.B16, V1.B16, V1.B16

tail2:
	CMP R0, R1
	BLE tail3

	VLD1.P (R0), [V6.B16]
	AESE   V10.B16, V2.B16
	AESMC  V2.B16, V2.B16
	VEOR   V6.B16, V2.B16, V2.B16

tail3:
	VLD1  (R1), [V7.B16]
	AESE  V10.B16, V3.B16
	AESMC V3.B16, V3.B16
	VEOR  V7.B16, V3.B16, V3.B16

	AESE  V10.B16, V0.B16
	AESMC V0.B16, V0.B16
	VEOR  V1.B16, V0.B16, V0.B16

	AESE  V10.B16, V2.B16
	AESMC V2.B16, V2.B16
	VEOR  V3.B16, V2.B16, V2.B16

	AESE  V10.B16, V0.B16
	AESMC V0.B16, V0.B16
	VEOR  V2.B16, V0.B16, V0.B16

	VST1 [V0.B16], (R2)

	RET

////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////

// func Crc32Int192Hash(data *[3]uint64)
// Requires: CRC32
TEXT ·Crc32Int192Hash(SB), NOSPLIT, $0-16
	MOVD    $data+0(FP), R0
	MOVD    $-1, R1
	LDP.P   16(R0), (R2, R3)
	MOVD    (R0), R4
	CRC32CX R2, R1
	CRC32CX R3, R1
	CRC32CX R4, R1
	MOVD    R1, ret+8(FP)
	RET

// func Crc32Int256Hash(data *[4]uint64)
// Requires: CRC32
TEXT ·Crc32Int256Hash(SB), NOSPLIT, $0-16
	MOVD    $data+0(FP), R0
	MOVD    $-1, R1
	LDP.P   16(R0), (R2, R3)
	LDP.P   (R0), (R4, R5)
	CRC32CX R2, R1
	CRC32CX R3, R1
	CRC32CX R4, R1
	CRC32CX R5, R1
	MOVD    R1, ret+8(FP)
	RET

// func Crc32Int320Hash(data *[4]uint64)
// Requires: CRC32
TEXT ·Crc32Int320Hash(SB), NOSPLIT, $0-16
	MOVD    $data+0(FP), R0
	MOVD    $-1, R1
	LDP.P   16(R0), (R2, R3)
	LDP.P   16(R0), (R4, R5)
	MOVD    (R0), R6
	CRC32CX R2, R1
	CRC32CX R3, R1
	CRC32CX R4, R1
	CRC32CX R5, R1
	CRC32CX R6, R1
	MOVD    R1, ret+8(FP)
	RET

// func Crc32Int192BatchHash(data *[3]uint64, hashes *uint64, length int)
// Requires: CRC32
TEXT ·Crc32Int192BatchHash(SB), NOSPLIT, $0-24
	MOVD data+0(FP), R0
	MOVD hashes+8(FP), R1
	MOVD length+16(FP), R2
	CBZ  R2, done

loop:
	SUBS $6, R2
	BLT  tail

	MOVD $-1, R3
	MOVD $-1, R4
	MOVD $-1, R5
	MOVD $-1, R6
	MOVD $-1, R7
	MOVD $-1, R8

	LDP.P 16(R0), (R9, R10)
	LDP.P 16(R0), (R11, R12)
	LDP.P 16(R0), (R13, R14)
	LDP.P 16(R0), (R15, R16)
	LDP.P 16(R0), (R17, R19)
	LDP.P 16(R0), (R20, R21)
	LDP.P 16(R0), (R22, R23)
	LDP.P 16(R0), (R24, R25)
	LDP.P 16(R0), (R26, R27)

	CRC32CX R9, R3
	CRC32CX R12, R4
	CRC32CX R15, R5
	CRC32CX R19, R6
	CRC32CX R22, R7
	CRC32CX R25, R8

	CRC32CX R10, R3
	CRC32CX R13, R4
	CRC32CX R16, R5
	CRC32CX R20, R6
	CRC32CX R23, R7
	CRC32CX R26, R8

	CRC32CX R11, R3
	CRC32CX R14, R4
	CRC32CX R17, R5
	CRC32CX R21, R6
	CRC32CX R24, R7
	CRC32CX R27, R8

	STP.P (R3, R4), 16(R1)
	STP.P (R5, R6), 16(R1)
	STP.P (R7, R8), 16(R1)

	JMP loop

tail:
	ADDS $6, R2
	BEQ  done

tailLoop:
	MOVD $-1, R3

	LDP.P  16(R0), (R4, R5)
	MOVD.P 8(R0), R6

	CRC32CX R4, R3
	CRC32CX R5, R3
	CRC32CX R6, R3

	MOVD.P  R3, 8(R1)

	SUBS $1, R2
	BNE  tailLoop

done:
	RET

// func Crc32Int256BatchHash(data *[4]uint64, hashes *uint64, length int)
// Requires: CRC32
TEXT ·Crc32Int256BatchHash(SB), NOSPLIT, $0-24
	MOVD data+0(FP), R0
	MOVD hashes+8(FP), R1
	MOVD length+16(FP), R2
	CBZ  R2, done

loop:
	SUBS $4, R2
	BLT  tail

	MOVD $-1, R3
	MOVD $-1, R4
	MOVD $-1, R5
	MOVD $-1, R6

	LDP.P 16(R0), (R7, R8)
	LDP.P 16(R0), (R9, R10)
	LDP.P 16(R0), (R11, R12)
	LDP.P 16(R0), (R13, R14)
	LDP.P 16(R0), (R15, R16)
	LDP.P 16(R0), (R17, R19)
	LDP.P 16(R0), (R20, R21)
	LDP.P 16(R0), (R22, R23)

	CRC32CX R7, R3
	CRC32CX R11, R4
	CRC32CX R15, R5
	CRC32CX R20, R6

	CRC32CX R8, R3
	CRC32CX R12, R4
	CRC32CX R16, R5
	CRC32CX R21, R6

	CRC32CX R9, R3
	CRC32CX R13, R4
	CRC32CX R17, R5
	CRC32CX R22, R6

	CRC32CX R10, R3
	CRC32CX R14, R4
	CRC32CX R19, R5
	CRC32CX R23, R6

	STP.P (R3, R4), 16(R1)
	STP.P (R5, R6), 16(R1)

	JMP loop

tail:
	ADDS $4, R2
	BEQ  done

tailLoop:
	MOVD $-1, R3

	LDP.P 16(R0), (R4, R5)
	LDP.P 16(R0), (R6, R7)

	CRC32CX R4, R3
	CRC32CX R5, R3
	CRC32CX R6, R3
	CRC32CX R7, R3

	MOVD.P  R3, 8(R1)

	SUBS $1, R2
	BNE  tailLoop

done:
	RET

// func Crc32Int320BatchHash(data *[5]uint64, hashes *uint64, length int)
// Requires: CRC32
TEXT ·Crc32Int320BatchHash(SB), NOSPLIT, $0-24
	MOVD data+0(FP), R0
	MOVD hashes+8(FP), R1
	MOVD length+16(FP), R2
	CBZ  R2, done

loop:
	SUBS $4, R2
	BLT  tail

	MOVD $-1, R3
	MOVD $-1, R4
	MOVD $-1, R5
	MOVD $-1, R6

	LDP.P 16(R0), (R7, R8)
	LDP.P 16(R0), (R9, R10)
	LDP.P 16(R0), (R11, R12)
	LDP.P 16(R0), (R13, R14)
	LDP.P 16(R0), (R15, R16)
	LDP.P 16(R0), (R17, R19)
	LDP.P 16(R0), (R20, R21)
	LDP.P 16(R0), (R22, R23)
	LDP.P 16(R0), (R24, R25)
	LDP.P 16(R0), (R26, R27)

	CRC32CX R7, R3
	CRC32CX R12, R4
	CRC32CX R17, R5
	CRC32CX R23, R6

	CRC32CX R8, R3
	CRC32CX R13, R4
	CRC32CX R19, R5
	CRC32CX R24, R6

	CRC32CX R9, R3
	CRC32CX R14, R4
	CRC32CX R20, R5
	CRC32CX R25, R6

	CRC32CX R10, R3
	CRC32CX R15, R4
	CRC32CX R21, R5
	CRC32CX R26, R6

	CRC32CX R11, R3
	CRC32CX R16, R4
	CRC32CX R22, R5
	CRC32CX R27, R6

	STP.P (R3, R4), 16(R1)
	STP.P (R5, R6), 16(R1)

	JMP loop

tail:
	ADDS $4, R2
	BEQ  done

tailLoop:
	MOVD $-1, R3

	LDP.P  16(R0), (R4, R5)
	LDP.P  16(R0), (R6, R7)
	MOVD.P 8(R0), R8

	CRC32CX R4, R3
	CRC32CX R5, R3
	CRC32CX R6, R3
	CRC32CX R7, R3
	CRC32CX R8, R3

	MOVD.P R3, 8(R1)

	SUBS $1, R2
	BNE  tailLoop

done:
	RET
