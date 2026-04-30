#include "textflag.h"

// func Decimal64SignExtend(dst, src unsafe.Pointer, n int)
//
// Widens n Decimal64 (8-byte) values to Decimal128 (16-byte struct {B0_63, B64_127 uint64})
// via arithmetic right-shift sign extension.
// 4× unrolled with PREFETCHT0 on destination to hide L2 RFO latency.
TEXT ·Decimal64SignExtend(SB), NOSPLIT, $0-24
	MOVQ dst+0(FP), DI
	MOVQ src+8(FP), SI
	MOVQ n+16(FP), CX

	TESTQ CX, CX
	JLE   done

	// DX = n/4 (number of 4-element iterations)
	// CX = n%4 (remainder)
	MOVQ CX, DX
	SHRQ $2, DX
	ANDQ $3, CX

	TESTQ DX, DX
	JZ    remainder

loop4:
	// Prefetch destination 4 cache lines ahead (16 elements × 16 bytes = 256 bytes).
	PREFETCHT0 256(DI)
	// Prefetch source 2 cache lines ahead (16 elements × 8 bytes = 128 bytes).
	PREFETCHT0 128(SI)

	// Element 0
	MOVQ 0(SI), AX
	MOVQ AX, R8
	SARQ $63, R8
	MOVQ AX, 0(DI)
	MOVQ R8, 8(DI)

	// Element 1
	MOVQ 8(SI), AX
	MOVQ AX, R8
	SARQ $63, R8
	MOVQ AX, 16(DI)
	MOVQ R8, 24(DI)

	// Element 2
	MOVQ 16(SI), AX
	MOVQ AX, R8
	SARQ $63, R8
	MOVQ AX, 32(DI)
	MOVQ R8, 40(DI)

	// Element 3
	MOVQ 24(SI), AX
	MOVQ AX, R8
	SARQ $63, R8
	MOVQ AX, 48(DI)
	MOVQ R8, 56(DI)

	ADDQ $32, SI  // 4 × 8-byte source elements
	ADDQ $64, DI  // 4 × 16-byte destination elements
	DECQ DX
	JNZ  loop4

remainder:
	TESTQ CX, CX
	JZ    done

loop1:
	MOVQ 0(SI), AX
	MOVQ AX, R8
	SARQ $63, R8
	MOVQ AX, 0(DI)
	MOVQ R8, 8(DI)
	ADDQ $8, SI
	ADDQ $16, DI
	DECQ CX
	JNZ  loop1

done:
	RET
