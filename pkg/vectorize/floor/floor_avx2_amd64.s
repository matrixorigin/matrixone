#include "textflag.h" 

// func float32FloorAvx2Asm(xs, rs []float32, scale float32) Requires: AVX
TEXT ·float32FloorAvx2Asm(SB), NOSPLIT, $0-56
	MOVQ         xs_base+0(FP), AX
	MOVQ         rs_base+24(FP), CX
	MOVSS        scale+48(FP), X0
	MOVQ         xs_len+8(FP), DX
	VBROADCASTSS X0, Y0

float32FloorBlockLoop:
	CMPQ        DX,         $0x00000078
	JL          float32FloorTailLoop
	VMULPS     (AX), Y0, Y1
	VMULPS  32(AX), Y0, Y2
	VMULPS  64(AX), Y0, Y3
	VMULPS  96(AX), Y0, Y4
	VMULPS  128(AX), Y0, Y5
	VMULPS  160(AX), Y0, Y6
	VMULPS  192(AX), Y0, Y7
	VMULPS  224(AX), Y0, Y8
	VMULPS  256(AX), Y0, Y9
	VMULPS  288(AX), Y0, Y10
	VMULPS  320(AX), Y0, Y11
	VMULPS  352(AX), Y0, Y12
	VMULPS  384(AX), Y0, Y13
	VMULPS  416(AX), Y0, Y14
	VMULPS  448(AX), Y0, Y15
	VROUNDPS    $0x09, Y1, Y1    // 0x09 is the VROUNDPS parameter for _MM_FROUND_TO_NEG_INF | _MM_FROUND_NO_EXC
	VROUNDPS    $0x09, Y2, Y2
	VROUNDPS    $0x09, Y3, Y3
	VROUNDPS    $0x09, Y4, Y4
	VROUNDPS    $0x09, Y5, Y5
	VROUNDPS    $0x09, Y6, Y6
	VROUNDPS    $0x09, Y7, Y7
	VROUNDPS    $0x09, Y8, Y8
	VROUNDPS    $0x09, Y9, Y9
	VROUNDPS    $0x09, Y10, Y10
	VROUNDPS    $0x09, Y11, Y11
	VROUNDPS    $0x09, Y12, Y12
	VROUNDPS    $0x09, Y13, Y13
	VROUNDPS    $0x09, Y14, Y14
	VROUNDPS    $0x09, Y15, Y15
	VDIVPS      Y0, Y1, Y1
	VDIVPS      Y0, Y2, Y2
	VDIVPS      Y0, Y3, Y3
	VDIVPS      Y0, Y4, Y4
	VDIVPS      Y0, Y5, Y5
	VDIVPS      Y0, Y6, Y6
	VDIVPS      Y0, Y7, Y7
	VDIVPS      Y0, Y8, Y8
	VDIVPS      Y0, Y9, Y9
	VDIVPS      Y0, Y10, Y10
	VDIVPS      Y0, Y11, Y11
	VDIVPS      Y0, Y12, Y12
	VDIVPS      Y0, Y13, Y13
	VDIVPS      Y0, Y14, Y14
	VDIVPS      Y0, Y15, Y15
	VMOVUPS     Y1, (CX)
	VMOVUPS     Y2, 32(CX)
	VMOVUPS     Y3, 64(CX)
	VMOVUPS     Y4, 96(CX)
	VMOVUPS     Y5, 128(CX)
	VMOVUPS     Y6, 160(CX)
	VMOVUPS     Y7, 192(CX)
	VMOVUPS     Y8, 224(CX)
	VMOVUPS     Y9, 256(CX)
	VMOVUPS     Y10, 288(CX)
	VMOVUPS     Y11, 320(CX)
	VMOVUPS     Y12, 352(CX)
	VMOVUPS     Y13, 384(CX)
	VMOVUPS     Y14, 416(CX)
	VMOVUPS     Y15, 448(CX)
	ADDQ    $0x000001e0, AX
	ADDQ    $0x000001e0, CX
	SUBQ    $0x00000078, DX
	JMP     float32FloorBlockLoop

float32FloorTailLoop:
	CMPQ    DX, $0x00000008
	JL      float32FloorDone
	VMULPS  (AX), Y0, Y1
	VROUNDPS    $0x09, Y1, Y1
	VDIVPS      Y0, Y1, Y1
	VMOVUPS Y1, (CX)
	ADDQ    $0x00000020, AX
	ADDQ    $0x00000020, CX
	SUBQ    $0x00000008, DX
	JMP     float32FloorTailLoop

float32FloorDone:
	CMPQ    DX, $0x00000004
	JL      float32FloorDone1
	VMULPS  (AX), X0, X1
	VROUNDPS    $0x09, X1, X1
	VDIVPS      X0, X1, X1
	VMOVUPS X1, (CX)
	ADDQ    $0x00000010, AX
	ADDQ    $0x00000010, CX
	SUBQ    $0x00000004, DX
	JMP     float32FloorDone

float32FloorDone1:
	CMPQ    DX, $0x00000000
	JL      float32FloorDone2
	VMULSS  (AX), X0, X1
	VROUNDPS    $0x09, X1, X1
	VDIVSS      X0, X1, X1
	VMOVSS X1, (CX)
	ADDQ    $0x00000004, AX
	ADDQ    $0x00000004, CX
	SUBQ    $0x00000001, DX
	JMP     float32FloorDone1

float32FloorDone2:
	RET


// func float32FloorAvx2AsmZero(xs, rs []float32, scale float32) Requires: AVX
TEXT ·float32FloorAvx2AsmZero(SB), NOSPLIT, $0-56
	MOVQ         xs_base+0(FP), AX
	MOVQ         rs_base+24(FP), CX
	MOVSS        scale+48(FP), X0
	MOVQ         xs_len+8(FP), DX
	VBROADCASTSS X0, Y0

float32FloorZeroBlockLoop:
	CMPQ        DX,         $0x00000078
	JL          float32FloorZeroTailLoop
	VROUNDPS    $0x09, (AX), Y1
	VROUNDPS    $0x09, 32(AX), Y2
	VROUNDPS    $0x09, 64(AX), Y3
	VROUNDPS    $0x09, 96(AX), Y4
	VROUNDPS    $0x09, 128(AX), Y5
	VROUNDPS    $0x09, 160(AX), Y6
	VROUNDPS    $0x09, 192(AX), Y7
	VROUNDPS    $0x09, 224(AX), Y8
	VROUNDPS    $0x09, 256(AX), Y9
	VROUNDPS    $0x09, 288(AX), Y10
	VROUNDPS    $0x09, 320(AX), Y11
	VROUNDPS    $0x09, 352(AX), Y12
	VROUNDPS    $0x09, 384(AX), Y13
	VROUNDPS    $0x09, 416(AX), Y14
	VROUNDPS    $0x09, 448(AX), Y15
	VMOVUPS     Y1, (CX)
	VMOVUPS     Y2, 32(CX)
	VMOVUPS     Y3, 64(CX)
	VMOVUPS     Y4, 96(CX)
	VMOVUPS     Y5, 128(CX)
	VMOVUPS     Y6, 160(CX)
	VMOVUPS     Y7, 192(CX)
	VMOVUPS     Y8, 224(CX)
	VMOVUPS     Y9, 256(CX)
	VMOVUPS     Y10, 288(CX)
	VMOVUPS     Y11, 320(CX)
	VMOVUPS     Y12, 352(CX)
	VMOVUPS     Y13, 384(CX)
	VMOVUPS     Y14, 416(CX)
	VMOVUPS     Y15, 448(CX)
	ADDQ    $0x000001e0, AX
	ADDQ    $0x000001e0, CX
	SUBQ    $0x00000078, DX
	JMP     float32FloorZeroBlockLoop

float32FloorZeroTailLoop:
	CMPQ    DX, $0x00000008
	JL      float32FloorZeroDone
	VROUNDPS    $0x09, (AX), Y1
	VMOVUPS Y1, (CX)
	ADDQ    $0x00000020, AX
	ADDQ    $0x00000020, CX
	SUBQ    $0x00000008, DX
	JMP     float32FloorZeroTailLoop

float32FloorZeroDone:
	CMPQ    DX, $0x00000004
	JL      float32FloorZeroDone1
	VROUNDPS    $0x09, (AX), X1
	VMOVUPS X1, (CX)
	ADDQ    $0x00000010, AX
	ADDQ    $0x00000010, CX
	SUBQ    $0x00000004, DX
	JMP     float32FloorZeroDone

float32FloorZeroDone1:
	CMPQ    DX, $0x00000000
	JL      float32FloorZeroDone2
	VROUNDSS    $0x09, (AX), X1, X1
	VMOVSS X1, (CX)
	ADDQ    $0x00000004, AX
	ADDQ    $0x00000004, CX
	SUBQ    $0x00000001, DX
	JMP     float32FloorZeroDone1

float32FloorZeroDone2:
	RET


// func float64FloorAvx2Asm(xs, rs []float64, scale float64) Requires: AVX
TEXT ·float64FloorAvx2Asm(SB), NOSPLIT, $0-56
	MOVQ         xs_base+0(FP), AX
	MOVQ         rs_base+24(FP), CX
	MOVSD        scale+48(FP), X0
	MOVQ         xs_len+8(FP), DX
	VBROADCASTSD X0, Y0

float64FloorBlockLoop:
	CMPQ        DX,         $0x0000003c
	JL          float64FloorTailLoop
	VMULPD     (AX), Y0, Y1
	VMULPD  32(AX), Y0, Y2
	VMULPD  64(AX), Y0, Y3
	VMULPD  96(AX), Y0, Y4
	VMULPD  128(AX), Y0, Y5
	VMULPD  160(AX), Y0, Y6
	VMULPD  192(AX), Y0, Y7
	VMULPD  224(AX), Y0, Y8
	VMULPD  256(AX), Y0, Y9
	VMULPD  288(AX), Y0, Y10
	VMULPD  320(AX), Y0, Y11
	VMULPD  352(AX), Y0, Y12
	VMULPD  384(AX), Y0, Y13
	VMULPD  416(AX), Y0, Y14
	VMULPD  448(AX), Y0, Y15
	VROUNDPD    $0x09, Y1, Y1
	VROUNDPD    $0x09, Y2, Y2
	VROUNDPD    $0x09, Y3, Y3
	VROUNDPD    $0x09, Y4, Y4
	VROUNDPD    $0x09, Y5, Y5
	VROUNDPD    $0x09, Y6, Y6
	VROUNDPD    $0x09, Y7, Y7
	VROUNDPD    $0x09, Y8, Y8
	VROUNDPD    $0x09, Y9, Y9
	VROUNDPD    $0x09, Y10, Y10
	VROUNDPD    $0x09, Y11, Y11
	VROUNDPD    $0x09, Y12, Y12
	VROUNDPD    $0x09, Y13, Y13
	VROUNDPD    $0x09, Y14, Y14
	VROUNDPD    $0x09, Y15, Y15
	VDIVPD      Y0, Y1, Y1
	VDIVPD      Y0, Y2, Y2
	VDIVPD      Y0, Y3, Y3
	VDIVPD      Y0, Y4, Y4
	VDIVPD      Y0, Y5, Y5
	VDIVPD      Y0, Y6, Y6
	VDIVPD      Y0, Y7, Y7
	VDIVPD      Y0, Y8, Y8
	VDIVPD      Y0, Y9, Y9
	VDIVPD      Y0, Y10, Y10
	VDIVPD      Y0, Y11, Y11
	VDIVPD      Y0, Y12, Y12
	VDIVPD      Y0, Y13, Y13
	VDIVPD      Y0, Y14, Y14
	VDIVPD      Y0, Y15, Y15
	VMOVUPD     Y1, (CX)
	VMOVUPD     Y2, 32(CX)
	VMOVUPD     Y3, 64(CX)
	VMOVUPD     Y4, 96(CX)
	VMOVUPD     Y5, 128(CX)
	VMOVUPD     Y6, 160(CX)
	VMOVUPD     Y7, 192(CX)
	VMOVUPD     Y8, 224(CX)
	VMOVUPD     Y9, 256(CX)
	VMOVUPD     Y10, 288(CX)
	VMOVUPD     Y11, 320(CX)
	VMOVUPD     Y12, 352(CX)
	VMOVUPD     Y13, 384(CX)
	VMOVUPD     Y14, 416(CX)
	VMOVUPD     Y15, 448(CX)
	ADDQ    $0x000001e0, AX
	ADDQ    $0x000001e0, CX
	SUBQ    $0x0000003c, DX
	JMP     float64FloorBlockLoop

float64FloorTailLoop:
	CMPQ    DX, $0x00000004
	JL      float64FloorDone
	VMULPD  (AX), Y0, Y1
	VROUNDPD    $0x09, Y1, Y1
	VDIVPD      Y0, Y1, Y1
	VMOVUPD Y1, (CX)
	ADDQ    $0x00000020, AX
	ADDQ    $0x00000020, CX
	SUBQ    $0x00000004, DX
	JMP     float64FloorTailLoop

float64FloorDone:
	CMPQ    DX, $0x00000002
	JL      float64FloorDone1
	VMULPD  (AX), X0, X1
	VROUNDPD    $0x09, X1, X1
	VDIVPD      X0, X1, X1
	VMOVUPD X1, (CX)
	ADDQ    $0x00000010, AX
	ADDQ    $0x00000010, CX
	SUBQ    $0x00000002, DX
	JMP     float64FloorDone

float64FloorDone1:
	CMPQ    DX, $0x00000000
	JL      float64FloorDone2
	VMULSD  (AX), X0, X1
	VROUNDPD    $0x09, X1, X1
	VDIVSD      X0, X1, X1
	VMOVSD X1, (CX)
	ADDQ    $0x00000008, AX
	ADDQ    $0x00000008, CX
	SUBQ    $0x00000001, DX
	JMP     float64FloorDone1

float64FloorDone2:
	RET


// func float64FloorAvx2AsmZero(xs, rs []float64, scale float64) Requires: AVX
TEXT ·float64FloorAvx2AsmZero(SB), NOSPLIT, $0-56
	MOVQ         xs_base+0(FP), AX
	MOVQ         rs_base+24(FP), CX
	MOVSD        scale+48(FP), X0
	MOVQ         xs_len+8(FP), DX
	VBROADCASTSD X0, Y0

float64FloorZeroBlockLoop:
	CMPQ        DX,         $0x0000003c
	JL          float64FloorZeroTailLoop
	VROUNDPD    $0x09, (AX), Y1
	VROUNDPD    $0x09, 32(AX), Y2
	VROUNDPD    $0x09, 64(AX), Y3
	VROUNDPD    $0x09, 96(AX), Y4
	VROUNDPD    $0x09, 128(AX), Y5
	VROUNDPD    $0x09, 160(AX), Y6
	VROUNDPD    $0x09, 192(AX), Y7
	VROUNDPD    $0x09, 224(AX), Y8
	VROUNDPD    $0x09, 256(AX), Y9
	VROUNDPD    $0x09, 288(AX), Y10
	VROUNDPD    $0x09, 320(AX), Y11
	VROUNDPD    $0x09, 352(AX), Y12
	VROUNDPD    $0x09, 384(AX), Y13
	VROUNDPD    $0x09, 416(AX), Y14
	VROUNDPD    $0x09, 448(AX), Y15
	VMOVUPD     Y1, (CX)
	VMOVUPD     Y2, 32(CX)
	VMOVUPD     Y3, 64(CX)
	VMOVUPD     Y4, 96(CX)
	VMOVUPD     Y5, 128(CX)
	VMOVUPD     Y6, 160(CX)
	VMOVUPD     Y7, 192(CX)
	VMOVUPD     Y8, 224(CX)
	VMOVUPD     Y9, 256(CX)
	VMOVUPD     Y10, 288(CX)
	VMOVUPD     Y11, 320(CX)
	VMOVUPD     Y12, 352(CX)
	VMOVUPD     Y13, 384(CX)
	VMOVUPD     Y14, 416(CX)
	VMOVUPD     Y15, 448(CX)
	ADDQ    $0x000001e0, AX
	ADDQ    $0x000001e0, CX
	SUBQ    $0x0000003c, DX
	JMP     float64FloorZeroBlockLoop

float64FloorZeroTailLoop:
	CMPQ    DX, $0x00000004
	JL      float64FloorZeroDone
	VROUNDPD    $0x09, (AX), Y1
	VMOVUPD Y1, (CX)
	ADDQ    $0x00000020, AX
	ADDQ    $0x00000020, CX
	SUBQ    $0x00000004, DX
	JMP     float64FloorZeroTailLoop

float64FloorZeroDone:
	CMPQ    DX, $0x00000002
	JL      float64FloorZeroDone1
	VROUNDPD    $0x09, (AX), X1
	VMOVUPD X1, (CX)
	ADDQ    $0x00000010, AX
	ADDQ    $0x00000010, CX
	SUBQ    $0x00000002, DX
	JMP     float64FloorZeroDone

float64FloorZeroDone1:
	CMPQ    DX, $0x00000000
	JL      float64FloorZeroDone2
	VROUNDSD    $0x09, (AX), X1, X1
	VMOVSD X1, (CX)
	ADDQ    $0x00000008, AX
	ADDQ    $0x00000008, CX
	SUBQ    $0x00000001, DX
	JMP     float64FloorZeroDone1

float64FloorZeroDone2:
	RET
