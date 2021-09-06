//+build !noasm !appengine

// See Input struct
#define INPUT_BASE   0x38

// See Output struct
#define COLUMNS_BASE 0x0
#define INDEX_OFFSET 0x8
#define ROWS_BASE    0x10
#define LINE_OFFSET  0x18

#define INPUT_STAGE2_SEPARATOR_MASK 0
#define INPUT_STAGE2_DELIMITER_MASK 8
#define INPUT_STAGE2_QUOTE_MASK     16

#define Y_DELIMITER   Y4
#define Y_SEPARATOR   Y5
#define Y_QUOTE_CHAR  Y6

// func stage2_parse_test(input *Input, offset uint64, output *Output)
TEXT ·stage2_parse_test(SB), 7, $0
	MOVQ input+0(FP), DX
	MOVQ offset+8(FP), DI
	MOVQ output+16(FP), R9
	CALL ·stage2_parse(SB)
	RET

// func stage2_parse()
TEXT ·stage2_parse(SB), 7, $0
	MOVL    $0x40, BX
	MOVQ    (DX), SI
	BSFQ    SI, SI
	CMOVQEQ BX, SI
	MOVQ    0x8(DX), R8
	BSFQ    R8, R8
	CMOVQEQ BX, R8
	MOVQ    0x10(DX), R10
	BSFQ    R10, R10
	CMOVQEQ BX, R10

label1:
	CMPQ  SI, R8
	JGE   label5
	CMPQ  SI, R10
	JGE   label5
	CMPQ  0x18(DX), $0x0
	JNE   label4
	MOVQ  0x28(DX), R11
	TESTQ R11, R11
	JBE   label2
	INCQ  R11
	LEAQ  (SI)(DI*1), R12
	CMPQ  R12, R11
	JE    label2
	CMPQ  0x30(DX), $0x0
	JNE   label2
	MOVQ  R12, 0x30(DX)

label2:
	MOVQ  $0x0, 0x28(DX)
	MOVQ  (R9), R11
	TESTB AL, (R11)
	MOVQ  0x8(R9), AX
	MOVQ  0x38(DX), R12
	ADDQ  0x20(R9), R12
	MOVQ  R12, (R11)(AX*8)
	MOVQ  0x8(R9), R11
	LEAQ  0x1(R11), AX
	MOVQ  AX, 0x8(R9)
	MOVQ  (R9), R12
	TESTB AL, (R12)
	MOVQ  0x28(R9), R13
	MOVQ  SI, CX
	SUBQ  R13, SI
	ADDQ  DI, SI
	SUBQ  0x20(R9), SI
	MOVQ  SI, 0x8(R12)(R11*8)
	INCQ  0x8(R9)
	LEAQ  (CX)(DI*1), SI
	LEAQ  0x1(SI), SI
	MOVQ  SI, 0x20(R9)
	MOVQ  $0x0, 0x28(R9)
	LEAQ  (CX)(DI*1), SI
	MOVQ  SI, 0x20(DX)

label3:
	MOVQ    (DX), SI
	CMPQ    CX, $0x40
	SBBQ    R11, R11
	MOVQ    $-0x2, R12
	SHLQ    CL, R12
	ANDQ    R11, R12
	ANDQ    SI, R12
	BSFQ    R12, SI
	MOVQ    R12, (DX)
	CMOVQEQ BX, SI
	JMP     label1

label4:
	MOVQ SI, CX
	JMP  label3

label5:
	CMPQ  R8, SI
	JGE   label13
	CMPQ  R8, R10
	JGE   label13
	CMPQ  0x18(DX), $0x0
	JNE   label12
	MOVQ  0x28(DX), R11
	TESTQ R11, R11
	JBE   label6
	INCQ  R11
	LEAQ  (R8)(DI*1), R12
	CMPQ  R12, R11
	JE    label6
	CMPQ  0x30(DX), $0x0
	JNE   label6
	MOVQ  R12, 0x30(DX)

label6:
	MOVQ  $0x0, 0x28(DX)
	MOVQ  0x28(R9), R11
	MOVQ  R8, CX
	SUBQ  R11, R8
	ADDQ  DI, R8
	MOVQ  0x20(R9), R11
	CMPQ  R11, R8
	JNE   label11
	MOVQ  (R9), R8
	TESTB AL, (R8)
	MOVQ  0x8(R9), AX
	MOVQ  $0x0, (R8)(AX*8)

label7:
	MOVQ  0x8(R9), R8
	LEAQ  0x1(R8), AX
	MOVQ  AX, 0x8(R9)
	MOVQ  (R9), R11
	TESTB AL, (R11)
	MOVQ  0x28(R9), R12
	MOVQ  CX, R13
	SUBQ  R12, CX
	LEAQ  (CX)(DI*1), R12
	SUBQ  0x20(R9), R12
	MOVQ  R12, 0x8(R11)(R8*8)
	MOVQ  0x8(R9), AX
	LEAQ  0x1(AX), R8
	MOVQ  R8, 0x8(R9)
	LEAQ  (R13)(DI*1), R11
	LEAQ  0x1(R11), R11
	MOVQ  R11, 0x20(R9)
	MOVQ  $0x0, 0x28(R9)
	LEAQ  (R13)(DI*1), R11
	SHRQ  $0x1, R8
	MOVQ  0x30(R9), R12
	SUBQ  R12, R8
	CMPQ  R8, $0x1
	JNE   label10
	MOVQ  (R9), R8
	TESTB AL, (R8)
	MOVQ  (R8)(AX*8), R8
	TESTQ R8, R8
	JNE   label10

label8:
	MOVQ 0x8(R9), R8
	SHRQ $0x1, R8
	MOVQ R8, 0x30(R9)
	MOVQ R11, 0x20(DX)

label9:
	MOVQ    0x8(DX), R8
	CMPQ    R13, $0x40
	SBBQ    R11, R11
	MOVQ    R13, CX
	MOVQ    $-0x2, R12
	SHLQ    CL, R12
	ANDQ    R11, R12
	ANDQ    R8, R12
	BSFQ    R12, R8
	MOVQ    R12, 0x8(DX)
	CMOVQEQ BX, R8
	JMP     label1

label10:
	MOVQ  0x10(R9), R8
	TESTB AL, (R8)
	MOVQ  0x18(R9), AX
	MOVQ  R12, (R8)(AX*8)
	MOVQ  0x18(R9), R8
	LEAQ  0x1(R8), AX
	MOVQ  AX, 0x18(R9)
	MOVQ  0x10(R9), R12
	TESTB AL, (R12)
	MOVQ  0x8(R9), R14
	SHRQ  $0x1, R14
	SUBQ  0x30(R9), R14
	MOVQ  R14, 0x8(R12)(R8*8)
	INCQ  0x18(R9)
	JMP   label8

label11:
	MOVQ  (R9), R8
	TESTB AL, (R8)
	MOVQ  0x8(R9), AX
	MOVQ  0x38(DX), R12
	ADDQ  R12, R11
	MOVQ  R11, (R8)(AX*8)
	JMP   label7

label12:
	MOVQ R8, R13
	JMP  label9

label13:
	CMPQ R10, SI
	JGE  label17
	CMPQ R10, R8
	JGE  label17
	CMPQ 0x18(DX), $0x0
	JNE  label16
	MOVQ 0x20(DX), R11
	INCQ R11
	LEAQ (R10)(DI*1), R12
	CMPQ R11, R12
	JE   label14
	CMPQ 0x30(DX), $0x0
	JNE  label14
	MOVQ R12, 0x30(DX)

label14:
	INCQ 0x20(R9)

label15:
	MOVQ    0x18(DX), R11
	NOTQ    R11
	MOVQ    R11, 0x18(DX)
	MOVQ    0x10(DX), R11
	CMPQ    R10, $0x40
	SBBQ    R12, R12
	MOVQ    R10, CX
	MOVQ    $-0x2, R13
	SHLQ    CL, R13
	ANDQ    R13, R12
	ANDQ    R11, R12
	BSFQ    R12, R11
	MOVQ    R12, 0x10(DX)
	CMOVQEQ BX, R11
	MOVQ    R11, R10
	JMP     label1

label16:
	INCQ 0x28(R9)
	LEAQ (R10)(DI*1), R11
	MOVQ R11, 0x28(DX)
	JMP  label15

label17:
	RET
