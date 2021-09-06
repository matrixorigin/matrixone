//+build !noasm !appengine

// func stage1_preprocess_test(input *stage1Input, output *stage1Output)
TEXT ·stage1_preprocess_test(SB), 7, $0
	MOVQ input+0(FP), AX
	MOVQ output+8(FP), R10
	CALL ·stage1_preprocess(SB)
	RET

// func stage1_preprocess()
TEXT ·stage1_preprocess(SB), 7, $0
	MOVQ    0x8(AX), DX
	BSFQ    DX, BX
	MOVQ    0x10(AX), SI
	BSFQ    SI, DI
	MOVQ    (AX), R8
	BSFQ    R8, R9
	MOVQ    R8, (R10)
	MOVQ    DX, 0x8(R10)
	MOVQ    SI, 0x10(R10)
	MOVQ    $0x0, 0x18(R10)
	BSFQ    DX, R11
	MOVL    $0x40, R11
	CMOVQEQ R11, BX
	BSFQ    SI, R12
	CMOVQEQ R11, DI
	BSFQ    R8, R12
	CMOVQEQ R11, R9

label1:
	CMPQ  R9, BX
	JGE   label7
	CMPQ  R9, DI
	JGE   label7
	MOVQ  0x20(AX), R12
	TESTQ R12, R12
	JE    label4
	CMPQ  R9, $0x3f
	JNE   label6
	MOVQ  0x18(AX), R13
	ANDQ  $0x1, R13
	CMPQ  R13, $0x1
	JNE   label3
	BTRQ  R9, (R10)
	MOVQ  $0x1, 0x18(R10)
	ANDQ  $-0x2, 0x18(AX)
	MOVQ  R9, CX
	MOVQ  $-0x2, R12
	SHLQ  CL, R12
	ANDQ  R12, R8

label2:
	BSFQ    R8, R9
	CMOVQEQ R11, R9
	JMP     label1

label3:
	TESTQ R12, R12

label4:
	JE    label5
	LEAQ  0x1(R9), CX
	CMPQ  CX, $0x40
	SBBQ  R13, R13
	MOVL  $0x1, R14
	SHLQ  CL, R14
	ANDQ  R13, R14
	TESTQ R8, R14
	JE    label5
	MOVQ  $-0x2, R12
	SHLQ  CL, R12
	ANDQ  R13, R12
	ANDQ  R12, R8
	CMPQ  R9, $0x40
	SBBQ  R12, R12
	MOVQ  R9, CX
	MOVL  $0x3, R13
	SHLQ  CL, R13
	ANDQ  R12, R13
	NOTQ  R13
	ANDQ  R13, (R10)
	MOVQ  $0x1, 0x18(R10)
	JMP   label2

label5:
	NOTQ R12
	MOVQ R12, 0x20(AX)
	CMPQ R9, $0x40
	SBBQ R12, R12
	MOVQ R9, CX
	MOVQ $-0x2, R13
	SHLQ CL, R13
	ANDQ R12, R13
	ANDQ R13, R8
	JMP  label2

label6:
	TESTQ R12, R12
	JMP   label4

label7:
	CMPQ BX, R9
	JGE  label10
	CMPQ BX, DI
	JGE  label10
	CMPQ 0x20(AX), $0x0
	JE   label9
	CMPQ BX, $0x40
	SBBQ R12, R12
	MOVQ BX, CX
	MOVL $0x1, R13
	SHLQ CL, R13
	ANDQ R12, R13
	NOTQ R13
	ANDQ R13, 0x8(R10)

label8:
	CMPQ    CX, $0x40
	SBBQ    R12, R12
	MOVQ    $-0x2, R13
	SHLQ    CL, R13
	ANDQ    R12, R13
	ANDQ    R13, DX
	BSFQ    DX, BX
	CMOVQEQ R11, BX
	JMP     label1

label9:
	MOVQ BX, CX
	JMP  label8

label10:
	CMPQ DI, R9
	JGE  label16
	CMPQ DI, BX
	JGE  label16
	CMPQ 0x20(AX), $0x0
	JE   label12
	CMPQ DI, $0x40
	SBBQ R12, R12
	MOVQ DI, CX
	MOVL $0x1, R13
	SHLQ CL, R13
	ANDQ R12, R13
	NOTQ R13
	ANDQ R13, 0x10(R10)
	MOVQ $0x1, 0x18(R10)

label11:
	CMPQ    CX, $0x40
	SBBQ    R12, R12
	MOVQ    $-0x2, R13
	SHLQ    CL, R13
	ANDQ    R12, R13
	ANDQ    R13, SI
	BSFQ    SI, R12
	CMOVQEQ R11, R12
	MOVQ    R12, DI
	JMP     label1

label12:
	CMPQ DI, $0x3f
	JNE  label14
	MOVQ 0x30(AX), R12
	BTL  $0x0, R12
	JB   label13
	BTRQ DI, 0x10(R10)

label13:
	MOVQ DI, CX
	JMP  label11

label14:
	MOVQ  0x28(AX), R12
	LEAQ  0x1(DI), CX
	CMPQ  CX, $0x40
	SBBQ  R13, R13
	MOVL  $0x1, R14
	SHLQ  CL, R14
	ANDQ  R13, R14
	TESTQ R12, R14
	JNE   label15
	CMPQ  DI, $0x40
	SBBQ  R12, R12
	MOVQ  DI, CX
	MOVL  $0x1, R13
	SHLQ  CL, R13
	ANDQ  R12, R13
	NOTQ  R13
	ANDQ  R13, 0x10(R10)
	JMP   label13

label15:
	MOVQ DI, CX
	JMP  label13

label16:
	RET

