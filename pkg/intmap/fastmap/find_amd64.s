#include "textflag.h"

// func findAvx(x []uint64, y uint64) int
TEXT ·findAvx(SB), NOSPLIT, $0-40
	MOVQ        x_base+0(FP), DI
	MOVQ 		y+24(FP), AX
	MOVD 		 AX, X0
	VPBROADCASTQ X0, Y0
	MOVQ 		$0, CX
	MOVQ   		x_len+8(FP), AX
	SHRL 		$0x2, AX

loop:
	TESTQ 		AX, AX
	JZ 		end
	VMOVDQU 	(DI), Y1
	VPCMPEQQ 	Y0, Y1, Y1
	VPMOVMSKB 	Y1, DX

	TESTQ 		$0xFF000000, DX
	JNZ 		end3
	TESTQ 		$0x00FF0000, DX
	JNZ 		end2
	TESTQ 		$0x0000FF00, DX
	JNZ 		end1
	TESTQ 		$0x000000FF, DX
	JNZ 		end0
	ADDQ 		$0x04, CX
	ADDQ 		$0x20, DI
	SUBQ 		$0x01, AX
	JMP 		loop

end0:
	MOVQ 		CX, ret+32(FP)
	RET

end1:
	ADDQ 		$1, CX
	MOVQ 		CX, ret+32(FP)
	RET

end2:
	ADDQ 		$2, CX
	MOVQ 		CX, ret+32(FP)
	RET

end3:
	ADDQ 		$3, CX
	MOVQ 		CX, ret+32(FP)
	RET

end:
	MOVQ 		$-1, ret+32(FP)
	RET
