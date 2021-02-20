// +build ignore
//go:generate go run iplusone_amd64.go -out iplusone_amd64.s

package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

var unroll = 6

func main() {
	TEXT("iPlusOneAvx", NOSPLIT, "func(x int64, y, r []int64)")
	x := Load(Param("x"), GP64())
	y := Mem{Base: Load(Param("y").Base(), GP64())}
	r := Mem{Base: Load(Param("r").Base(), GP64())}
	n := Load(Param("y").Len(), GP64())

	xs1 := XMM()
	MOVQ(x, xs1)
	xs := YMM()
	VPBROADCASTQ(xs1, xs)

	blockitems := 4 * unroll
	blocksize := 8 * blockitems
	Label("blockloop")
	CMPQ(n, U32(blockitems))
	JL(LabelRef("tailloop"))

	rs := make([]VecVirtual, unroll)
	for i := 0; i < unroll; i++ {
		rs[i] = YMM()
	}

	for i := 0; i < unroll; i++ {
		VPADDQ(y.Offset(32*i), xs, rs[i])
	}

	for i := 0; i < unroll; i++ {
		VMOVDQU(rs[i], r.Offset(32*i))
	}

	ADDQ(U32(blocksize), y.Base)
	ADDQ(U32(blocksize), r.Base)
	SUBQ(U32(blockitems), n)
	JMP(LabelRef("blockloop"))

	Label("tailloop")
	CMPQ(n, U32(4))
	JL(LabelRef("done"))

	VPADDQ(y.Offset(0), xs, rs[0])
	VMOVDQU(rs[0], r.Offset(0))

	ADDQ(U32(32), y.Base)
	ADDQ(U32(32), r.Base)
	SUBQ(U32(4), n)
	JMP(LabelRef("tailloop"))

	Label("done")
	RET()

	Generate()
}
