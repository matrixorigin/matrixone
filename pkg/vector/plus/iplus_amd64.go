// +build ignore
//go:generate go run iplus_amd64.go -out iplus_amd64.s

package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

var unroll = 6

func main() {
	TEXT("iPlusAvx", NOSPLIT, "func(x, y, r []int64)")
	x := Mem{Base: Load(Param("x").Base(), GP64())}
	y := Mem{Base: Load(Param("y").Base(), GP64())}
	r := Mem{Base: Load(Param("r").Base(), GP64())}
	n := Load(Param("x").Len(), GP64())

	blockitems := 4 * unroll
	blocksize := 8 * blockitems
	Label("blockloop")
	CMPQ(n, U32(blockitems))
	JL(LabelRef("tailloop"))

	// Load x.
	xs := make([]VecVirtual, unroll)
	for i := 0; i < unroll; i++ {
		xs[i] = YMM()
	}

	for i := 0; i < unroll; i++ {
		VMOVDQU(x.Offset(32*i), xs[i])
	}

	for i := 0; i < unroll; i++ {
		VPADDQ(y.Offset(32*i), xs[i], xs[i])
	}

	for i := 0; i < unroll; i++ {
		VMOVDQU(xs[i], r.Offset(32*i))
	}

	ADDQ(U32(blocksize), x.Base)
	ADDQ(U32(blocksize), y.Base)
	ADDQ(U32(blocksize), r.Base)
	SUBQ(U32(blockitems), n)
	JMP(LabelRef("blockloop"))

	Label("tailloop")
	CMPQ(n, U32(4))
	JL(LabelRef("done"))

	VMOVDQU(x.Offset(0), xs[0])
	VPADDQ(y.Offset(0), xs[0], xs[0])
	VMOVDQU(xs[0], r.Offset(0))

	ADDQ(U32(32), x.Base)
	ADDQ(U32(32), y.Base)
	ADDQ(U32(32), r.Base)
	SUBQ(U32(4), n)
	JMP(LabelRef("tailloop"))

	Label("done")
	RET()

	Generate()
}
