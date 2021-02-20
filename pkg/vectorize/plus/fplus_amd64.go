// +build ignore
//go:generate go run fplus_amd64.go -out fplus_amd64.s

package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

var unroll = 6

func main() {
	TEXT("fPlusAvx", NOSPLIT, "func(x, y, r []float64)")
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
		VMOVUPD(x.Offset(32*i), xs[i])
	}

	for i := 0; i < unroll; i++ {
		VADDPD(y.Offset(32*i), xs[i], xs[i])
	}

	for i := 0; i < unroll; i++ {
		VMOVUPD(xs[i], r.Offset(32*i))
	}

	ADDQ(U32(blocksize), x.Base)
	ADDQ(U32(blocksize), y.Base)
	ADDQ(U32(blocksize), r.Base)
	SUBQ(U32(blockitems), n)
	JMP(LabelRef("blockloop"))

	Label("tailloop")
	CMPQ(n, U32(4))
	JL(LabelRef("done"))

	VMOVUPD(x.Offset(0), xs[0])
	VADDPD(y.Offset(0), xs[0], xs[0])
	VMOVUPD(xs[0], r.Offset(0))

	ADDQ(U32(32), x.Base)
	ADDQ(U32(32), y.Base)
	ADDQ(U32(32), r.Base)
	SUBQ(U32(4), n)
	JMP(LabelRef("tailloop"))

	Label("done")
	RET()

	Generate()
}
