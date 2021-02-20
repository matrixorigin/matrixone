// +build ignore
//go:generate go run fplusone_amd64.go -out fplusone_amd64.s

package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

var unroll = 6

func main() {
	TEXT("fPlusOneAvx", NOSPLIT, "func(x float64, y, r []float64)")
	x := Load(Param("x"), XMM())
	y := Mem{Base: Load(Param("y").Base(), GP64())}
	r := Mem{Base: Load(Param("r").Base(), GP64())}
	n := Load(Param("y").Len(), GP64())

	xs := YMM()
	VBROADCASTSD(x, xs)

	// Loop over blocks and process them with vector instructions.
	blockitems := 4 * unroll
	blocksize := 8 * blockitems
	Label("blockloop")
	CMPQ(n, U32(blockitems))
	JL(LabelRef("tailloop"))

	// Load x.
	rs := make([]VecVirtual, unroll)
	for i := 0; i < unroll; i++ {
		rs[i] = YMM()
	}

	for i := 0; i < unroll; i++ {
		VADDPD(y.Offset(32*i), xs, rs[i])
	}

	for i := 0; i < unroll; i++ {
		VMOVUPD(rs[i], r.Offset(32*i))
	}

	ADDQ(U32(blocksize), y.Base)
	ADDQ(U32(blocksize), r.Base)
	SUBQ(U32(blockitems), n)
	JMP(LabelRef("blockloop"))

	// Process any trailing entries.
	Label("tailloop")
	CMPQ(n, U32(4))
	JL(LabelRef("done"))

	VADDPD(y.Offset(0), xs, rs[0])
	VMOVUPD(rs[0], r.Offset(0))

	ADDQ(U32(32), y.Base)
	ADDQ(U32(32), r.Base)
	SUBQ(U32(4), n)
	JMP(LabelRef("tailloop"))

	Label("done")
	RET()

	Generate()
}
