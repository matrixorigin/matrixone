package neg

import (
	"golang.org/x/sys/cpu"
)

var (
	int8Neg    func([]int8, []int8) []int8
	int16Neg   func([]int16, []int16) []int16
	int32Neg   func([]int32, []int32) []int32
	int64Neg   func([]int64, []int64) []int64
	float32Neg func([]float32, []float32) []float32
	float64Neg func([]float64, []float64) []float64
)

func init() {
	if cpu.X86.HasAVX512 {
		int8Neg = int8NegAvx512
		int16Neg = int16NegAvx512
		int32Neg = int32NegAvx512
		int64Neg = int64NegAvx512
		float32Neg = float32NegAvx512
		float64Neg = float64NegAvx512
	} else if cpu.X86.HasAVX2 {
		int8Neg = int8NegAvx2
		int16Neg = int16NegAvx2
		int32Neg = int32NegAvx2
		int64Neg = int64NegAvx2
		float32Neg = float32NegAvx2
		float64Neg = float64NegAvx2
	} else {
		int8Neg = int8NegPure
		int16Neg = int16NegPure
		int32Neg = int32NegPure
		int64Neg = int64NegPure
		float32Neg = float32NegPure
		float64Neg = float64NegPure
	}
}

func Int8Neg(xs, rs []int8) []int8 {
	return int8Neg(xs, rs)
}

func int8NegPure(xs, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int8NegAvx2(xs, rs []int8) []int8 {
	n := len(xs) / 16
	int8NegAvx2Asm(xs[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func int8NegAvx512(xs, rs []int8) []int8 {
	n := len(xs) / 16
	int8NegAvx512Asm(xs[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func Int16Neg(xs, rs []int16) []int16 {
	return int16Neg(xs, rs)
}

func int16NegPure(xs, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int16NegAvx2(xs, rs []int16) []int16 {
	n := len(xs) / 8
	int16NegAvx2Asm(xs[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func int16NegAvx512(xs, rs []int16) []int16 {
	n := len(xs) / 8
	int16NegAvx512Asm(xs[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func Int32Neg(xs, rs []int32) []int32 {
	return int32Neg(xs, rs)
}

func int32NegPure(xs, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int32NegAvx2(xs, rs []int32) []int32 {
	n := len(xs) / 4
	int32NegAvx2Asm(xs[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func int32NegAvx512(xs, rs []int32) []int32 {
	n := len(xs) / 4
	int32NegAvx512Asm(xs[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func Int64Neg(xs, rs []int64) []int64 {
	return int64Neg(xs, rs)
}

func int64NegPure(xs, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int64NegAvx2(xs, rs []int64) []int64 {
	n := len(xs) / 2
	int64NegAvx2Asm(xs[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func int64NegAvx512(xs, rs []int64) []int64 {
	n := len(xs) / 2
	int64NegAvx512Asm(xs[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func Float32Neg(xs, rs []float32) []float32 {
	return float32Neg(xs, rs)
}

func float32NegPure(xs, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func float32NegAvx2(xs, rs []float32) []float32 {
	n := len(xs) / 4
	float32NegAvx2Asm(xs[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func float32NegAvx512(xs, rs []float32) []float32 {
	n := len(xs) / 4
	float32NegAvx512Asm(xs[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func Float64Neg(xs, rs []float64) []float64 {
	return float64Neg(xs, rs)
}

func float64NegPure(xs, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func float64NegAvx2(xs, rs []float64) []float64 {
	n := len(xs) / 2
	float64NegAvx2Asm(xs[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}

func float64NegAvx512(xs, rs []float64) []float64 {
	n := len(xs) / 2
	float64NegAvx512Asm(xs[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = -xs[i]
	}
	return rs
}
