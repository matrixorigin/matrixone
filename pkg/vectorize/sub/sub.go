package minus

import "golang.org/x/sys/cpu"

var (
    int8Minus func([]int8, []int8, []int8) []int8
    int8MinusSels func([]int8, []int8, []int8, []int64) []int8
    int8MinusScalar func(int8, []int8, []int8) []int8
    int8MinusScalarSels func(int8, []int8, []int8, []int64) []int8
    int8MinusByScalar func(int8, []int8, []int8) []int8
    int8MinusByScalarSels func(int8, []int8, []int8, []int64) []int8
    int16Minus func([]int16, []int16, []int16) []int16
    int16MinusSels func([]int16, []int16, []int16, []int64) []int16
    int16MinusScalar func(int16, []int16, []int16) []int16
    int16MinusScalarSels func(int16, []int16, []int16, []int64) []int16
    int16MinusByScalar func(int16, []int16, []int16) []int16
    int16MinusByScalarSels func(int16, []int16, []int16, []int64) []int16
    int32Minus func([]int32, []int32, []int32) []int32
    int32MinusSels func([]int32, []int32, []int32, []int64) []int32
    int32MinusScalar func(int32, []int32, []int32) []int32
    int32MinusScalarSels func(int32, []int32, []int32, []int64) []int32
    int32MinusByScalar func(int32, []int32, []int32) []int32
    int32MinusByScalarSels func(int32, []int32, []int32, []int64) []int32
    int64Minus func([]int64, []int64, []int64) []int64
    int64MinusSels func([]int64, []int64, []int64, []int64) []int64
    int64MinusScalar func(int64, []int64, []int64) []int64
    int64MinusScalarSels func(int64, []int64, []int64, []int64) []int64
    int64MinusByScalar func(int64, []int64, []int64) []int64
    int64MinusByScalarSels func(int64, []int64, []int64, []int64) []int64
    uint8Minus func([]uint8, []uint8, []uint8) []uint8
    uint8MinusSels func([]uint8, []uint8, []uint8, []int64) []uint8
    uint8MinusScalar func(uint8, []uint8, []uint8) []uint8
    uint8MinusScalarSels func(uint8, []uint8, []uint8, []int64) []uint8
    uint8MinusByScalar func(uint8, []uint8, []uint8) []uint8
    uint8MinusByScalarSels func(uint8, []uint8, []uint8, []int64) []uint8
    uint16Minus func([]uint16, []uint16, []uint16) []uint16
    uint16MinusSels func([]uint16, []uint16, []uint16, []int64) []uint16
    uint16MinusScalar func(uint16, []uint16, []uint16) []uint16
    uint16MinusScalarSels func(uint16, []uint16, []uint16, []int64) []uint16
    uint16MinusByScalar func(uint16, []uint16, []uint16) []uint16
    uint16MinusByScalarSels func(uint16, []uint16, []uint16, []int64) []uint16
    uint32Minus func([]uint32, []uint32, []uint32) []uint32
    uint32MinusSels func([]uint32, []uint32, []uint32, []int64) []uint32
    uint32MinusScalar func(uint32, []uint32, []uint32) []uint32
    uint32MinusScalarSels func(uint32, []uint32, []uint32, []int64) []uint32
    uint32MinusByScalar func(uint32, []uint32, []uint32) []uint32
    uint32MinusByScalarSels func(uint32, []uint32, []uint32, []int64) []uint32
    uint64Minus func([]uint64, []uint64, []uint64) []uint64
    uint64MinusSels func([]uint64, []uint64, []uint64, []int64) []uint64
    uint64MinusScalar func(uint64, []uint64, []uint64) []uint64
    uint64MinusScalarSels func(uint64, []uint64, []uint64, []int64) []uint64
    uint64MinusByScalar func(uint64, []uint64, []uint64) []uint64
    uint64MinusByScalarSels func(uint64, []uint64, []uint64, []int64) []uint64
    float32Minus func([]float32, []float32, []float32) []float32
    float32MinusSels func([]float32, []float32, []float32, []int64) []float32
    float32MinusScalar func(float32, []float32, []float32) []float32
    float32MinusScalarSels func(float32, []float32, []float32, []int64) []float32
    float32MinusByScalar func(float32, []float32, []float32) []float32
    float32MinusByScalarSels func(float32, []float32, []float32, []int64) []float32
    float64Minus func([]float64, []float64, []float64) []float64
    float64MinusSels func([]float64, []float64, []float64, []int64) []float64
    float64MinusScalar func(float64, []float64, []float64) []float64
    float64MinusScalarSels func(float64, []float64, []float64, []int64) []float64
    float64MinusByScalar func(float64, []float64, []float64) []float64
    float64MinusByScalarSels func(float64, []float64, []float64, []int64) []float64
)

func init() {
	if cpu.X86.HasAVX512 {
        int8Minus = int8MinusAvx512
        //int8MinusSels = int8MinusSelsAvx512
        int8MinusScalar = int8MinusScalarAvx512
        //int8MinusScalarSels = int8MinusScalarSelsAvx512
        int8MinusByScalar = int8MinusByScalarAvx512
        //int8MinusByScalarSels = int8MinusByScalarSelsAvx512
        int16Minus = int16MinusAvx512
        //int16MinusSels = int16MinusSelsAvx512
        int16MinusScalar = int16MinusScalarAvx512
        //int16MinusScalarSels = int16MinusScalarSelsAvx512
        int16MinusByScalar = int16MinusByScalarAvx512
        //int16MinusByScalarSels = int16MinusByScalarSelsAvx512
        int32Minus = int32MinusAvx512
        //int32MinusSels = int32MinusSelsAvx512
        int32MinusScalar = int32MinusScalarAvx512
        //int32MinusScalarSels = int32MinusScalarSelsAvx512
        int32MinusByScalar = int32MinusByScalarAvx512
        //int32MinusByScalarSels = int32MinusByScalarSelsAvx512
        int64Minus = int64MinusAvx512
        //int64MinusSels = int64MinusSelsAvx512
        int64MinusScalar = int64MinusScalarAvx512
        //int64MinusScalarSels = int64MinusScalarSelsAvx512
        int64MinusByScalar = int64MinusByScalarAvx512
        //int64MinusByScalarSels = int64MinusByScalarSelsAvx512
        uint8Minus = uint8MinusAvx512
        //uint8MinusSels = uint8MinusSelsAvx512
        uint8MinusScalar = uint8MinusScalarAvx512
        //uint8MinusScalarSels = uint8MinusScalarSelsAvx512
        uint8MinusByScalar = uint8MinusByScalarAvx512
        //uint8MinusByScalarSels = uint8MinusByScalarSelsAvx512
        uint16Minus = uint16MinusAvx512
        //uint16MinusSels = uint16MinusSelsAvx512
        uint16MinusScalar = uint16MinusScalarAvx512
        //uint16MinusScalarSels = uint16MinusScalarSelsAvx512
        uint16MinusByScalar = uint16MinusByScalarAvx512
        //uint16MinusByScalarSels = uint16MinusByScalarSelsAvx512
        uint32Minus = uint32MinusAvx512
        //uint32MinusSels = uint32MinusSelsAvx512
        uint32MinusScalar = uint32MinusScalarAvx512
        //uint32MinusScalarSels = uint32MinusScalarSelsAvx512
        uint32MinusByScalar = uint32MinusByScalarAvx512
        //uint32MinusByScalarSels = uint32MinusByScalarSelsAvx512
        uint64Minus = uint64MinusAvx512
        //uint64MinusSels = uint64MinusSelsAvx512
        uint64MinusScalar = uint64MinusScalarAvx512
        //uint64MinusScalarSels = uint64MinusScalarSelsAvx512
        uint64MinusByScalar = uint64MinusByScalarAvx512
        //uint64MinusByScalarSels = uint64MinusByScalarSelsAvx512
        float32Minus = float32MinusAvx512
        //float32MinusSels = float32MinusSelsAvx512
        float32MinusScalar = float32MinusScalarAvx512
        //float32MinusScalarSels = float32MinusScalarSelsAvx512
        float32MinusByScalar = float32MinusByScalarAvx512
        //float32MinusByScalarSels = float32MinusByScalarSelsAvx512
        float64Minus = float64MinusAvx512
        //float64MinusSels = float64MinusSelsAvx512
        float64MinusScalar = float64MinusScalarAvx512
        //float64MinusScalarSels = float64MinusScalarSelsAvx512
        float64MinusByScalar = float64MinusByScalarAvx512
        //float64MinusByScalarSels = float64MinusByScalarSelsAvx512
	} else if cpu.X86.HasAVX2 {
        int8Minus = int8MinusAvx2
        //int8MinusSels = int8MinusSelsAvx2
        int8MinusScalar = int8MinusScalarAvx2
        //int8MinusScalarSels = int8MinusScalarSelsAvx2
        int8MinusByScalar = int8MinusByScalarAvx2
        //int8MinusByScalarSels = int8MinusByScalarSelsAvx2
        int16Minus = int16MinusAvx2
        //int16MinusSels = int16MinusSelsAvx2
        int16MinusScalar = int16MinusScalarAvx2
        //int16MinusScalarSels = int16MinusScalarSelsAvx2
        int16MinusByScalar = int16MinusByScalarAvx2
        //int16MinusByScalarSels = int16MinusByScalarSelsAvx2
        int32Minus = int32MinusAvx2
        //int32MinusSels = int32MinusSelsAvx2
        int32MinusScalar = int32MinusScalarAvx2
        //int32MinusScalarSels = int32MinusScalarSelsAvx2
        int32MinusByScalar = int32MinusByScalarAvx2
        //int32MinusByScalarSels = int32MinusByScalarSelsAvx2
        int64Minus = int64MinusAvx2
        //int64MinusSels = int64MinusSelsAvx2
        int64MinusScalar = int64MinusScalarAvx2
        //int64MinusScalarSels = int64MinusScalarSelsAvx2
        int64MinusByScalar = int64MinusByScalarAvx2
        //int64MinusByScalarSels = int64MinusByScalarSelsAvx2
        uint8Minus = uint8MinusAvx2
        //uint8MinusSels = uint8MinusSelsAvx2
        uint8MinusScalar = uint8MinusScalarAvx2
        //uint8MinusScalarSels = uint8MinusScalarSelsAvx2
        uint8MinusByScalar = uint8MinusByScalarAvx2
        //uint8MinusByScalarSels = uint8MinusByScalarSelsAvx2
        uint16Minus = uint16MinusAvx2
        //uint16MinusSels = uint16MinusSelsAvx2
        uint16MinusScalar = uint16MinusScalarAvx2
        //uint16MinusScalarSels = uint16MinusScalarSelsAvx2
        uint16MinusByScalar = uint16MinusByScalarAvx2
        //uint16MinusByScalarSels = uint16MinusByScalarSelsAvx2
        uint32Minus = uint32MinusAvx2
        //uint32MinusSels = uint32MinusSelsAvx2
        uint32MinusScalar = uint32MinusScalarAvx2
        //uint32MinusScalarSels = uint32MinusScalarSelsAvx2
        uint32MinusByScalar = uint32MinusByScalarAvx2
        //uint32MinusByScalarSels = uint32MinusByScalarSelsAvx2
        uint64Minus = uint64MinusAvx2
        //uint64MinusSels = uint64MinusSelsAvx2
        uint64MinusScalar = uint64MinusScalarAvx2
        //uint64MinusScalarSels = uint64MinusScalarSelsAvx2
        uint64MinusByScalar = uint64MinusByScalarAvx2
        //uint64MinusByScalarSels = uint64MinusByScalarSelsAvx2
        float32Minus = float32MinusAvx2
        //float32MinusSels = float32MinusSelsAvx2
        float32MinusScalar = float32MinusScalarAvx2
        //float32MinusScalarSels = float32MinusScalarSelsAvx2
        float32MinusByScalar = float32MinusByScalarAvx2
        //float32MinusByScalarSels = float32MinusByScalarSelsAvx2
        float64Minus = float64MinusAvx2
        //float64MinusSels = float64MinusSelsAvx2
        float64MinusScalar = float64MinusScalarAvx2
        //float64MinusScalarSels = float64MinusScalarSelsAvx2
        float64MinusByScalar = float64MinusByScalarAvx2
        //float64MinusByScalarSels = float64MinusByScalarSelsAvx2
	} else {
        int8Minus = int8MinusPure
        //int8MinusSels = int8MinusSelsPure
        int8MinusScalar = int8MinusScalarPure
        //int8MinusScalarSels = int8MinusScalarSelsPure
        int8MinusByScalar = int8MinusByScalarPure
        //int8MinusByScalarSels = int8MinusByScalarSelsPure
        int16Minus = int16MinusPure
        //int16MinusSels = int16MinusSelsPure
        int16MinusScalar = int16MinusScalarPure
        //int16MinusScalarSels = int16MinusScalarSelsPure
        int16MinusByScalar = int16MinusByScalarPure
        //int16MinusByScalarSels = int16MinusByScalarSelsPure
        int32Minus = int32MinusPure
        //int32MinusSels = int32MinusSelsPure
        int32MinusScalar = int32MinusScalarPure
        //int32MinusScalarSels = int32MinusScalarSelsPure
        int32MinusByScalar = int32MinusByScalarPure
        //int32MinusByScalarSels = int32MinusByScalarSelsPure
        int64Minus = int64MinusPure
        //int64MinusSels = int64MinusSelsPure
        int64MinusScalar = int64MinusScalarPure
        //int64MinusScalarSels = int64MinusScalarSelsPure
        int64MinusByScalar = int64MinusByScalarPure
        //int64MinusByScalarSels = int64MinusByScalarSelsPure
        uint8Minus = uint8MinusPure
        //uint8MinusSels = uint8MinusSelsPure
        uint8MinusScalar = uint8MinusScalarPure
        //uint8MinusScalarSels = uint8MinusScalarSelsPure
        uint8MinusByScalar = uint8MinusByScalarPure
        //uint8MinusByScalarSels = uint8MinusByScalarSelsPure
        uint16Minus = uint16MinusPure
        //uint16MinusSels = uint16MinusSelsPure
        uint16MinusScalar = uint16MinusScalarPure
        //uint16MinusScalarSels = uint16MinusScalarSelsPure
        uint16MinusByScalar = uint16MinusByScalarPure
        //uint16MinusByScalarSels = uint16MinusByScalarSelsPure
        uint32Minus = uint32MinusPure
        //uint32MinusSels = uint32MinusSelsPure
        uint32MinusScalar = uint32MinusScalarPure
        //uint32MinusScalarSels = uint32MinusScalarSelsPure
        uint32MinusByScalar = uint32MinusByScalarPure
        //uint32MinusByScalarSels = uint32MinusByScalarSelsPure
        uint64Minus = uint64MinusPure
        //uint64MinusSels = uint64MinusSelsPure
        uint64MinusScalar = uint64MinusScalarPure
        //uint64MinusScalarSels = uint64MinusScalarSelsPure
        uint64MinusByScalar = uint64MinusByScalarPure
        //uint64MinusByScalarSels = uint64MinusByScalarSelsPure
        float32Minus = float32MinusPure
        //float32MinusSels = float32MinusSelsPure
        float32MinusScalar = float32MinusScalarPure
        //float32MinusScalarSels = float32MinusScalarSelsPure
        float32MinusByScalar = float32MinusByScalarPure
        //float32MinusByScalarSels = float32MinusByScalarSelsPure
        float64Minus = float64MinusPure
        //float64MinusSels = float64MinusSelsPure
        float64MinusScalar = float64MinusScalarPure
        //float64MinusScalarSels = float64MinusScalarSelsPure
        float64MinusByScalar = float64MinusByScalarPure
        //float64MinusByScalarSels = float64MinusByScalarSelsPure
	}
	int8MinusSels = int8MinusSelsPure
	int8MinusScalarSels = int8MinusScalarSelsPure
	int8MinusByScalarSels = int8MinusByScalarSelsPure
	int16MinusSels = int16MinusSelsPure
	int16MinusScalarSels = int16MinusScalarSelsPure
	int16MinusByScalarSels = int16MinusByScalarSelsPure
	int32MinusSels = int32MinusSelsPure
	int32MinusScalarSels = int32MinusScalarSelsPure
	int32MinusByScalarSels = int32MinusByScalarSelsPure
	int64MinusSels = int64MinusSelsPure
	int64MinusScalarSels = int64MinusScalarSelsPure
	int64MinusByScalarSels = int64MinusByScalarSelsPure
	uint8MinusSels = uint8MinusSelsPure
	uint8MinusScalarSels = uint8MinusScalarSelsPure
	uint8MinusByScalarSels = uint8MinusByScalarSelsPure
	uint16MinusSels = uint16MinusSelsPure
	uint16MinusScalarSels = uint16MinusScalarSelsPure
	uint16MinusByScalarSels = uint16MinusByScalarSelsPure
	uint32MinusSels = uint32MinusSelsPure
	uint32MinusScalarSels = uint32MinusScalarSelsPure
	uint32MinusByScalarSels = uint32MinusByScalarSelsPure
	uint64MinusSels = uint64MinusSelsPure
	uint64MinusScalarSels = uint64MinusScalarSelsPure
	uint64MinusByScalarSels = uint64MinusByScalarSelsPure
	float32MinusSels = float32MinusSelsPure
	float32MinusScalarSels = float32MinusScalarSelsPure
	float32MinusByScalarSels = float32MinusByScalarSelsPure
	float64MinusSels = float64MinusSelsPure
	float64MinusScalarSels = float64MinusScalarSelsPure
	float64MinusByScalarSels = float64MinusByScalarSelsPure
}

func Int8Minus(xs, ys, rs []int8) []int8 {
	return int8Minus(xs, ys, rs)
}

func int8MinusPure(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int8MinusAvx2(xs, ys, rs []int8) []int8 {
    const regItems = 32 / 1
	n := len(xs) / regItems
	int8MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func int8MinusAvx512(xs, ys, rs []int8) []int8 {
    const regItems = 64 / 1
	n := len(xs) / regItems
	int8MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Int8MinusSels(xs, ys, rs []int8, sels []int64) []int8 {
	return int8MinusSels(xs, ys, rs, sels)
}

func int8MinusSelsPure(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func int8MinusSelsAvx2(xs, ys, rs []int8, sels []int64) []int8 {
//    const regItems = 32 / 1
//	n := len(sels) / regItems
//	int8MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func int8MinusSelsAvx512(xs, ys, rs []int8, sels []int64) []int8 {
//    const regItems = 64 / 1
//	n := len(sels) / regItems
//	int8MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Int8MinusScalar(x int8, ys, rs []int8) []int8 {
	return int8MinusScalar(x, ys, rs)
}

func int8MinusScalarPure(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int8MinusScalarAvx2(x int8, ys, rs []int8) []int8 {
    const regItems = 32 / 1
	n := len(ys) / regItems
	int8MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func int8MinusScalarAvx512(x int8, ys, rs []int8) []int8 {
    const regItems = 64 / 1
	n := len(ys) / regItems
	int8MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Int8MinusScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	return int8MinusScalarSels(x, ys, rs, sels)
}

func int8MinusScalarSelsPure(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func int8MinusScalarSelsAvx2(x int8, ys, rs []int8, sels []int64) []int8 {
//    const regItems = 32 / 1
//	n := len(sels) / regItems
//	int8MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func int8MinusScalarSelsAvx512(x int8, ys, rs []int8, sels []int64) []int8 {
//    const regItems = 64 / 1
//	n := len(sels) / regItems
//	int8MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Int8MinusByScalar(x int8, ys, rs []int8) []int8 {
	return int8MinusByScalar(x, ys, rs)
}

func int8MinusByScalarPure(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int8MinusByScalarAvx2(x int8, ys, rs []int8) []int8 {
    const regItems = 32 / 1
	n := len(ys) / regItems
	int8MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func int8MinusByScalarAvx512(x int8, ys, rs []int8) []int8 {
    const regItems = 64 / 1
	n := len(ys) / regItems
	int8MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Int8MinusByScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	return int8MinusByScalarSels(x, ys, rs, sels)
}

func int8MinusByScalarSelsPure(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func int8MinusByScalarSelsAvx2(x int8, ys, rs []int8, sels []int64) []int8 {
//    const regItems = 32 / 1
//	n := len(sels) / regItems
//	int8MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func int8MinusByScalarSelsAvx512(x int8, ys, rs []int8, sels []int64) []int8 {
//    const regItems = 64 / 1
//	n := len(sels) / regItems
//	int8MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

func Int16Minus(xs, ys, rs []int16) []int16 {
	return int16Minus(xs, ys, rs)
}

func int16MinusPure(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int16MinusAvx2(xs, ys, rs []int16) []int16 {
    const regItems = 32 / 2
	n := len(xs) / regItems
	int16MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func int16MinusAvx512(xs, ys, rs []int16) []int16 {
    const regItems = 64 / 2
	n := len(xs) / regItems
	int16MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Int16MinusSels(xs, ys, rs []int16, sels []int64) []int16 {
	return int16MinusSels(xs, ys, rs, sels)
}

func int16MinusSelsPure(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func int16MinusSelsAvx2(xs, ys, rs []int16, sels []int64) []int16 {
//    const regItems = 32 / 2
//	n := len(sels) / regItems
//	int16MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func int16MinusSelsAvx512(xs, ys, rs []int16, sels []int64) []int16 {
//    const regItems = 64 / 2
//	n := len(sels) / regItems
//	int16MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Int16MinusScalar(x int16, ys, rs []int16) []int16 {
	return int16MinusScalar(x, ys, rs)
}

func int16MinusScalarPure(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int16MinusScalarAvx2(x int16, ys, rs []int16) []int16 {
    const regItems = 32 / 2
	n := len(ys) / regItems
	int16MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func int16MinusScalarAvx512(x int16, ys, rs []int16) []int16 {
    const regItems = 64 / 2
	n := len(ys) / regItems
	int16MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Int16MinusScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	return int16MinusScalarSels(x, ys, rs, sels)
}

func int16MinusScalarSelsPure(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func int16MinusScalarSelsAvx2(x int16, ys, rs []int16, sels []int64) []int16 {
//    const regItems = 32 / 2
//	n := len(sels) / regItems
//	int16MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func int16MinusScalarSelsAvx512(x int16, ys, rs []int16, sels []int64) []int16 {
//    const regItems = 64 / 2
//	n := len(sels) / regItems
//	int16MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Int16MinusByScalar(x int16, ys, rs []int16) []int16 {
	return int16MinusByScalar(x, ys, rs)
}

func int16MinusByScalarPure(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int16MinusByScalarAvx2(x int16, ys, rs []int16) []int16 {
    const regItems = 32 / 2
	n := len(ys) / regItems
	int16MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func int16MinusByScalarAvx512(x int16, ys, rs []int16) []int16 {
    const regItems = 64 / 2
	n := len(ys) / regItems
	int16MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Int16MinusByScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	return int16MinusByScalarSels(x, ys, rs, sels)
}

func int16MinusByScalarSelsPure(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func int16MinusByScalarSelsAvx2(x int16, ys, rs []int16, sels []int64) []int16 {
//    const regItems = 32 / 2
//	n := len(sels) / regItems
//	int16MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func int16MinusByScalarSelsAvx512(x int16, ys, rs []int16, sels []int64) []int16 {
//    const regItems = 64 / 2
//	n := len(sels) / regItems
//	int16MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

func Int32Minus(xs, ys, rs []int32) []int32 {
	return int32Minus(xs, ys, rs)
}

func int32MinusPure(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int32MinusAvx2(xs, ys, rs []int32) []int32 {
    const regItems = 32 / 4
	n := len(xs) / regItems
	int32MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func int32MinusAvx512(xs, ys, rs []int32) []int32 {
    const regItems = 64 / 4
	n := len(xs) / regItems
	int32MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Int32MinusSels(xs, ys, rs []int32, sels []int64) []int32 {
	return int32MinusSels(xs, ys, rs, sels)
}

func int32MinusSelsPure(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func int32MinusSelsAvx2(xs, ys, rs []int32, sels []int64) []int32 {
//    const regItems = 32 / 4
//	n := len(sels) / regItems
//	int32MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func int32MinusSelsAvx512(xs, ys, rs []int32, sels []int64) []int32 {
//    const regItems = 64 / 4
//	n := len(sels) / regItems
//	int32MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Int32MinusScalar(x int32, ys, rs []int32) []int32 {
	return int32MinusScalar(x, ys, rs)
}

func int32MinusScalarPure(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int32MinusScalarAvx2(x int32, ys, rs []int32) []int32 {
    const regItems = 32 / 4
	n := len(ys) / regItems
	int32MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func int32MinusScalarAvx512(x int32, ys, rs []int32) []int32 {
    const regItems = 64 / 4
	n := len(ys) / regItems
	int32MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Int32MinusScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	return int32MinusScalarSels(x, ys, rs, sels)
}

func int32MinusScalarSelsPure(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func int32MinusScalarSelsAvx2(x int32, ys, rs []int32, sels []int64) []int32 {
//    const regItems = 32 / 4
//	n := len(sels) / regItems
//	int32MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func int32MinusScalarSelsAvx512(x int32, ys, rs []int32, sels []int64) []int32 {
//    const regItems = 64 / 4
//	n := len(sels) / regItems
//	int32MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Int32MinusByScalar(x int32, ys, rs []int32) []int32 {
	return int32MinusByScalar(x, ys, rs)
}

func int32MinusByScalarPure(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int32MinusByScalarAvx2(x int32, ys, rs []int32) []int32 {
    const regItems = 32 / 4
	n := len(ys) / regItems
	int32MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func int32MinusByScalarAvx512(x int32, ys, rs []int32) []int32 {
    const regItems = 64 / 4
	n := len(ys) / regItems
	int32MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Int32MinusByScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	return int32MinusByScalarSels(x, ys, rs, sels)
}

func int32MinusByScalarSelsPure(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func int32MinusByScalarSelsAvx2(x int32, ys, rs []int32, sels []int64) []int32 {
//    const regItems = 32 / 4
//	n := len(sels) / regItems
//	int32MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func int32MinusByScalarSelsAvx512(x int32, ys, rs []int32, sels []int64) []int32 {
//    const regItems = 64 / 4
//	n := len(sels) / regItems
//	int32MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

func Int64Minus(xs, ys, rs []int64) []int64 {
	return int64Minus(xs, ys, rs)
}

func int64MinusPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int64MinusAvx2(xs, ys, rs []int64) []int64 {
    const regItems = 32 / 8
	n := len(xs) / regItems
	int64MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func int64MinusAvx512(xs, ys, rs []int64) []int64 {
    const regItems = 64 / 8
	n := len(xs) / regItems
	int64MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Int64MinusSels(xs, ys, rs []int64, sels []int64) []int64 {
	return int64MinusSels(xs, ys, rs, sels)
}

func int64MinusSelsPure(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func int64MinusSelsAvx2(xs, ys, rs []int64, sels []int64) []int64 {
//    const regItems = 32 / 8
//	n := len(sels) / regItems
//	int64MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func int64MinusSelsAvx512(xs, ys, rs []int64, sels []int64) []int64 {
//    const regItems = 64 / 8
//	n := len(sels) / regItems
//	int64MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Int64MinusScalar(x int64, ys, rs []int64) []int64 {
	return int64MinusScalar(x, ys, rs)
}

func int64MinusScalarPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int64MinusScalarAvx2(x int64, ys, rs []int64) []int64 {
    const regItems = 32 / 8
	n := len(ys) / regItems
	int64MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func int64MinusScalarAvx512(x int64, ys, rs []int64) []int64 {
    const regItems = 64 / 8
	n := len(ys) / regItems
	int64MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Int64MinusScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return int64MinusScalarSels(x, ys, rs, sels)
}

func int64MinusScalarSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func int64MinusScalarSelsAvx2(x int64, ys, rs []int64, sels []int64) []int64 {
//    const regItems = 32 / 8
//	n := len(sels) / regItems
//	int64MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func int64MinusScalarSelsAvx512(x int64, ys, rs []int64, sels []int64) []int64 {
//    const regItems = 64 / 8
//	n := len(sels) / regItems
//	int64MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Int64MinusByScalar(x int64, ys, rs []int64) []int64 {
	return int64MinusByScalar(x, ys, rs)
}

func int64MinusByScalarPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int64MinusByScalarAvx2(x int64, ys, rs []int64) []int64 {
    const regItems = 32 / 8
	n := len(ys) / regItems
	int64MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func int64MinusByScalarAvx512(x int64, ys, rs []int64) []int64 {
    const regItems = 64 / 8
	n := len(ys) / regItems
	int64MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Int64MinusByScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return int64MinusByScalarSels(x, ys, rs, sels)
}

func int64MinusByScalarSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func int64MinusByScalarSelsAvx2(x int64, ys, rs []int64, sels []int64) []int64 {
//    const regItems = 32 / 8
//	n := len(sels) / regItems
//	int64MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func int64MinusByScalarSelsAvx512(x int64, ys, rs []int64, sels []int64) []int64 {
//    const regItems = 64 / 8
//	n := len(sels) / regItems
//	int64MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

func Uint8Minus(xs, ys, rs []uint8) []uint8 {
	return uint8Minus(xs, ys, rs)
}

func uint8MinusPure(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint8MinusAvx2(xs, ys, rs []uint8) []uint8 {
    const regItems = 32 / 1
	n := len(xs) / regItems
	uint8MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func uint8MinusAvx512(xs, ys, rs []uint8) []uint8 {
    const regItems = 64 / 1
	n := len(xs) / regItems
	uint8MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Uint8MinusSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	return uint8MinusSels(xs, ys, rs, sels)
}

func uint8MinusSelsPure(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func uint8MinusSelsAvx2(xs, ys, rs []uint8, sels []int64) []uint8 {
//    const regItems = 32 / 1
//	n := len(sels) / regItems
//	uint8MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func uint8MinusSelsAvx512(xs, ys, rs []uint8, sels []int64) []uint8 {
//    const regItems = 64 / 1
//	n := len(sels) / regItems
//	uint8MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Uint8MinusScalar(x uint8, ys, rs []uint8) []uint8 {
	return uint8MinusScalar(x, ys, rs)
}

func uint8MinusScalarPure(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint8MinusScalarAvx2(x uint8, ys, rs []uint8) []uint8 {
    const regItems = 32 / 1
	n := len(ys) / regItems
	uint8MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint8MinusScalarAvx512(x uint8, ys, rs []uint8) []uint8 {
    const regItems = 64 / 1
	n := len(ys) / regItems
	uint8MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Uint8MinusScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	return uint8MinusScalarSels(x, ys, rs, sels)
}

func uint8MinusScalarSelsPure(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func uint8MinusScalarSelsAvx2(x uint8, ys, rs []uint8, sels []int64) []uint8 {
//    const regItems = 32 / 1
//	n := len(sels) / regItems
//	uint8MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func uint8MinusScalarSelsAvx512(x uint8, ys, rs []uint8, sels []int64) []uint8 {
//    const regItems = 64 / 1
//	n := len(sels) / regItems
//	uint8MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Uint8MinusByScalar(x uint8, ys, rs []uint8) []uint8 {
	return uint8MinusByScalar(x, ys, rs)
}

func uint8MinusByScalarPure(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint8MinusByScalarAvx2(x uint8, ys, rs []uint8) []uint8 {
    const regItems = 32 / 1
	n := len(ys) / regItems
	uint8MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func uint8MinusByScalarAvx512(x uint8, ys, rs []uint8) []uint8 {
    const regItems = 64 / 1
	n := len(ys) / regItems
	uint8MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Uint8MinusByScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	return uint8MinusByScalarSels(x, ys, rs, sels)
}

func uint8MinusByScalarSelsPure(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func uint8MinusByScalarSelsAvx2(x uint8, ys, rs []uint8, sels []int64) []uint8 {
//    const regItems = 32 / 1
//	n := len(sels) / regItems
//	uint8MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func uint8MinusByScalarSelsAvx512(x uint8, ys, rs []uint8, sels []int64) []uint8 {
//    const regItems = 64 / 1
//	n := len(sels) / regItems
//	uint8MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

func Uint16Minus(xs, ys, rs []uint16) []uint16 {
	return uint16Minus(xs, ys, rs)
}

func uint16MinusPure(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint16MinusAvx2(xs, ys, rs []uint16) []uint16 {
    const regItems = 32 / 2
	n := len(xs) / regItems
	uint16MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func uint16MinusAvx512(xs, ys, rs []uint16) []uint16 {
    const regItems = 64 / 2
	n := len(xs) / regItems
	uint16MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Uint16MinusSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	return uint16MinusSels(xs, ys, rs, sels)
}

func uint16MinusSelsPure(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func uint16MinusSelsAvx2(xs, ys, rs []uint16, sels []int64) []uint16 {
//    const regItems = 32 / 2
//	n := len(sels) / regItems
//	uint16MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func uint16MinusSelsAvx512(xs, ys, rs []uint16, sels []int64) []uint16 {
//    const regItems = 64 / 2
//	n := len(sels) / regItems
//	uint16MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Uint16MinusScalar(x uint16, ys, rs []uint16) []uint16 {
	return uint16MinusScalar(x, ys, rs)
}

func uint16MinusScalarPure(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint16MinusScalarAvx2(x uint16, ys, rs []uint16) []uint16 {
    const regItems = 32 / 2
	n := len(ys) / regItems
	uint16MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint16MinusScalarAvx512(x uint16, ys, rs []uint16) []uint16 {
    const regItems = 64 / 2
	n := len(ys) / regItems
	uint16MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Uint16MinusScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	return uint16MinusScalarSels(x, ys, rs, sels)
}

func uint16MinusScalarSelsPure(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func uint16MinusScalarSelsAvx2(x uint16, ys, rs []uint16, sels []int64) []uint16 {
//    const regItems = 32 / 2
//	n := len(sels) / regItems
//	uint16MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func uint16MinusScalarSelsAvx512(x uint16, ys, rs []uint16, sels []int64) []uint16 {
//    const regItems = 64 / 2
//	n := len(sels) / regItems
//	uint16MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Uint16MinusByScalar(x uint16, ys, rs []uint16) []uint16 {
	return uint16MinusByScalar(x, ys, rs)
}

func uint16MinusByScalarPure(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint16MinusByScalarAvx2(x uint16, ys, rs []uint16) []uint16 {
    const regItems = 32 / 2
	n := len(ys) / regItems
	uint16MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func uint16MinusByScalarAvx512(x uint16, ys, rs []uint16) []uint16 {
    const regItems = 64 / 2
	n := len(ys) / regItems
	uint16MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Uint16MinusByScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	return uint16MinusByScalarSels(x, ys, rs, sels)
}

func uint16MinusByScalarSelsPure(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func uint16MinusByScalarSelsAvx2(x uint16, ys, rs []uint16, sels []int64) []uint16 {
//    const regItems = 32 / 2
//	n := len(sels) / regItems
//	uint16MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func uint16MinusByScalarSelsAvx512(x uint16, ys, rs []uint16, sels []int64) []uint16 {
//    const regItems = 64 / 2
//	n := len(sels) / regItems
//	uint16MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

func Uint32Minus(xs, ys, rs []uint32) []uint32 {
	return uint32Minus(xs, ys, rs)
}

func uint32MinusPure(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint32MinusAvx2(xs, ys, rs []uint32) []uint32 {
    const regItems = 32 / 4
	n := len(xs) / regItems
	uint32MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func uint32MinusAvx512(xs, ys, rs []uint32) []uint32 {
    const regItems = 64 / 4
	n := len(xs) / regItems
	uint32MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Uint32MinusSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	return uint32MinusSels(xs, ys, rs, sels)
}

func uint32MinusSelsPure(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func uint32MinusSelsAvx2(xs, ys, rs []uint32, sels []int64) []uint32 {
//    const regItems = 32 / 4
//	n := len(sels) / regItems
//	uint32MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func uint32MinusSelsAvx512(xs, ys, rs []uint32, sels []int64) []uint32 {
//    const regItems = 64 / 4
//	n := len(sels) / regItems
//	uint32MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Uint32MinusScalar(x uint32, ys, rs []uint32) []uint32 {
	return uint32MinusScalar(x, ys, rs)
}

func uint32MinusScalarPure(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint32MinusScalarAvx2(x uint32, ys, rs []uint32) []uint32 {
    const regItems = 32 / 4
	n := len(ys) / regItems
	uint32MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint32MinusScalarAvx512(x uint32, ys, rs []uint32) []uint32 {
    const regItems = 64 / 4
	n := len(ys) / regItems
	uint32MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Uint32MinusScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	return uint32MinusScalarSels(x, ys, rs, sels)
}

func uint32MinusScalarSelsPure(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func uint32MinusScalarSelsAvx2(x uint32, ys, rs []uint32, sels []int64) []uint32 {
//    const regItems = 32 / 4
//	n := len(sels) / regItems
//	uint32MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func uint32MinusScalarSelsAvx512(x uint32, ys, rs []uint32, sels []int64) []uint32 {
//    const regItems = 64 / 4
//	n := len(sels) / regItems
//	uint32MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Uint32MinusByScalar(x uint32, ys, rs []uint32) []uint32 {
	return uint32MinusByScalar(x, ys, rs)
}

func uint32MinusByScalarPure(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint32MinusByScalarAvx2(x uint32, ys, rs []uint32) []uint32 {
    const regItems = 32 / 4
	n := len(ys) / regItems
	uint32MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func uint32MinusByScalarAvx512(x uint32, ys, rs []uint32) []uint32 {
    const regItems = 64 / 4
	n := len(ys) / regItems
	uint32MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Uint32MinusByScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	return uint32MinusByScalarSels(x, ys, rs, sels)
}

func uint32MinusByScalarSelsPure(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func uint32MinusByScalarSelsAvx2(x uint32, ys, rs []uint32, sels []int64) []uint32 {
//    const regItems = 32 / 4
//	n := len(sels) / regItems
//	uint32MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func uint32MinusByScalarSelsAvx512(x uint32, ys, rs []uint32, sels []int64) []uint32 {
//    const regItems = 64 / 4
//	n := len(sels) / regItems
//	uint32MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

func Uint64Minus(xs, ys, rs []uint64) []uint64 {
	return uint64Minus(xs, ys, rs)
}

func uint64MinusPure(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint64MinusAvx2(xs, ys, rs []uint64) []uint64 {
    const regItems = 32 / 8
	n := len(xs) / regItems
	uint64MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func uint64MinusAvx512(xs, ys, rs []uint64) []uint64 {
    const regItems = 64 / 8
	n := len(xs) / regItems
	uint64MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Uint64MinusSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	return uint64MinusSels(xs, ys, rs, sels)
}

func uint64MinusSelsPure(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func uint64MinusSelsAvx2(xs, ys, rs []uint64, sels []int64) []uint64 {
//    const regItems = 32 / 8
//	n := len(sels) / regItems
//	uint64MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func uint64MinusSelsAvx512(xs, ys, rs []uint64, sels []int64) []uint64 {
//    const regItems = 64 / 8
//	n := len(sels) / regItems
//	uint64MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Uint64MinusScalar(x uint64, ys, rs []uint64) []uint64 {
	return uint64MinusScalar(x, ys, rs)
}

func uint64MinusScalarPure(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint64MinusScalarAvx2(x uint64, ys, rs []uint64) []uint64 {
    const regItems = 32 / 8
	n := len(ys) / regItems
	uint64MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint64MinusScalarAvx512(x uint64, ys, rs []uint64) []uint64 {
    const regItems = 64 / 8
	n := len(ys) / regItems
	uint64MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Uint64MinusScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	return uint64MinusScalarSels(x, ys, rs, sels)
}

func uint64MinusScalarSelsPure(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func uint64MinusScalarSelsAvx2(x uint64, ys, rs []uint64, sels []int64) []uint64 {
//    const regItems = 32 / 8
//	n := len(sels) / regItems
//	uint64MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func uint64MinusScalarSelsAvx512(x uint64, ys, rs []uint64, sels []int64) []uint64 {
//    const regItems = 64 / 8
//	n := len(sels) / regItems
//	uint64MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Uint64MinusByScalar(x uint64, ys, rs []uint64) []uint64 {
	return uint64MinusByScalar(x, ys, rs)
}

func uint64MinusByScalarPure(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint64MinusByScalarAvx2(x uint64, ys, rs []uint64) []uint64 {
    const regItems = 32 / 8
	n := len(ys) / regItems
	uint64MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func uint64MinusByScalarAvx512(x uint64, ys, rs []uint64) []uint64 {
    const regItems = 64 / 8
	n := len(ys) / regItems
	uint64MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Uint64MinusByScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	return uint64MinusByScalarSels(x, ys, rs, sels)
}

func uint64MinusByScalarSelsPure(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func uint64MinusByScalarSelsAvx2(x uint64, ys, rs []uint64, sels []int64) []uint64 {
//    const regItems = 32 / 8
//	n := len(sels) / regItems
//	uint64MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func uint64MinusByScalarSelsAvx512(x uint64, ys, rs []uint64, sels []int64) []uint64 {
//    const regItems = 64 / 8
//	n := len(sels) / regItems
//	uint64MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

func Float32Minus(xs, ys, rs []float32) []float32 {
	return float32Minus(xs, ys, rs)
}

func float32MinusPure(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func float32MinusAvx2(xs, ys, rs []float32) []float32 {
    const regItems = 32 / 4
	n := len(xs) / regItems
	float32MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func float32MinusAvx512(xs, ys, rs []float32) []float32 {
    const regItems = 64 / 4
	n := len(xs) / regItems
	float32MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Float32MinusSels(xs, ys, rs []float32, sels []int64) []float32 {
	return float32MinusSels(xs, ys, rs, sels)
}

func float32MinusSelsPure(xs, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func float32MinusSelsAvx2(xs, ys, rs []float32, sels []int64) []float32 {
//    const regItems = 32 / 4
//	n := len(sels) / regItems
//	float32MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func float32MinusSelsAvx512(xs, ys, rs []float32, sels []int64) []float32 {
//    const regItems = 64 / 4
//	n := len(sels) / regItems
//	float32MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Float32MinusScalar(x float32, ys, rs []float32) []float32 {
	return float32MinusScalar(x, ys, rs)
}

func float32MinusScalarPure(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func float32MinusScalarAvx2(x float32, ys, rs []float32) []float32 {
    const regItems = 32 / 4
	n := len(ys) / regItems
	float32MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func float32MinusScalarAvx512(x float32, ys, rs []float32) []float32 {
    const regItems = 64 / 4
	n := len(ys) / regItems
	float32MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Float32MinusScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	return float32MinusScalarSels(x, ys, rs, sels)
}

func float32MinusScalarSelsPure(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func float32MinusScalarSelsAvx2(x float32, ys, rs []float32, sels []int64) []float32 {
//    const regItems = 32 / 4
//	n := len(sels) / regItems
//	float32MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func float32MinusScalarSelsAvx512(x float32, ys, rs []float32, sels []int64) []float32 {
//    const regItems = 64 / 4
//	n := len(sels) / regItems
//	float32MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Float32MinusByScalar(x float32, ys, rs []float32) []float32 {
	return float32MinusByScalar(x, ys, rs)
}

func float32MinusByScalarPure(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func float32MinusByScalarAvx2(x float32, ys, rs []float32) []float32 {
    const regItems = 32 / 4
	n := len(ys) / regItems
	float32MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func float32MinusByScalarAvx512(x float32, ys, rs []float32) []float32 {
    const regItems = 64 / 4
	n := len(ys) / regItems
	float32MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Float32MinusByScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	return float32MinusByScalarSels(x, ys, rs, sels)
}

func float32MinusByScalarSelsPure(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func float32MinusByScalarSelsAvx2(x float32, ys, rs []float32, sels []int64) []float32 {
//    const regItems = 32 / 4
//	n := len(sels) / regItems
//	float32MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func float32MinusByScalarSelsAvx512(x float32, ys, rs []float32, sels []int64) []float32 {
//    const regItems = 64 / 4
//	n := len(sels) / regItems
//	float32MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

func Float64Minus(xs, ys, rs []float64) []float64 {
	return float64Minus(xs, ys, rs)
}

func float64MinusPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func float64MinusAvx2(xs, ys, rs []float64) []float64 {
    const regItems = 32 / 8
	n := len(xs) / regItems
	float64MinusAvx2Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func float64MinusAvx512(xs, ys, rs []float64) []float64 {
    const regItems = 64 / 8
	n := len(xs) / regItems
	float64MinusAvx512Asm(xs[:n*regItems], ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Float64MinusSels(xs, ys, rs []float64, sels []int64) []float64 {
	return float64MinusSels(xs, ys, rs, sels)
}

func float64MinusSelsPure(xs, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

//func float64MinusSelsAvx2(xs, ys, rs []float64, sels []int64) []float64 {
//    const regItems = 32 / 8
//	n := len(sels) / regItems
//	float64MinusSelsAvx2Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

//func float64MinusSelsAvx512(xs, ys, rs []float64, sels []int64) []float64 {
//    const regItems = 64 / 8
//	n := len(sels) / regItems
//	float64MinusSelsAvx512Asm(xs, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = xs[sels[i]] - ys[sels[i]]
//	}
//	return rs
//}

func Float64MinusScalar(x float64, ys, rs []float64) []float64 {
	return float64MinusScalar(x, ys, rs)
}

func float64MinusScalarPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func float64MinusScalarAvx2(x float64, ys, rs []float64) []float64 {
    const regItems = 32 / 8
	n := len(ys) / regItems
	float64MinusScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func float64MinusScalarAvx512(x float64, ys, rs []float64) []float64 {
    const regItems = 64 / 8
	n := len(ys) / regItems
	float64MinusScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Float64MinusScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return float64MinusScalarSels(x, ys, rs, sels)
}

func float64MinusScalarSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

//func float64MinusScalarSelsAvx2(x float64, ys, rs []float64, sels []int64) []float64 {
//    const regItems = 32 / 8
//	n := len(sels) / regItems
//	float64MinusScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

//func float64MinusScalarSelsAvx512(x float64, ys, rs []float64, sels []int64) []float64 {
//    const regItems = 64 / 8
//	n := len(sels) / regItems
//	float64MinusScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = x - ys[sels[i]]
//	}
//	return rs
//}

func Float64MinusByScalar(x float64, ys, rs []float64) []float64 {
	return float64MinusByScalar(x, ys, rs)
}

func float64MinusByScalarPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func float64MinusByScalarAvx2(x float64, ys, rs []float64) []float64 {
    const regItems = 32 / 8
	n := len(ys) / regItems
	float64MinusByScalarAvx2Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func float64MinusByScalarAvx512(x float64, ys, rs []float64) []float64 {
    const regItems = 64 / 8
	n := len(ys) / regItems
	float64MinusByScalarAvx512Asm(x, ys[:n*regItems], rs[:n*regItems])
	for i, j := n * regItems, len(xs); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Float64MinusByScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return float64MinusByScalarSels(x, ys, rs, sels)
}

func float64MinusByScalarSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

//func float64MinusByScalarSelsAvx2(x float64, ys, rs []float64, sels []int64) []float64 {
//    const regItems = 32 / 8
//	n := len(sels) / regItems
//	float64MinusByScalarSelsAvx2Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}

//func float64MinusByScalarSelsAvx512(x float64, ys, rs []float64, sels []int64) []float64 {
//    const regItems = 64 / 8
//	n := len(sels) / regItems
//	float64MinusByScalarSelsAvx512Asm(x, ys, rs, sels[:n*regItems])
//	for i, j := n * regItems, len(sels); i < j; i++ {
//		rs[i] = ys[sels[i]] - x
//	}
//	return rs
//}
