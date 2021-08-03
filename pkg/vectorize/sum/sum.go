package sum

var (
	Int8Sum        func([]int8) int64
	Int8SumSels    func([]int8, []int64) int64
	Int16Sum       func([]int16) int64
	Int16SumSels   func([]int16, []int64) int64
	Int32Sum       func([]int32) int64
	Int32SumSels   func([]int32, []int64) int64
	Int64Sum       func([]int64) int64
	Int64SumSels   func([]int64, []int64) int64
	Uint8Sum       func([]uint8) uint64
	Uint8SumSels   func([]uint8, []int64) uint64
	Uint16Sum      func([]uint16) uint64
	Uint16SumSels  func([]uint16, []int64) uint64
	Uint32Sum      func([]uint32) uint64
	Uint32SumSels  func([]uint32, []int64) uint64
	Uint64Sum      func([]uint64) uint64
	Uint64SumSels  func([]uint64, []int64) uint64
	Float32Sum     func([]float32) float32
	Float32SumSels func([]float32, []int64) float32
	Float64Sum     func([]float64) float64
	Float64SumSels func([]float64, []int64) float64
)

func int8Sum(xs []int8) int64 {
	var res int64

	for _, x := range xs {
		res += int64(x)
	}
	return res
}

func int8SumSels(xs []int8, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += int64(xs[sel])
	}
	return res
}

func int16Sum(xs []int16) int64 {
	var res int64

	for _, x := range xs {
		res += int64(x)
	}
	return res
}

func int16SumSels(xs []int16, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += int64(xs[sel])
	}
	return res
}

func int32Sum(xs []int32) int64 {
	var res int64

	for _, x := range xs {
		res += int64(x)
	}
	return res
}

func int32SumSels(xs []int32, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += int64(xs[sel])
	}
	return res
}

func int64Sum(xs []int64) int64 {
	var res int64

	for _, x := range xs {
		res += x
	}
	return res
}

func int64SumSels(xs []int64, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func uint8Sum(xs []uint8) uint64 {
	var res uint64

	for _, x := range xs {
		res += uint64(x)
	}
	return res
}

func uint8SumSels(xs []uint8, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += uint64(xs[sel])
	}
	return res
}

func uint16Sum(xs []uint16) uint64 {
	var res uint64

	for _, x := range xs {
		res += uint64(x)
	}
	return res
}

func uint16SumSels(xs []uint16, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += uint64(xs[sel])
	}
	return res
}

func uint32Sum(xs []uint32) uint64 {
	var res uint64

	for _, x := range xs {
		res += uint64(x)
	}
	return res
}

func uint32SumSels(xs []uint32, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += uint64(xs[sel])
	}
	return res
}

func uint64Sum(xs []uint64) uint64 {
	var res uint64

	for _, x := range xs {
		res += x
	}
	return res
}

func uint64SumSels(xs []uint64, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func float32Sum(xs []float32) float32 {
	var res float32

	for _, x := range xs {
		res += x
	}
	return res
}

func float32SumSels(xs []float32, sels []int64) float32 {
	var res float32

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func float64Sum(xs []float64) float64 {
	var res float64

	for _, x := range xs {
		res += x
	}
	return res
}

func float64SumSels(xs []float64, sels []int64) float64 {
	var res float64

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}
