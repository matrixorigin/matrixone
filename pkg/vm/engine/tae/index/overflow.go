package index

type signed interface {
	int8 | int16 | int32 | int64
}

type unsigned interface {
	uint8 | uint16 | uint32 | uint64
}

type float interface {
	float32 | float64
}

// addi adds two integers of the same type and returns the result and whether
// the result overflowed.
func addi[T signed | unsigned](a, b T, overflow func(int64) bool) (int64, bool) {
	s := int64(a) + int64(b)
	if overflow(s) {
		return s, true
	}
	return s, false
}

// addf adds two floats of the same type and returns the result and whether
// the result overflowed.
func addf[T float](a, b T, overflow func(float64) bool) (float64, bool) {
	s := float64(a) + float64(b)
	if overflow(s) {
		return s, true
	}
	return s, false
}
