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

// add adds two numbers of the same type and returns the result and whether
// the result overflowed.
func add[T signed | unsigned | float](a, b T, overflow func(T) bool) (T, bool) {
	s := a + b
	if overflow(s) {
		return s, true
	}
	return s, false
}
