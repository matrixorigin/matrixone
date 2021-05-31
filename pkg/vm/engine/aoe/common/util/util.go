package util

const (
	UINT64_MAX = ^uint64(0)
)

func MemsetRepeatByte(a []byte, v byte) {
	if len(a) == 0 {
		return
	}
	a[0] = v
	for bp := 1; bp < len(a); bp *= 2 {
		copy(a[bp:], a[:bp])
	}
}
