package fastmap

import (
	"fmt"
	"testing"
)

func TestMap(t *testing.T) {
	m := New()
	for i := 0; i < 100; i++ {
		m.Set(uint64(i), i)
	}
	for i := 0; i < 100; i++ {
		v, ok := m.Get(uint64(i))
		fmt.Printf("%v: %v, %v\n", i, v, ok)
	}
}
