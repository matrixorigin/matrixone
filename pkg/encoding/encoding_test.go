package encoding

import (
	"fmt"
	"testing"
)

func TestEncoding(t *testing.T) {
	xs := []string{"a", "bc", "d"}
	data := EncodeStringSlice(xs)
	ys := DecodeStringSlice(data)
	fmt.Printf("ys: %v\n", ys)
}
