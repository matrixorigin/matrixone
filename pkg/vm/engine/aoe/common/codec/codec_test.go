package codec

import (
	"bytes"
	"fmt"
	"testing"
)

func TestFormat(t *testing.T) {
	{
		data := EncodeKey(1, 3, "x", []byte("g"))
		fmt.Printf("data: %v\n", data)
	}
	{
		data := Encode(1, 3, "x", []byte("g"), uint8(0), uint64(33))
		fmt.Printf("data: %v\n", data)
		vs := Decode(data)
		fmt.Printf("vs: %v\n", vs)
	}
	{
		x, y := Encode(1, "x", 10), Encode(1, "x", 2)
		fmt.Printf("x: %v, y: %v - :%v\n", x, y, bytes.Compare(x, y))
	}
}
