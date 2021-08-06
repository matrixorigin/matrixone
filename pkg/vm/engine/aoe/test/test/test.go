package main

import (
	"bytes"
	"fmt"
)

func main() {
	a := []byte("/DeletedTableQueue/3/1/1")
	b := []byte("/DeletedTableQueue/10")

	println(bytes.Compare(a, b))
	println(bytes.Compare([]byte(fmt.Sprintf("%d", 3)), []byte(fmt.Sprintf("%d", 10))))
}
