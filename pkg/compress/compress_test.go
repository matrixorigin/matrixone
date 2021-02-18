package compress

import (
	"fmt"
	"log"
	"matrixbase/pkg/encoding"
	"testing"

	"github.com/pierrec/lz4"
)

func TestLz4(t *testing.T) {
	var err error

	xs := []int64{200, 200, 0, 200, 10, 30, 20, 1111}
	raw := encoding.EncodeInt64Slice(xs)
	fmt.Printf("raw: %v\n", raw)
	buf := make([]byte, lz4.CompressBlockBound(len(raw)))
	if buf, err = Compress(raw, buf, Lz4); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("buf: %v\n", buf)
	data := make([]byte, len(buf)*10)
	if data, err = Decompress(buf, raw, Lz4); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("dat: %v\n", data)
}
