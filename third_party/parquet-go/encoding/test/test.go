package test

import (
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

func EncodeInt32(t *testing.T, enc encoding.Encoding, min, max int, bitWidth uint) {
	t.Helper()
	encode(t, enc, min, max,
		encoding.Encoding.EncodeInt32,
		encoding.Encoding.DecodeInt32,
		func(i int) int32 {
			value := int32(i)
			mask := int32((1 << bitWidth) - 1)
			if (i % 2) != 0 {
				value = -value
			}
			return value & mask
		},
	)
}

func EncodeInt64(t *testing.T, enc encoding.Encoding, min, max int, bitWidth uint) {
	t.Helper()
	encode(t, enc, min, max,
		encoding.Encoding.EncodeInt64,
		encoding.Encoding.DecodeInt64,
		func(i int) int64 {
			value := int64(i)
			mask := int64((1 << bitWidth) - 1)
			if (i % 2) != 0 {
				value = -value
			}
			return value & mask
		},
	)
}

func EncodeFloat(t *testing.T, enc encoding.Encoding, min, max int) {
	t.Helper()
	encode(t, enc, min, max,
		encoding.Encoding.EncodeFloat,
		encoding.Encoding.DecodeFloat,
		func(i int) float32 { return float32(i) },
	)
}

func EncodeDouble(t *testing.T, enc encoding.Encoding, min, max int) {
	t.Helper()
	encode(t, enc, min, max,
		encoding.Encoding.EncodeDouble,
		encoding.Encoding.DecodeDouble,
		func(i int) float64 { return float64(i) },
	)
}

type encodingFunc[T comparable] func(encoding.Encoding, []byte, []T) ([]byte, error)

type decodingFunc[T comparable] func(encoding.Encoding, []T, []byte) ([]T, error)

func encode[T comparable](t *testing.T, enc encoding.Encoding, min, max int, encode encodingFunc[T], decode decodingFunc[T], valueOf func(int) T) {
	t.Helper()

	for k := min; k <= max; k++ {
		t.Run(fmt.Sprintf("N=%d", k), func(t *testing.T) {
			src := make([]T, k)
			for i := range src {
				src[i] = valueOf(i)
			}

			buf, err := encode(enc, nil, src)
			if err != nil {
				t.Fatalf("encoding %d values: %v", k, err)
			}

			res, err := decode(enc, nil, buf)
			if err != nil {
				t.Fatalf("decoding %d values: %v", k, err)
			}

			if err := assertEqual(src, res); err != nil {
				t.Fatalf("testing %d values: %v", k, err)
			}
		})
	}
}

func assertEqual[T comparable](want, got []T) error {
	if len(want) != len(got) {
		return fmt.Errorf("number of values mismatch: want=%d got=%d", len(want), len(got))
	}

	for i := range want {
		if want[i] != got[i] {
			return fmt.Errorf("values at index %d/%d mismatch: want=%+v got=%+v", i, len(want), want[i], got[i])
		}
	}

	return nil
}
