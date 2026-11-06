// Package fuzz contains functions to help fuzz test parquet encodings.
package fuzz

import (
	"math/rand"
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/internal/unsafecast"
)

func EncodeBoolean(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeBoolean,
		encoding.Encoding.DecodeBoolean,
		generate[byte],
	)
}

func EncodeLevels(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeLevels,
		encoding.Encoding.DecodeLevels,
		generate[byte],
	)
}

func EncodeInt32(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeInt32,
		encoding.Encoding.DecodeInt32,
		generate[int32],
	)
}

func EncodeInt64(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeInt64,
		encoding.Encoding.DecodeInt64,
		generate[int64],
	)
}

func EncodeFloat(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeFloat,
		encoding.Encoding.DecodeFloat,
		generate[float32],
	)
}

func EncodeDouble(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeDouble,
		encoding.Encoding.DecodeDouble,
		generate[float64],
	)
}

func EncodeByteArray(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		func(enc encoding.Encoding, dst []byte, src []string) ([]byte, error) {
			size := 0
			for _, s := range src {
				size += len(s)
			}

			offsets := make([]uint32, 0, len(src)+1)
			values := make([]byte, 0, size)

			for _, s := range src {
				offsets = append(offsets, uint32(len(values)))
				values = append(values, s...)
			}

			offsets = append(offsets, uint32(len(values)))
			return enc.EncodeByteArray(dst, values, offsets)
		},

		func(enc encoding.Encoding, dst []string, src []byte) ([]string, error) {
			dst = dst[:0]

			values, offsets, err := enc.DecodeByteArray(nil, src, nil)
			if err != nil {
				return dst, err
			}

			if len(offsets) > 0 {
				baseOffset := offsets[0]

				for _, endOffset := range offsets[1:] {
					dst = append(dst, unsafecast.String(values[baseOffset:endOffset]))
					baseOffset = endOffset
				}
			}

			return dst, nil
		},

		func(dst []string, src []byte, prng *rand.Rand) []string {
			limit := len(src)/10 + 1

			for i := 0; i < len(src); {
				n := prng.Intn(limit) + 1
				r := len(src) - i
				if n > r {
					n = r
				}
				dst = append(dst, unsafecast.String(src[i:i+n]))
				i += n
			}

			return dst
		},
	)
}

type encodingFunc[T comparable] func(encoding.Encoding, []byte, []T) ([]byte, error)

type decodingFunc[T comparable] func(encoding.Encoding, []T, []byte) ([]T, error)

type generateFunc[T comparable] func(dst []T, src []byte, prng *rand.Rand) []T

func encode[T comparable](f *testing.F, e encoding.Encoding, encode encodingFunc[T], decode decodingFunc[T], generate generateFunc[T]) {
	const bufferSize = 64 * 1024
	var zero T
	var err error
	var buf = make([]T, bufferSize/unsafe.Sizeof(zero))
	var src = make([]T, bufferSize/unsafe.Sizeof(zero))
	var dst = make([]byte, bufferSize)
	var prng = rand.New(rand.NewSource(0))

	f.Fuzz(func(t *testing.T, input []byte, seed int64) {
		prng.Seed(seed)
		src = generate(src[:0], input, prng)

		dst, err = encode(e, dst, src)
		if err != nil {
			t.Error(err)
			return
		}

		buf, err = decode(e, buf, dst)
		if err != nil {
			t.Error(err)
			return
		}

		if !equal(buf, src) {
			t.Error("decoded output does not match the original input")
			return
		}
	})
}

func equal[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func generate[T comparable](dst []T, src []byte, prng *rand.Rand) []T {
	return append(dst[:0], unsafecast.Slice[T](src)...)
}
