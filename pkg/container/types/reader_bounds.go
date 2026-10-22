// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package types

import (
	"fmt"
	"io"
	"math"
)

// ReaderRemaining reports a known frame boundary. A LimitedReader prevents
// over-reading, but without checking N before make/Grow a forged length can
// still allocate its entire claimed size before the read returns EOF.
func ReaderRemaining(r io.Reader) (int64, bool) {
	switch source := r.(type) {
	case *io.LimitedReader:
		n := source.N
		if inner, ok := ReaderRemaining(source.R); ok {
			n = min(n, inner)
		}
		return n, true
	case *io.SectionReader:
		offset, err := source.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, false
		}
		return source.Size() - offset, true
	case interface{ Len() int }:
		return int64(source.Len()), true
	default:
		return 0, false
	}
}

func CheckReadSize(r io.Reader, n int64) error {
	if n < 0 {
		return fmt.Errorf("negative encoded size %d", n)
	}
	if remaining, ok := ReaderRemaining(r); ok && (remaining < 0 || n > remaining) {
		return fmt.Errorf("encoded size %d exceeds remaining frame bytes %d", n, remaining)
	}
	return nil
}

// CheckReadCount bounds count-driven allocations by the minimum number of
// encoded bytes each item needs, including the mandatory remaining footer.
func CheckReadCount(r io.Reader, count, itemBytes, footerBytes int64) error {
	if count < 0 || itemBytes < 0 || footerBytes < 0 || itemBytes > 0 && count > (math.MaxInt64-footerBytes)/itemBytes {
		return fmt.Errorf("invalid encoded count %d", count)
	}
	return CheckReadSize(r, count*itemBytes+footerBytes)
}
