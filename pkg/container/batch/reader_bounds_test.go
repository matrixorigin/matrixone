// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package batch

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestBatchReaderRejectsImpossibleCountsBeforeAllocation(t *testing.T) {
	for _, field := range []string{"vectors", "attrs"} {
		for _, count := range []int32{-1, math.MaxInt32} {
			t.Run(fmt.Sprintf("%s/%d", field, count), func(t *testing.T) {
				var data bytes.Buffer
				require.NoError(t, binary.Write(&data, binary.LittleEndian, int64(0)))
				if field == "attrs" {
					require.NoError(t, binary.Write(&data, binary.LittleEndian, int32(0)))
				}
				require.NoError(t, binary.Write(&data, binary.LittleEndian, count))
				data.Write(make([]byte, 32))
				mp := mpool.MustNew("batch-count-bound")
				defer mpool.DeleteMPool(mp)
				bat := NewOffHeapEmpty()
				defer bat.Clean(mp)
				err := bat.UnmarshalFromReader(io.LimitReader(bytes.NewReader(data.Bytes()), int64(data.Len())), mp)
				require.Error(t, err)
				require.Zero(t, mp.AllocatedBytes())
				require.Empty(t, bat.Vecs)
				require.Empty(t, bat.Attrs)
			})
		}
	}
}
