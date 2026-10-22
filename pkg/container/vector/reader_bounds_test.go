// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package vector

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestVectorReaderRejectsImpossibleLengthsBeforeAllocation(t *testing.T) {
	for _, field := range []string{"rows", "data", "area", "nulls"} {
		for _, size := range []int32{-1, math.MaxInt32} {
			t.Run(fmt.Sprintf("%s/%d", field, size), func(t *testing.T) {
				var data bytes.Buffer
				data.WriteByte(byte(FLAT))
				typ := types.T_int64.ToType()
				data.Write(types.EncodeType(&typ))
				if field == "rows" {
					require.NoError(t, binary.Write(&data, binary.LittleEndian, size))
				} else {
					require.NoError(t, binary.Write(&data, binary.LittleEndian, int32(0)))
					for _, name := range []string{"data", "area", "nulls"} {
						if name == field {
							require.NoError(t, binary.Write(&data, binary.LittleEndian, size))
							break
						}
						require.NoError(t, binary.Write(&data, binary.LittleEndian, int32(0)))
					}
				}
				data.Write(make([]byte, 32))
				mp := mpool.MustNew("vector-length-bound")
				defer mpool.DeleteMPool(mp)
				v := NewOffHeapVec()
				defer v.Free(mp)
				err := v.UnmarshalWithReader(io.LimitReader(bytes.NewReader(data.Bytes()), int64(data.Len())), mp)
				require.Error(t, err)
				require.Zero(t, mp.AllocatedBytes())
			})
		}
	}
}

type noKnownLengthReader struct{ io.Reader }

func TestVectorReaderShortReadKeepsGrownOwnerForCleanup(t *testing.T) {
	mp := mpool.MustNew("vector-short-owner")
	defer mpool.DeleteMPool(mp)
	v := NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, AppendFixed(v, int64(1), false, mp))
	var data bytes.Buffer
	data.WriteByte(byte(FLAT))
	typ := types.T_int64.ToType()
	data.Write(types.EncodeType(&typ))
	require.NoError(t, binary.Write(&data, binary.LittleEndian, int32(128)))
	require.NoError(t, binary.Write(&data, binary.LittleEndian, int32(1024)))
	// No frame boundary is available for this reader. Growth is permitted,
	// but the subsequent short read must leave exactly one valid owner.
	err := v.UnmarshalWithReader(noKnownLengthReader{bytes.NewReader(data.Bytes())}, mp)
	require.ErrorIs(t, err, io.EOF)
	require.Positive(t, mp.AllocatedBytes())
	v.Free(mp)
	require.Zero(t, mp.AllocatedBytes())
}
