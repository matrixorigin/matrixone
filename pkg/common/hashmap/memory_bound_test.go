// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package hashmap

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStringIteratorDoesNotKeepRotatingWidePeaks(t *testing.T) {
	pool := mpool.MustNewZero()
	table, err := NewStrHashMap(false, pool)
	require.NoError(t, err)
	defer table.Free()
	iterator := table.NewIterator()
	wide := bytes.Repeat([]byte{'x'}, 128<<10)
	for position := 0; position < 4; position++ {
		vec := vector.NewVec(types.T_varchar.ToType())
		for row := 0; row < 4; row++ {
			value := []byte("s")
			if row == position {
				value = wide
			}
			require.NoError(t, vector.AppendBytes(vec, value, false, pool))
		}
		_, _, err := iterator.Insert(0, 4, []*vector.Vector{vec})
		require.NoError(t, err)
		vec.Free(pool)
	}
	require.Less(t, StrIteratorCapacity(iterator), len(wide)*3, "key buffers follow current window, not every historical row-position peak")
}
