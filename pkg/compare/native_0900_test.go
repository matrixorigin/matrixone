// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

package compare

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestNative0900StringCompareUsesResolvedCollation(t *testing.T) {
	values := [][]byte{[]byte("z"), []byte("Å"), []byte("a"), []byte("😀")}
	for _, tc := range []struct {
		name    string
		charset uint8
		cmp     func([]byte, []byte) int
	}{
		{name: "ai", charset: types.CharsetUTF8MB40900AI, cmp: collation.UCA0900AICollate},
		{name: "bin", charset: types.CharsetUTF8MB40900Bin, cmp: collation.UCA0900BinCollate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.NewWithCharset(types.T_varchar, 32, 0, tc.charset)
			left := vector.NewVec(typ)
			right := vector.NewVec(typ)
			defer left.Free(mp)
			defer right.Free(mp)
			for _, value := range values {
				require.NoError(t, vector.AppendBytes(left, value, false, mp))
			}
			require.NoError(t, vector.AppendBytes(right, values[0], false, mp))
			cmp := New(typ, false, false)
			cmp.Set(0, left)
			cmp.Set(1, right)
			for i, value := range values {
				require.Equal(t, sign(tc.cmp(value, values[0])), sign(cmp.Compare(0, 1, int64(i), 0)))
			}
		})
	}
}

func sign(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	default:
		return 0
	}
}
