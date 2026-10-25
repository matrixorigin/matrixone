// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package batch_test

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// Flattening a constant creates a legal flat vector with shared offsets. A
// second real batch clone must remain feasible, independently of logical row
// payload (8 GiB here, exceeding one mpool allocation's limit).
func TestBatchClonePreservesSharedVarlen(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	source := batch.NewWithSize(1)
	source.Attrs = []string{"payload"}
	var err error
	source.Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), bytes.Repeat([]byte{'x'}, 1<<20), 8192, mp)
	require.NoError(t, err)
	source.SetRowCount(8192)
	defer source.Clean(mp)
	flat, err := source.Dup(mp)
	require.NoError(t, err)
	defer flat.Clean(mp)
	require.False(t, flat.Vecs[0].IsConst())
	require.Equal(t, 1<<20, len(flat.Vecs[0].GetArea()))
	copy, err := flat.Dup(mp)
	require.NoError(t, err)
	defer copy.Clean(mp)
	require.Equal(t, flat.RowCount(), copy.RowCount())
	require.Equal(t, 1<<20, len(copy.Vecs[0].GetArea()))
	source.Clean(mp)
	flat.Clean(mp)
	require.Equal(t, bytes.Repeat([]byte{'x'}, 1<<20), copy.Vecs[0].GetBytesAt(8191), "clone owns its area after both sources retire")
}
