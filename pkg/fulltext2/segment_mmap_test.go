// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build darwin || linux

package fulltext2

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentFreeRetainsLeaseWhenMunmapFails(t *testing.T) {
	original := munmapFn
	t.Cleanup(func() { munmapFn = original })
	munmapErr := errors.New("synthetic munmap failure")
	munmapFn = func([]byte) error { return munmapErr }

	releases := 0
	s := &Segment{
		mmapData:    []byte("mapped"),
		mmapRelease: func() { releases++ },
	}
	s.Free()
	require.NotNil(t, s.mmapData)
	require.NotNil(t, s.mmapRelease)
	require.Zero(t, releases)

	munmapFn = func([]byte) error { return nil }
	s.Free()
	require.Nil(t, s.mmapData)
	require.Nil(t, s.mmapRelease)
	require.Equal(t, 1, releases)
}

func TestFulltext2SearchDestroyDefersFailedMunmapToPoolOwner(t *testing.T) {
	original := munmapFn
	t.Cleanup(func() { munmapFn = original })
	munmapErr := errors.New("synthetic destroy munmap failure")
	munmapFn = func([]byte) error { return munmapErr }

	pool := newBaseFilePool(1, 1)
	releases := 0
	seg := &Segment{
		mmapData:      []byte("mapped"),
		mmapRelease:   func() { releases++ },
		mmapRetryPool: pool,
	}
	search := newFulltext2SearchWithBasePool(TableConfig{}, pool)
	search.basePoolOwned = true
	search.idx = NewIndex([]*Segment{seg}, nil)
	search.loaded = true
	search.Destroy()
	require.Nil(t, search.idx)
	require.Same(t, pool, search.basePool, "an owned pool remains the retry owner while munmap is failing")
	require.True(t, search.basePoolOwned)
	require.Equal(t, 1, pool.deferredCount(), "pool owner must retain a failed mapping after Destroy")
	require.Zero(t, releases)

	munmapFn = func([]byte) error { return nil }
	search.Destroy()
	require.Zero(t, pool.deferredCount())
	require.Equal(t, 1, releases)
	require.Nil(t, search.basePool)
}
