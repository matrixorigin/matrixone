// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
//go:build linux

package rscthrottler

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnifiedMemoryDistinguishesAbsentAndIncompleteControllers(t *testing.T) {
	path := t.TempDir()
	_, _, _, err := readUnifiedMemory(path)
	require.ErrorIs(t, err, errNoMemoryController)
	write := func(name, value string) {
		require.NoError(t, os.WriteFile(filepath.Join(path, name), []byte(value), 0600))
	}
	write("memory.max", "2200\n")
	_, _, _, err = readUnifiedMemory(path)
	require.Error(t, err)
	require.NotErrorIs(t, err, errNoMemoryController, "a partial accounting sample cannot select host-memory fallback")
	write("memory.current", "1900\n")
	write("memory.stat", "inactive_file 500\nfile_dirty 100\nfile_writeback 50\n")
	limit, current, inactive, err := readUnifiedMemory(path)
	require.NoError(t, err)
	require.Equal(t, int64(2200), limit)
	require.Equal(t, int64(1900), current)
	require.Equal(t, int64(350), inactive)
	write("memory.max", "max\n")
	limit, _, _, err = readUnifiedMemory(path)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), limit)
	write("memory.stat", "inactive_file 500\n")
	_, _, inactive, err = readUnifiedMemory(path)
	require.NoError(t, err)
	require.Zero(t, inactive, "omitted dirty/writeback counters cannot create reclaim credit")
	write("memory.current", "broken\n")
	_, _, _, err = readUnifiedMemory(path)
	require.Error(t, err)
	require.NotErrorIs(t, err, errNoMemoryController)
	require.NoError(t, os.Remove(filepath.Join(path, "memory.current")))
	require.NoError(t, os.Mkdir(filepath.Join(path, "memory.current"), 0700))
	_, _, _, err = readUnifiedMemory(path)
	require.Error(t, err)
	require.NotErrorIs(t, err, errNoMemoryController, "I/O errors must remain errors even before first successful controller sample")
}
