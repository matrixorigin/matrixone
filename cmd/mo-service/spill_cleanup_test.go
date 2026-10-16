// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package main

import (
	"context"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type startupSpillDeleteFS struct {
	fileservice.FileService
	blocked bool
}

func (fs *startupSpillDeleteFS) Delete(ctx context.Context, paths ...string) error {
	if fs.blocked {
		return os.ErrPermission
	}
	return fs.FileService.Delete(ctx, paths...)
}

func TestClearSpillFilesRejectsUndeletedPreviousOwners(t *testing.T) {
	ctx := context.Background()
	local, err := fileservice.NewLocalFS(ctx, defines.LocalFileServiceName, t.TempDir(), fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	fault := &startupSpillDeleteFS{FileService: local, blocked: true}
	fs, err := fileservice.NewFileServices(defines.LocalFileServiceName, fault)
	require.NoError(t, err)
	defer fs.Close(ctx)
	spill := fileservice.SubPath(local, defines.SpillFileServiceName)
	require.NoError(t, spill.Write(ctx, fileservice.IOVector{FilePath: "previous-owner", Entries: []fileservice.IOEntry{{Size: 1, Data: []byte{1}}}}))
	require.ErrorIs(t, clearSpillFiles(ctx, fs), os.ErrPermission)
	count := 0
	for entry, err := range spill.List(ctx, "/") {
		require.NoError(t, err)
		require.Equal(t, "previous-owner", entry.Name)
		count++
	}
	require.Equal(t, 1, count)
	fault.blocked = false
	require.NoError(t, clearSpillFiles(ctx, fs))
	for _, err := range spill.List(ctx, "/") {
		require.NoError(t, err)
		t.Fatal("startup left an unowned spill file")
	}
}
