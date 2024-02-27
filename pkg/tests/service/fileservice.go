// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// fileServices contains all FileService instances.
type fileServices struct {
	sync.RWMutex

	t            *testing.T
	tnServiceNum int
	cnServiceNum int

	tnLocalFSs []fileservice.FileService
	cnLocalFSs []fileservice.FileService
	s3FS       fileservice.FileService
	etlFS      fileservice.FileService
}

func (c *testCluster) createFS(
	ctx context.Context,
	dir string,
	name string) fileservice.FileService {
	if c.opt.keepData {
		fs, err := fileservice.NewLocalFS(ctx, name, filepath.Join(dir, name), fileservice.CacheConfig{}, nil)
		require.NoError(c.t, err)
		return fs
	}

	fs, err := fileservice.NewMemoryFS(name, fileservice.DisabledCacheConfig, nil)
	require.NoError(c.t, err)
	return fs

}

// newFileServices constructs an instance of fileServices.
func (c *testCluster) buildFileServices(ctx context.Context) *fileServices {
	tnServiceNum := c.opt.initial.tnServiceNum

	tnLocals := make([]fileservice.FileService, 0, tnServiceNum)
	for i := 0; i < tnServiceNum; i++ {
		tnLocals = append(tnLocals, c.createFS(ctx, c.tn.cfgs[i].DataDir, defines.LocalFileServiceName))
	}

	return &fileServices{
		t:            c.t,
		tnServiceNum: tnServiceNum,
		tnLocalFSs:   tnLocals,
		s3FS:         c.createFS(ctx, c.opt.rootDataDir, defines.SharedFileServiceName),
		etlFS:        c.createFS(ctx, c.opt.rootDataDir, defines.ETLFileServiceName),
	}
}

// assertFileServiceLocked asserts constructed file services.
func (f *fileServices) assertFileServiceLocked() {
	assert.Equal(f.t, f.tnServiceNum, len(f.tnLocalFSs))
	assert.Equal(f.t, f.cnServiceNum, len(f.cnLocalFSs))
}

// getTNLocalFileService gets local FileService for TN service.
func (f *fileServices) getTNLocalFileService(index int) fileservice.FileService {
	f.RLock()
	defer f.RUnlock()

	f.assertFileServiceLocked()

	if index >= len(f.tnLocalFSs) {
		return nil
	}
	return f.tnLocalFSs[index]
}

func (f *fileServices) getCNLocalFileService(index int) fileservice.FileService {
	f.RLock()
	defer f.RUnlock()

	f.assertFileServiceLocked()

	if index >= len(f.cnLocalFSs) {
		return nil
	}
	return f.cnLocalFSs[index]
}

// getS3FileService gets S3 FileService for all TN services.
func (f *fileServices) getS3FileService() fileservice.FileService {
	f.RLock()
	defer f.RUnlock()
	f.assertFileServiceLocked()
	return f.s3FS
}

// getETLFileService gets ETL FileService for all TN services.
func (f *fileServices) getETLFileService() fileservice.FileService {
	f.RLock()
	defer f.RUnlock()
	f.assertFileServiceLocked()
	return f.etlFS
}
