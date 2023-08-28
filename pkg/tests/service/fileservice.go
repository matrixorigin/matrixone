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

// newFileServices constructs an instance of fileServices.
func (c *testCluster) buildFileServices(ctx context.Context) *fileServices {
	tnServiceNum := c.opt.initial.tnServiceNum
	cnServiceNum := c.opt.initial.cnServiceNum

	factory := func(_ string, name string) fileservice.FileService {
		fs, err := fileservice.NewMemoryFS(name, fileservice.DisabledCacheConfig, nil)
		require.NoError(c.t, err)
		return fs
	}
	if c.opt.keepData {
		factory = func(dir string, name string) fileservice.FileService {
			fs, err := fileservice.NewLocalFS(ctx, name, filepath.Join(dir, name), fileservice.CacheConfig{}, nil)
			require.NoError(c.t, err)
			return fs
		}
	}

	tnLocals := make([]fileservice.FileService, 0, tnServiceNum)
	for i := 0; i < tnServiceNum; i++ {
		tnLocals = append(tnLocals, factory(c.tn.cfgs[i].DataDir, defines.LocalFileServiceName))
	}

	cnLocals := make([]fileservice.FileService, 0, cnServiceNum)
	for i := 0; i < cnServiceNum; i++ {
		cnLocals = append(cnLocals, factory(filepath.Join(c.opt.rootDataDir, c.cn.cfgs[i].UUID), defines.LocalFileServiceName))
	}

	return &fileServices{
		t:            c.t,
		tnServiceNum: tnServiceNum,
		cnServiceNum: cnServiceNum,
		tnLocalFSs:   tnLocals,
		cnLocalFSs:   cnLocals,
		s3FS:         factory(c.opt.rootDataDir, defines.SharedFileServiceName),
		etlFS:        factory(c.opt.rootDataDir, defines.ETLFileServiceName),
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
