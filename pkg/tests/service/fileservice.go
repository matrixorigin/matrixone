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
	dnServiceNum int
	cnServiceNum int

	dnLocalFSs []fileservice.FileService
	cnLocalFSs []fileservice.FileService
	sharedFS   fileservice.FileService
	publicFS   fileservice.FileService
}

// newFileServices constructs an instance of fileServices.
func (c *testCluster) buildFileServices(ctx context.Context) *fileServices {
	dnServiceNum := c.opt.initial.dnServiceNum
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

	dnLocals := make([]fileservice.FileService, 0, dnServiceNum)
	for i := 0; i < dnServiceNum; i++ {
		dnLocals = append(dnLocals, factory(c.dn.cfgs[i].DataDir, defines.LocalFileServiceName))
	}

	cnLocals := make([]fileservice.FileService, 0, cnServiceNum)
	for i := 0; i < cnServiceNum; i++ {
		cnLocals = append(cnLocals, factory(filepath.Join(c.opt.rootDataDir, c.cn.cfgs[i].UUID), defines.LocalFileServiceName))
	}

	return &fileServices{
		t:            c.t,
		dnServiceNum: dnServiceNum,
		cnServiceNum: cnServiceNum,
		dnLocalFSs:   dnLocals,
		cnLocalFSs:   cnLocals,
		sharedFS:     factory(c.opt.rootDataDir, defines.SharedFileServiceName),
		publicFS:     factory(c.opt.rootDataDir, defines.PublicFileServiceName),
	}
}

// assertFileServiceLocked asserts constructed file services.
func (f *fileServices) assertFileServiceLocked() {
	assert.Equal(f.t, f.dnServiceNum, len(f.dnLocalFSs))
	assert.Equal(f.t, f.cnServiceNum, len(f.cnLocalFSs))
}

// getDNLocalFileService gets local FileService for DN service.
func (f *fileServices) getDNLocalFileService(index int) fileservice.FileService {
	f.RLock()
	defer f.RUnlock()

	f.assertFileServiceLocked()

	if index >= len(f.dnLocalFSs) {
		return nil
	}
	return f.dnLocalFSs[index]
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

// getSharedileService gets Shared FileService for all DN services.
func (f *fileServices) getSharedileService() fileservice.FileService {
	f.RLock()
	defer f.RUnlock()
	f.assertFileServiceLocked()
	return f.sharedFS
}

// getPublicFileService gets Public FileService for all DN services.
func (f *fileServices) getPublicFileService() fileservice.FileService {
	f.RLock()
	defer f.RUnlock()
	f.assertFileServiceLocked()
	return f.publicFS
}
