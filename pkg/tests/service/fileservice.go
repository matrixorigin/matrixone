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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	s3FS       fileservice.FileService
}

// newFileServices constructs an instance of fileServices.
func newFileServices(t *testing.T, dnServiceNum int, cnServiceNum int) *fileServices {
	dnLocals := make([]fileservice.FileService, 0, dnServiceNum)
	for i := 0; i < dnServiceNum; i++ {
		fs, err := fileservice.NewMemoryFS("LOCAL")
		require.NoError(t, err)
		dnLocals = append(dnLocals, fs)
	}

	cnLocals := make([]fileservice.FileService, 0, cnServiceNum)
	for i := 0; i < cnServiceNum; i++ {
		fs, err := fileservice.NewMemoryFS("LOCAL")
		require.NoError(t, err)
		cnLocals = append(cnLocals, fs)
	}

	s3fs, err := fileservice.NewMemoryFS("S3")
	require.NoError(t, err)

	return &fileServices{
		t:            t,
		dnServiceNum: dnServiceNum,
		cnServiceNum: cnServiceNum,
		dnLocalFSs:   dnLocals,
		cnLocalFSs:   cnLocals,
		s3FS:         s3fs,
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

// getS3FileService gets S3 FileService for all DN services.
func (f *fileServices) getS3FileService() fileservice.FileService {
	f.RLock()
	defer f.RUnlock()
	f.assertFileServiceLocked()
	return f.s3FS
}
