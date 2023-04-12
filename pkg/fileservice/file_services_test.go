// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

func TestFileServices(t *testing.T) {
	t.Run("file service", func(t *testing.T) {
		testFileService(t, func(name string) FileService {
			dir := t.TempDir()
			fs, err := NewLocalFS(name, dir, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			fs2, err := NewFileServices(name, fs)
			assert.Nil(t, err)
			return fs2
		})
	})
}

func TestFileServicesNameCaseInsensitive(t *testing.T) {
	fs1, err := NewMemoryFS("foo", DisabledCacheConfig, nil)
	assert.Nil(t, err)
	fs2, err := NewMemoryFS("FOO", DisabledCacheConfig, nil)
	assert.Nil(t, err)
	_, err = NewFileServices(fs1.Name(), fs1, fs2)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDupServiceName))
}
