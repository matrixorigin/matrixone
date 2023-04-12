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

	"github.com/stretchr/testify/assert"
)

func TestMemoryFS(t *testing.T) {

	t.Run("file service", func(t *testing.T) {
		testFileService(t, func(name string) FileService {
			fs, err := NewMemoryFS(name, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("replaceable file service", func(t *testing.T) {
		testReplaceableFileService(t, func() ReplaceableFileService {
			fs, err := NewMemoryFS("memory", DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

}

func BenchmarkMemoryFS(b *testing.B) {
	benchmarkFileService(b, func() FileService {
		fs, err := NewMemoryFS("memory", DisabledCacheConfig, nil)
		assert.Nil(b, err)
		return fs
	})
}
