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

func TestLocalETLFS(t *testing.T) {

	t.Run("file service", func(t *testing.T) {
		testFileService(t, func() FileService {
			dir := t.TempDir()
			fs, err := NewLocalETLFS(dir)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("cache", func(t *testing.T) {
		testCache(t, func() FileService {
			dir := t.TempDir()
			fs, err := NewLocalETLFS(dir)
			assert.Nil(t, err)
			return fs
		})
	})

}
