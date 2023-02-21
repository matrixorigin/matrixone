// Copyright 2023 Matrix Origin
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

package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetUnixSocketAddress(t *testing.T) {
	assert.NoError(t, os.RemoveAll("/tmp/TestGetUnixSocketAddress/"))
	f := &FrontendParameters{UnixSocketAddress: "/tmp/TestGetUnixSocketAddress/1"}
	assert.Equal(t, f.UnixSocketAddress, f.GetUnixSocketAddress())
}

func TestIsFileExist(t *testing.T) {
	existFile := "/tmp/TestIsFileExist/exist"
	notExistFile := "/tmp/TestIsFileExist/not_exist"
	assert.NoError(t, os.MkdirAll("/tmp/TestIsFileExist", 0755))
	defer func() {
		assert.NoError(t, os.RemoveAll("/tmp/TestIsFileExist"))
	}()

	f, err := os.Create(existFile)
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	ok, err := isFileExist(existFile)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = isFileExist(notExistFile)
	assert.NoError(t, err)
	assert.False(t, ok)
}
