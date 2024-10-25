// Copyright 2024 Matrix Origin
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

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func Test_saveProfile(t *testing.T) {
	dir := t.TempDir()
	fs, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, dir)
	assert.Nil(t, err)
	globalEtlFS = fs
	saveCpuProfile(time.Second)
	saveMallocProfile()
}

func Test_saveProfile2(t *testing.T) {
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	assert.NoError(t, err)
	globalEtlFS = fs
	saveCpuProfile(time.Second)
}
