// Copyright 2021 Matrix Origin
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

package testutils

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
)

func WaitExpect(timeout int, expect func() bool) {
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	interval := time.Duration(timeout) * time.Millisecond / 400
	for time.Now().Before(end) && !expect() {
		time.Sleep(interval)
	}
}

func GetDefaultTestPath(module string, t *testing.T) string {
	return filepath.Join("/tmp", module, t.Name())
}

func MakeDefaultTestPath(module string, t *testing.T) string {
	path := GetDefaultTestPath(module, t)
	err := os.MkdirAll(path, os.FileMode(0755))
	assert.Nil(t, err)
	return path
}

func RemoveDefaultTestPath(module string, t *testing.T) {
	path := GetDefaultTestPath(module, t)
	os.RemoveAll(path)
}

func InitTestEnv(module string, t *testing.T) string {
	RemoveDefaultTestPath(module, t)
	return MakeDefaultTestPath(module, t)
}

func EnsureNoLeak(t *testing.T) {
	// assert.Zerof(t, common.DefaultAllocator.CurrNB(), common.DefaultAllocator.Stats().Report(""))
	// XXX MPOOL: Too noisy
	if common.DefaultAllocator.CurrNB() != 0 {
		t.Log(common.DefaultAllocator.Stats().Report(""))
	}
}
