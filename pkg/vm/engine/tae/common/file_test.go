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

package common

import (
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mf := NewMemFile(0)
	stat := mf.Stat()
	assert.Equal(t, stat.Size(), int64(0))
	assert.Equal(t, stat.Name(), "")
	assert.Equal(t, stat.CompressAlgo(), 0)
	assert.Equal(t, stat.OriginSize(), int64(0))
	assert.Equal(t, mf.GetFileType(), MemFile)
	mf.Ref()
	mf.Unref()
	_, err := mf.Read(make([]byte, 0))
	assert.Nil(t, err)
}
