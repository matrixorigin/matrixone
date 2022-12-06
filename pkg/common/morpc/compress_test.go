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

package morpc

import (
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
)

func TestCompress(t *testing.T) {
	src := []byte(strings.Repeat("hello", 100))
	dst := make([]byte, lz4.CompressBlockBound(len(src)))
	dst, err := compress(src, dst)
	assert.NoError(t, err)

	v1 := make([]byte, len(dst)*100)
	v1, err = uncompress(dst, v1)
	assert.NoError(t, err)
	assert.Equal(t, src, v1)
}
