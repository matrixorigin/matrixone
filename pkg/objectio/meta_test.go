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

package objectio

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildMetaData(t *testing.T) {
	objectMeta := BuildMetaData(20, 30)
	for i := uint16(0); i < 20; i++ {
		blkMeta := objectMeta.GetBlockMeta(uint32(i))
		assert.Equal(t, i, blkMeta.BlockHeader().Sequence())
		assert.Equal(t, uint16(30), blkMeta.BlockHeader().ColumnCount())
	}
}
