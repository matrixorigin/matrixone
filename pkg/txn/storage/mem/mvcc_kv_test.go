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

package mem

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
)

func TestMVCCSetAndGetAndDelete(t *testing.T) {
	mkv := NewMVCCKV()
	k := []byte("k")

	versions := []timestamp.Timestamp{
		{PhysicalTime: 1},
		{PhysicalTime: 1, LogicalTime: 1},
		{PhysicalTime: 1, LogicalTime: 2},
		{PhysicalTime: 1, LogicalTime: 3},
	}
	values := [][]byte{[]byte("1-0"), []byte("1-1"), []byte("1-2"), []byte("1-3")}

	for i := 0; i < len(versions)-1; i++ {
		mkv.Set(k, versions[i], values[i])
	}

	for i := 0; i < len(versions); i++ {
		v, ok := mkv.Get(k, versions[i])
		if i < len(versions)-1 {
			assert.True(t, ok)
			assert.Equal(t, values[i], v)
		} else {
			assert.False(t, ok)
			assert.Empty(t, v)
		}
	}

	mkv.Delete(k, versions[0])
	v, ok := mkv.Get(k, versions[0])
	assert.False(t, ok)
	assert.Empty(t, v)
}

func TestMVCCAscendRange(t *testing.T) {
	mkv := NewMVCCKV()
	k := []byte("k")

	versions := []timestamp.Timestamp{
		{PhysicalTime: 1},
		{PhysicalTime: 1, LogicalTime: 1},
		{PhysicalTime: 1, LogicalTime: 2},
		{PhysicalTime: 1, LogicalTime: 3},
	}
	values := [][]byte{[]byte("1-0"), []byte("1-1"), []byte("1-2"), []byte("1-3")}

	for i := 0; i < len(versions); i++ {
		mkv.Set(k, versions[i], values[i])
	}

	var scanVersions []timestamp.Timestamp
	var scanValues [][]byte
	fn := func(v []byte, ts timestamp.Timestamp) {
		scanValues = append(scanValues, v)
		scanVersions = append(scanVersions, ts)
	}

	mkv.AscendRange(k, versions[0], versions[3], fn)
	assert.Equal(t, versions[:3], scanVersions)
	assert.Equal(t, values[:3], scanValues)

	scanVersions = scanVersions[:0]
	scanValues = scanValues[:0]
	mkv.AscendRange(k, versions[0], versions[0], fn)
	assert.Empty(t, scanVersions)
	assert.Empty(t, scanValues)
}
