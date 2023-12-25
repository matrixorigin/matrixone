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

package interval

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntervalChecker(t *testing.T) {

	tree := NewOverlapChecker("MemCache_LRU")

	// [0,5)
	err := tree.Insert("s3://db1/table1/A", 0, 5)
	assert.Nil(t, err)

	//[0,5), [5,10)
	err = tree.Insert("s3://db1/table1/A", 5, 10)
	assert.Nil(t, err)

	//[0,5), [5,10)
	err = tree.Insert("s3://db1/table1/A", 6, 7)
	assert.NotNil(t, err)

	//[0,5)
	err = tree.Remove("s3://db1/table1/A", 5, 10)
	assert.Nil(t, err)

	//[0,5), [6,7)
	err = tree.Insert("s3://db1/table1/A", 6, 7)
	assert.Nil(t, err)

	//[0,5), [5,6), [6,7)
	err = tree.Insert("s3://db1/table1/A", 5, 6)
	assert.Nil(t, err)

	//[0,5), [3,5), [5,6), [6,7)
	err = tree.Insert("s3://db1/table1/A", 3, 5)
	assert.NotNil(t, err)
	assert.Equal(t, "internal error: Overlapping key range found in MemCache_LRU when inserting [3 5). The key s3://db1/table1/A contains overlapping intervals [0 5), ", err.Error())

	//[0,5), [3,6), [5,6), [6,7)
	err = tree.Insert("s3://db1/table1/A", 3, 6)
	assert.NotNil(t, err)
	assert.Equal(t, "internal error: Overlapping key range found in MemCache_LRU when inserting [3 6). The key s3://db1/table1/A contains overlapping intervals [0 5), [5 6), ", err.Error())

	//[3,7)
	err = tree.Insert("s3://db1/table1/B", 3, 7)
	assert.Nil(t, err)
}
