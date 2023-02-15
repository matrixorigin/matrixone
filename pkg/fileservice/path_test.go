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

func TestParsePath(t *testing.T) {
	path, err := ParsePath("foo")
	assert.Nil(t, err)
	assert.Equal(t, "foo", path.File)

	path, err = ParsePath(JoinPath("foo", "bar"))
	assert.Nil(t, err)
	assert.Equal(t, "bar", path.File)
	assert.Equal(t, "foo", path.Service)

	path, err = ParsePath(JoinPath("foo,baz,quux", "bar"))
	assert.Nil(t, err)
	assert.Equal(t, "bar", path.File)
	assert.Equal(t, "foo", path.Service)
	assert.Equal(t, []string{"baz", "quux"}, path.ServiceArguments)

	path, err = ParsePath("矩阵")
	assert.Nil(t, err)
	assert.Equal(t, "矩阵", path.File)
}
