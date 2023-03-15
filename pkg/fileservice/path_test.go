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
	p := "foo"
	path, err := ParsePath(p)
	assert.Nil(t, err)
	assert.Equal(t, "foo", path.File)
	assert.Equal(t, p, path.String())

	p = JoinPath("foo", "bar")
	path, err = ParsePath(p)
	assert.Nil(t, err)
	assert.Equal(t, "bar", path.File)
	assert.Equal(t, "foo", path.Service)
	assert.Equal(t, p, path.String())

	p = JoinPath("foo,baz,quux", "bar")
	path, err = ParsePath(p)
	assert.Nil(t, err)
	assert.Equal(t, "bar", path.File)
	assert.Equal(t, "foo", path.Service)
	assert.Equal(t, []string{"baz", "quux"}, path.ServiceArguments)
	assert.Equal(t, p, path.String())

	path, err = ParsePath("矩阵")
	assert.Nil(t, err)
	assert.Equal(t, "矩阵", path.File)
	assert.Equal(t, "矩阵", path.String())
}
