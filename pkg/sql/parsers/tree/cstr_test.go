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

package tree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_CStr(t *testing.T) {
	c1 := NewCStr("Hello", 1)
	assert.Equal(t, "hello", c1.ToLower())
	assert.Equal(t, "Hello", c1.Origin())
	assert.Equal(t, false, c1.Empty())
	c2 := NewCStr("Hello", 1)
	assert.Equal(t, "hello", c2.ToLower())
	c2.SetConfig(1)
	assert.Equal(t, "hello", c2.Compare())
	c2.SetConfig(0)
	assert.Equal(t, "hello", c2.Compare())
}
