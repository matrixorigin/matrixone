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

package memtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTupleLess(t *testing.T) {
	assert.True(t, Tuple{Min}.Less(Tuple{Int(42)}))
	assert.True(t, Tuple{Min}.Less(Tuple{Int(42), Min}))
	assert.True(t, Tuple{Int(42)}.Less(Tuple{Max}))
	assert.True(t, Tuple{Min}.Less(Tuple{Max}))
	assert.True(t, Tuple{Int(42), Min}.Less(Tuple{Int(42), Max}))
	assert.True(t, Tuple{Int(42)}.Less(Tuple{Int(42), Max}))
	assert.True(t, Tuple{Text("a")}.Less(Tuple{Text("b")}))
	assert.True(t, Tuple{Bool(false)}.Less(Tuple{Bool(true)}))
	assert.True(t, Tuple{Int(1)}.Less(Tuple{Int(2)}))
	assert.True(t, Tuple{Uint(1)}.Less(Tuple{Uint(2)}))
	assert.True(t, Tuple{Float(1)}.Less(Tuple{Float(2)}))
	assert.True(t, Tuple{Bytes("a")}.Less(Tuple{Bytes("b")}))
	assert.True(t, Tuple{ID(1)}.Less(Tuple{ID(2)}))

	assert.False(t, Tuple{Int(42), Text("foo")}.Less(Tuple{Int(42)}))
	assert.False(t, Tuple{Max}.Less(Tuple{Uint(42)}))
	assert.False(t, Tuple{Int(42)}.Less(Tuple{Min}))
	assert.False(t, Tuple{Text("b")}.Less(Tuple{Text("a")}))
	assert.False(t, Tuple{Bool(true)}.Less(Tuple{Bool(false)}))
	assert.False(t, Tuple{Int(2)}.Less(Tuple{Int(1)}))
	assert.False(t, Tuple{Uint(2)}.Less(Tuple{Uint(1)}))
	assert.False(t, Tuple{Float(2)}.Less(Tuple{Float(1)}))
	assert.False(t, Tuple{Bytes("b")}.Less(Tuple{Bytes("a")}))
	assert.False(t, Tuple{ID(2)}.Less(Tuple{ID(1)}))
}
