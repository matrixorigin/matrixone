// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
)

func newTS(i int) timestamp.Timestamp {
	return timestamp.Timestamp{
		PhysicalTime: int64(i),
	}
}

func TestVersions(t *testing.T) {
	versions := new(Versions[int])

	// set begin and end
	i1 := 1
	versions.Set(newTS(1), &i1)
	versions.Set(newTS(3), nil)
	check1 := func() {
		p := versions.Get(newTS(1))
		assert.Nil(t, p)
		p = versions.Get(newTS(2))
		assert.Equal(t, 1, *p)
		p = versions.Get(newTS(3))
		assert.Nil(t, p)
	}
	check1()

	// set same begin time
	versions.Set(newTS(1), &i1)
	check1()

	// new span
	i2 := 42
	versions.Set(newTS(10), &i2)
	versions.Set(newTS(30), nil)
	check2 := func() {
		p := versions.Get(newTS(10))
		assert.Nil(t, p)
		p = versions.Get(newTS(20))
		assert.Equal(t, 42, *p)
		p = versions.Get(newTS(30))
		assert.Nil(t, p)
	}
	check2()
	check1()

	i3 := 99
	versions.Set(newTS(50), &i3)
	i4 := 100
	versions.Set(newTS(60), &i4)
	p := versions.Get(newTS(60))
	assert.Equal(t, 99, *p) // not visible until 61
	p = versions.Get(newTS(61))
	assert.Equal(t, 100, *p)

}
