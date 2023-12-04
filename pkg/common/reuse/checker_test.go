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

package reuse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDoubleFree(t *testing.T) {
	RunReuseTests(func() {
		c := newChecker[person](true)
		p := &person{}
		c.created(p)

		c.got(p)
		c.free(p)

		defer func() {
			assert.NotNil(t, recover())
		}()
		c.free(p)
	})
}

func TestLeakFree(t *testing.T) {
	RunReuseTests(func() {
		c := newChecker[person](true)
		p := &person{}
		c.created(p)
		c.got(p)

		defer func() {
			assert.NotNil(t, recover())
		}()
		c.gc(p)
	})
}
