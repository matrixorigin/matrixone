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

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateUUID(t *testing.T) {
	gen := newUUIDTxnIDGenerator("")
	assert.NotEmpty(t, gen.Generate())

	n := 1000000
	ids := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		id := string(gen.Generate())
		if _, ok := ids[id]; ok {
			assert.Fail(t, "repeat uuid")
		}
		ids[id] = struct{}{}
	}
}
