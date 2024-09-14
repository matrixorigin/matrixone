// Copyright 2024 Matrix Origin
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

package memoryengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Only for passing the UT coverage check.
func Test_TableReader(t *testing.T) {
	table := new(Table)
	assert.Panics(t, func() {
		table.BuildShardingReaders(
			nil,
			nil,
			nil,
			nil,
			0,
			0,
			false,
			0,
		)
	})
	assert.Panics(t, func() {
		table.GetProcess()
	})
}
