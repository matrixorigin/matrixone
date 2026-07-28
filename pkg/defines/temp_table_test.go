// Copyright 2025 Matrix Origin
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

package defines

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestGenTempTableName(t *testing.T) {
	sid := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	name := GenTempTableName(sid, "db1", "t1")
	assert.True(t, IsTempTableName(name))
	assert.Contains(t, name, "t1")
	assert.Contains(t, name, "db1")
	assert.NotContains(t, name, "-")
	assert.Equal(t, "__mo_tmp_123e4567e89b12d3a456426614174000_db1_t1", name)
}
