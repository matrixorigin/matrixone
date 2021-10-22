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

package metadata

import (
	"matrixone/pkg/container/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchema(t *testing.T) {
	schema0 := MockSchema(2)
	schema1 := MockVarCharSchema(2)
	schema2 := MockDateSchema(0)
	assert.False(t, (*Schema)(nil).Valid())
	assert.False(t, schema2.Valid())
	schema2 = MockDateSchema(2)
	assert.True(t, schema2.Valid())
	schema2 = MockSchemaAll(14)
	assert.Equal(t, 14, len(schema2.ColDefs))
	assert.True(t, schema0.Valid())
	schema1.ColDefs[0].Idx = 1
	assert.False(t, schema1.Valid())
	schema1.ColDefs[0].Idx = 0
	schema1.ColDefs[0].Name = schema1.ColDefs[1].Name
	assert.False(t, schema1.Valid())
	typs := schema0.Types()
	assert.True(t, typs[0].Eq(types.Type{
		Oid:   types.T_int32,
		Size:  4,
		Width: 32,
	}))
	assert.Equal(t, -1, schema0.GetColIdx("xxxx"))
	assert.Equal(t, 0, schema0.GetColIdx("mock_0"))
	schema0.Indices = append(schema0.Indices, &IndexInfo{})
	assert.Equal(t, "mock_1", schema0.Clone().ColDefs[1].Name)
}
