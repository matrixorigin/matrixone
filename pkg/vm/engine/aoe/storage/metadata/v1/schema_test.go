package metadata

import (
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/container/types"
	"testing"
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
		Oid:       types.T_int32,
		Size:      4,
		Width:     32,
	}))
	assert.Equal(t, -1, schema0.GetColIdx("xxxx"))
	assert.Equal(t, 0, schema0.GetColIdx("mock_0"))
	schema0.Indices = append(schema0.Indices, &IndexInfo{})
	assert.Equal(t, "mock_1", schema0.Clone().ColDefs[1].Name)
}
