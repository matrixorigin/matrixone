package containers

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"github.com/stretchr/testify/assert"
)

func TestBatch1(t *testing.T) {
	vecTypes := types.MockColTypes(4)[2:]
	attrs := []string{"attr1", "attr2"}
	nullable := []bool{false, true}
	bat := BuildBatch(attrs, vecTypes, nullable, 0)
	bat.Vecs[0].Append(int32(1))
	bat.Vecs[0].Append(int32(2))
	bat.Vecs[0].Append(int32(3))
	bat.Vecs[1].Append(int64(11))
	bat.Vecs[1].Append(int64(12))
	bat.Vecs[1].Append(int64(13))

	assert.Equal(t, 3, bat.Length())
	assert.False(t, bat.HasDelete())
	bat.Delete(1)
	assert.Equal(t, 3, bat.Length())
	assert.True(t, bat.HasDelete())
	assert.True(t, bat.IsDeleted(1))

	w := new(bytes.Buffer)
	_, err := bat.WriteTo(w)
	assert.NoError(t, err)

	r := bytes.NewBuffer(w.Bytes())
	bat2 := NewEmptyBatch()
	_, err = bat2.ReadFrom(r)
	assert.NoError(t, err)
	assert.Equal(t, bat.Length(), bat2.Length())

	bat.Close()
}
