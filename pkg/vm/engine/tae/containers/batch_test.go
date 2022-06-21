package containers

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"github.com/stretchr/testify/assert"
)

func checkEqualBatch(t *testing.T, b1, b2 *Batch) {
	assert.Equal(t, b1.Length(), b2.Length())
	assert.Equal(t, b1.DeleteCnt(), b2.DeleteCnt())
	assert.Equal(t, b1.DeleteCnt(), b2.DeleteCnt())
	if b1.HasDelete() {
		assert.True(t, b1.Deletes.Equals(b2.Deletes))
	}
	for i := range b1.Vecs {
		assert.Equal(t, b1.Attrs[i], b2.Attrs[i])
		checkEqualVector(t, b1.Vecs[i], b2.Vecs[i])
	}
}

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
	checkEqualBatch(t, bat, bat2)

	bat.Close()
}
