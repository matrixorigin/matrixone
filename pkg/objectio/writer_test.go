package objectio

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

const (
	ModuleName = "ObjectIo"
)

func TestNewObjectWriter(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	id := common.NextGlobalSeqNum()
	name := fmt.Sprintf("%d.blk", id)
	schema := catalog.MockSchemaAll(14, 3)
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.SegmentMaxBlocks = options.DefaultBlocksPerSegment
	data := catalog.MockBatch(schema, int(schema.BlockMaxRows*2))
	bat := batch.New(true, data.Attrs)
	bat.Vecs = moengine.CopyToMoVectors(data.Vecs)

	objectWriter, err := NewObjectWriter(name)
	assert.Nil(t, err)
	err = objectWriter.Write(bat)
	assert.Nil(t, err)
	err = objectWriter.Write(bat)
	assert.Nil(t, err)
	extents, err := objectWriter.WriteEnd()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(extents))
	err = objectWriter.Sync(dir)
	assert.Nil(t, err)

	objectReader, _ := NewObjectReader(name, dir)
	idxs := make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 3
	vec, err := objectReader.Read(extents[0], idxs)
	assert.Nil(t, err)
	opts := new(containers.Options)
	opts.Capacity = int(schema.BlockMaxRows)
	allocator := stl.NewSimpleAllocator()
	opts.Allocator = allocator
	newbat := containers.NewBatch()
	for i, entry := range vec.Entries {
		vec := containers.MakeVector(data.Vecs[int(idxs[i])].GetType(), false, opts)
		r := bytes.NewBuffer(entry.Data)
		if _, err = vec.ReadFrom(r); err != nil {
			return
		}
		newbat.AddVector(data.Attrs[int(idxs[i])], vec)
	}
	assert.Equal(t, 3, len(newbat.Vecs))
}
