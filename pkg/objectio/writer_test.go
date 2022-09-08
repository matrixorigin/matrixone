package objectio

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
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
	id := common.NextGlobalSeqNum()
	baseName := fmt.Sprintf("%d.blk", id)
	name := path.Join(dir, baseName)
	schema := catalog.MockSchemaAll(14, 3)
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.SegmentMaxBlocks = options.DefaultBlocksPerSegment
	data := catalog.MockBatch(schema, int(schema.BlockMaxRows*2))
	bat := batch.New(true, data.Attrs)
	bat.Vecs = moengine.CopyToMoVectors(data.Vecs)

	ow, err := NewObjectWriter(name)
	assert.Nil(t, err)
	err = ow.Write(&common.ID{BlockID: id}, bat)
	assert.Nil(t, err)
	err = ow.WriteEnd()
	assert.Nil(t, err)
	err = ow.Sync(dir)
	assert.Nil(t, err)
}
