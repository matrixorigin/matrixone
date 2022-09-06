package objectio

import (
	"fmt"
	mobat "github.com/matrixorigin/matrixone/pkg/container/batch"
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

func TestNewBlock(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	id := common.NextGlobalSeqNum()
	baseName := fmt.Sprintf("%d.blk", id)
	name := path.Join(dir, baseName)
	schema := catalog.MockSchemaAll(14, 3)
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.SegmentMaxBlocks = options.DefaultBlocksPerSegment
	data := catalog.MockBatch(schema, int(schema.BlockMaxRows*2))
	newbat := mobat.New(true, data.Attrs)
	newbat.Vecs = moengine.CopyToMoVectors(data.Vecs)
	ow, err := NewObjectWriter(name)
	assert.Nil(t, err)
	err = ow.Write(&common.ID{
		BlockID: id,
	}, newbat)
	assert.Nil(t, err)
	err = ow.WriteEnd()
	assert.Nil(t, err)
	err = ow.Sync()
	assert.Nil(t, err)
}
