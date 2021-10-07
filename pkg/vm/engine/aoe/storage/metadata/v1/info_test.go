package metadata

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInfo(t *testing.T) {
	dir := "/tmp/tbl"
	os.RemoveAll(dir)
	mu := &sync.RWMutex{}
	info := MockInfo(mu, blockRowCount, segmentBlockCount)
	assert.NotNil(t, info)
	info1 := NewMetaInfo(mu, &Configuration{
		BlockMaxRows:     2,
		SegmentMaxBlocks: 2,
	})
	schema1 := MockSchema(2)
	schema1.Name = "tbl1"
	schema2 := MockVarCharSchema(2)
	schema2.Name = "tbl2"
	tbl1, err := info1.CreateTable(NextGlobalSeqNum(), schema1)
	assert.Nil(t, err)
	_, err = info1.ReferenceTable(tbl1.ID)
	assert.NotNil(t, err)
	tbl2, err := info1.CreateTable(NextGlobalSeqNum(), schema2)
	assert.Nil(t, err)
	_, err = info1.CreateTable(NextGlobalSeqNum(), nil)
	assert.NotNil(t, err)
	assert.Equal(t, tbl1.GetBoundState(), Standalone)
	err = info1.RegisterTable(tbl1)
	assert.Nil(t, err)
	assert.NotNil(t, info1.RegisterTable(tbl1))
	delete(info1.Tables, tbl1.ID)
	assert.NotNil(t, info1.RegisterTable(tbl1))
	delete(info1.NameMap, "tbl1")
	assert.NotNil(t, info1.RegisterTable(tbl1))
	info1.Tables[tbl1.ID] = tbl1
	info1.NameMap["tbl1"] = tbl1.ID
	assert.Equal(t, tbl1.GetBoundState(), Attached)
	err = info1.RegisterTable(tbl2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(info1.TableNames()))
	assert.Equal(t, map[uint64]uint64{tbl1.ID: tbl1.ID, tbl2.ID: tbl2.ID}, info1.TableIDs())
	assert.Equal(t, 2, len(info1.GetTablesByNamePrefix("tbl")))
	assert.Panics(t, func() {
		info1.GetTableId()
	})

	_, err = info1.SoftDeleteTable("xxxx", NextGlobalSeqNum())
	assert.NotNil(t, err)
	_, err = info1.SoftDeleteTable("tbl1", NextGlobalSeqNum())
	assert.Nil(t, err)
	_, err = info1.SoftDeleteTable("tbl1", NextGlobalSeqNum())
	assert.NotNil(t, err)
	info1.NameMap["tbl1"] = tbl1.ID
	_, err = info1.SoftDeleteTable("tbl1", NextGlobalSeqNum())
	assert.NotNil(t, err)
	_, err = info1.ReferenceTableByName("tbl1")
	assert.NotNil(t, err)
	_, err = info1.ReferenceTable(tbl1.ID)
	assert.Nil(t, err)
	_, err = info1.ReferenceTable(tbl2.ID)
	assert.Nil(t, err)
	_, err = info1.ReferenceTableByName("tbl2")
	assert.Nil(t, err)

	_, err = info1.ReferenceBlock(3, 0, 0)
	assert.NotNil(t, err)
	_, err = info1.ReferenceBlock(1, 0, 0)
	assert.NotNil(t, err)
	_, err = info1.TableSegmentIDs(3)
	assert.NotNil(t, err)
	_, err = info1.TableSegmentIDs(1)
	assert.Nil(t, err)
	_, err = info1.TableSegmentIDs(1, NowMicro())
	assert.Nil(t, err)

	assert.Equal(t, 1, len(info1.GetTablesByNamePrefix("tbl")))
	assert.Equal(t, 1, len(info1.TableNames()))
	assert.Equal(t, 1, len(info1.TableNames(NowMicro())))
	assert.Equal(t, 1, len(info1.TableIDs()))
	assert.Equal(t, 1, len(info1.TableIDs(NowMicro())))

	assert.Equal(t, 4, len(strings.Split(info1.String(), "\n")))
	assert.Equal(t, "2", info1.GetLastFileName())
	assert.Equal(t, "3", info1.GetFileName())
	assert.Equal(t, ResInfo, info1.GetResourceType())

	//buf := bytes.NewBuffer(make([]byte, 10000))
	//bytes.NewReader()
	f, _ := os.Create("/tmp/tbl")
	assert.Nil(t, info1.Serialize(f))
	assert.Nil(t, f.Close())
	assert.Equal(t, 1, len(info1.Copy(CopyCtx{
		Ts:       NowMicro(),
		Attached: false,
	}).TableNames()))
	assert.Equal(t, 1, len(info1.Copy(CopyCtx{
		Attached: false,
	}).TableNames()))
	var info3 MetaInfo
	f, _ = os.Open("/tmp/tbl")
	_, err = info3.ReadFrom(f)
	assert.Nil(t, err)
	assert.Nil(t, f.Close())

	json, err := info3.MarshalJSON()
	assert.Nil(t, err)
	buf := make([]byte, 296)
	f, _ = os.Open("/tmp/tbl")
	n, err := f.Read(buf)
	assert.Nil(t, err)
	assert.Nil(t, f.Close())
	assert.Equal(t, n, 296)
	assert.Nil(t, err)
	assert.Equal(t, json, buf)
	var info4 MetaInfo
	assert.Nil(t, info4.Unmarshal(buf))
	assert.Equal(t, len(info4.Tables), len(info3.Tables))
	assert.NotNil(t, info4.Unmarshal(nil))

	assert.NotNil(t, info1.UpdateCheckpoint(info1.CheckPoint-1))
	assert.Nil(t, info1.UpdateCheckpoint(info1.CheckPoint+1))
	info1.UpdateCheckpointTime(NowMicro())
	info1.UpdateCheckpointTime(NowMicro() - 10)
	_, err = info1.CreateTableFromTableInfo(&aoe.TableInfo{}, dbi.TableOpCtx{})
	assert.NotNil(t, err)
	_, err = info1.CreateTableFromTableInfo(&aoe.TableInfo{Name: "tbl1", Columns: []aoe.ColumnInfo{{Name: "xxxx", Type: types.Type{}}}}, dbi.TableOpCtx{})
	assert.Nil(t, err)
	_, err = info1.CreateTableFromTableInfo(&aoe.TableInfo{Name: "tbl1", Columns: []aoe.ColumnInfo{{Name: "xxxx", Type: types.Type{}}}}, dbi.TableOpCtx{})
	assert.NotNil(t, err)
}
