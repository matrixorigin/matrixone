package dataio

import (
	"fmt"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
)

var SegmentFileMockFactory = func(dir string, id uint64) SegmentFile {
	return mockSegment(dir, id)
}

type mockBlockFile struct {
	NoopBlockFile
	id       uint64
	rows     uint32
	segFile  SegmentFile
	maxTs    uint64
	data     batch.IBatch
	logIndex *shard.Index
	ts       *gvec.Vector
}

type mockSegmentFile struct {
	NoopSegmentFile
	files  map[uint64]*mockBlockFile
	name   string
	sorted bool
}

func mockBlock(id uint64, bat batch.IBatch, segFile SegmentFile) *mockBlockFile {
	return &mockBlockFile{
		id:      id,
		segFile: segFile,
		data:    bat,
	}
}

func mockSegment(dir string, id uint64) *mockSegmentFile {
	name := fmt.Sprintf("%s-mock-%d", dir, id)
	return &mockSegmentFile{
		files: make(map[uint64]*mockBlockFile),
		name:  name,
	}
}

func (bf *mockBlockFile) Rows() uint32   { return bf.rows }
func (bf *mockBlockFile) IsSorted() bool { return bf.rows > 0 && bf.ts == nil }

func (bf *mockBlockFile) GetSegmentFile() SegmentFile { return bf.segFile }

func (bf *mockBlockFile) WriteData(bat batch.IBatch, logIndex *shard.Index, ts *gvec.Vector) error {
	bf.data = bat
	bf.rows = uint32(bat.Length())
	bf.logIndex = logIndex
	bf.ts = ts
	return nil
}

func (bf *mockBlockFile) LoadData() (bat batch.IBatch, err error) {
	bat = bf.data
	return
}

func (bf *mockBlockFile) GetMaxIndex() *shard.Index {
	return bf.logIndex
}

func (bf *mockBlockFile) GetTimeStamps() (ts *gvec.Vector, err error) {
	ts = bf.ts
	return
}

func (sf *mockSegmentFile) IsSorted() bool { return sf.sorted }
func (sf *mockSegmentFile) GetBlockFile(id uint64) BlockFile {
	bf := sf.files[id]
	if bf == nil {
		bf = mockBlock(id, nil, sf)
		sf.files[id] = bf
	}
	return bf
}

func (sf *mockSegmentFile) Destory() error {
	for _, bf := range sf.files {
		if err := bf.Destory(); err != nil {
			return err
		}
	}
	return nil
}
