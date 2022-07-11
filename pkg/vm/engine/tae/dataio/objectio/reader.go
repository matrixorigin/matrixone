package objectio

import (
	"bytes"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"os"
)

type Reader struct {
	block blockFile
	fs    tfs.FS
}

func NewReader(fs tfs.FS) *Reader {
	return &Reader{
		fs: fs,
	}
}

func (r *Reader) LoadIndexMeta(id *common.ID) (any, error) {
	name := EncodeMetaIndexName(id, r.fs)
	f, err := r.fs.OpenFile(name, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	size := f.Stat().Size()
	buf := make([]byte, size)
	_, err = f.Read(buf)
	if err != nil {
		return nil, err
	}
	indices := indexwrapper.NewEmptyIndicesMeta()
	if err = indices.Unmarshal(buf); err != nil {
		return nil, err
	}
	return indices, nil
}

func (r *Reader) LoadDeletes(id *common.ID) (mask *roaring.Bitmap, err error) {
	name := EncodeDeleteName(id, r.fs)
	f, err := r.fs.OpenFile(name, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	size := f.Stat().Size()
	if size == 0 {
		return
	}
	node := common.GPool.Alloc(uint64(size))
	defer common.GPool.Free(node)
	if _, err = f.Read(node.Buf[:size]); err != nil {
		return
	}
	mask = roaring.New()
	err = mask.UnmarshalBinary(node.Buf[:size])
	return
}

func (r *Reader) LoadABlkColumns(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	opts *containers.Options) (bat *containers.Batch, err error) {
	bat = containers.NewBatch()
	var f common.IRWFile
	for i, colBlk := range r.block.columns {
		if f, err = colBlk.OpenDataFile(); err != nil {
			return
		}
		defer f.Unref()
		vec := containers.MakeVector(colTypes[i], nullables[i], opts)
		bat.AddVector(colNames[i], vec)
		size := f.Stat().Size()
		if size == 0 {
			continue
		}
		buf := make([]byte, size)
		if _, err = f.Read(buf); err != nil {
			return
		}
		r := bytes.NewBuffer(buf)
		if _, err = vec.ReadFrom(r); err != nil {
			return
		}
		bat.Vecs[i] = vec
	}
	return
}

func (r *Reader) LoadUpdates() (masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]any) {
	tool := containers.NewCodecTool()
	defer tool.Close()
	for i, cb := range r.block.columns {
		tool.Reset()
		uf, err := cb.OpenUpdateFile()
		if err != nil {
			panic(any(err))
		}
		defer uf.Unref()
		if uf.Stat().OriginSize() == 0 {
			continue
		}
		if err := tool.ReadFromFile(uf, nil); err != nil {
			panic(any(err))
		}
		buf := tool.Get(0)
		typ := types.DecodeType(buf)
		nullable := containers.GetValueFrom[bool](tool, 1)
		mask := roaring.New()
		buf = tool.Get(2)
		if err = mask.UnmarshalBinary(buf); err != nil {
			panic(any(err))
		}
		buf = tool.Get(3)
		r := bytes.NewBuffer(buf)
		vec := containers.MakeVector(typ, nullable)
		if _, err = vec.ReadFrom(r); err != nil {
			panic(any(err))
		}
		defer vec.Close()
		val := make(map[uint32]any)
		it := mask.Iterator()
		pos := 0
		for it.HasNext() {
			row := it.Next()
			v := vec.Get(pos)
			val[row] = v
			pos++
		}
		if masks == nil {
			masks = make(map[uint16]*roaring.Bitmap)
			vals = make(map[uint16]map[uint32]any)
		}
		vals[uint16(i)] = val
		masks[uint16(i)] = mask
	}
	return
}
