package mockio

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	idxCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
)

type blockFile struct {
	common.RefHelper
	seg     file.Segment
	rows    uint32
	id      uint64
	ts      uint64
	columns []*columnBlock
	deletes *deletesFile
	indexMeta *dataFile
}

func newBlock(id uint64, seg file.Segment, colCnt int, indexCnt map[int]int) *blockFile {
	bf := &blockFile{
		seg:     seg,
		id:      id,
		columns: make([]*columnBlock, colCnt),
	}
	bf.deletes = newDeletes(bf)
	bf.indexMeta = newData(nil)
	bf.OnZeroCB = bf.close
	for i, _ := range bf.columns {
		cnt := 0
		if indexCnt != nil {
			cnt = indexCnt[i]
		}
		bf.columns[i] = newColumnBlock(bf, cnt)
	}
	bf.Ref()
	return bf
}

func (bf *blockFile) Fingerprint() *common.ID {
	return &common.ID{
		BlockID: bf.id,
	}
}

func (bf *blockFile) close() {
	bf.Close()
	bf.Destory()
}

func (bf *blockFile) WriteRows(rows uint32) (err error) {
	bf.rows = rows
	return nil
}

func (bf *blockFile) ReadRows() uint32 {
	return bf.rows
}

func (bf *blockFile) WriteTS(ts uint64) (err error) {
	bf.ts = ts
	return
}

func (bf *blockFile) ReadTS() (ts uint64, err error) {
	ts = bf.ts
	return
}

func (bf *blockFile) WriteDeletes(buf []byte) (err error) {
	_, err = bf.deletes.Write(buf)
	return
}

func (bf *blockFile) ReadDeletes(buf []byte) (err error) {
	_, err = bf.deletes.Read(buf)
	return
}

func (bf *blockFile) WriteIndexMeta(buf []byte) (err error) {
	_, err = bf.indexMeta.Write(buf)
	return
}

func (bf *blockFile) LoadIndexMeta() (*idxCommon.IndicesMeta, error) {
	size := bf.indexMeta.Stat().Size()
	buf := make([]byte, size)
	_, err := bf.indexMeta.Read(buf)
	if err != nil {
		return nil, err
	}
	indices := idxCommon.NewEmptyIndicesMeta()
	if err = indices.Unmarshal(buf); err != nil {
		return nil, err
	}
	return indices, nil
}

func (bf *blockFile) OpenColumn(colIdx int) (colBlk file.ColumnBlock, err error) {
	if colIdx >= len(bf.columns) {
		err = file.ErrInvalidParam
		return
	}
	bf.columns[colIdx].Ref()
	colBlk = bf.columns[colIdx]
	return
}

func (bf *blockFile) Close() error {
	return nil
}

func (bf *blockFile) Destory() {
	logutil.Infof("Destoring Blk %d @ TS %d", bf.id, bf.ts)
	for _, cb := range bf.columns {
		cb.Unref()
	}
	bf.columns = nil
	bf.deletes = nil
}

func (bf *blockFile) Sync() error { return nil }

func (bf *blockFile) LoadIBatch(colTypes []types.Type, maxRow uint32) (bat batch.IBatch, err error) {
	attrs := make([]int, len(bf.columns))
	vecs := make([]vector.IVector, len(attrs))
	var f common.IRWFile
	for i, colBlk := range bf.columns {
		if f, err = colBlk.OpenDataFile(); err != nil {
			return
		}
		defer f.Unref()
		size := f.Stat().Size()
		buf := make([]byte, size)
		if _, err = f.Read(buf); err != nil {
			return
		}
		vec := vector.NewVector(colTypes[i], uint64(maxRow))
		if err = vec.Unmarshal(buf); err != nil {
			return
		}
		vecs[i] = vec
		attrs[i] = i
	}
	bat, err = batch.NewBatch(attrs, vecs)
	return
}

func (bf *blockFile) LoadBatch(attrs []string, colTypes []types.Type) (bat *gbat.Batch, err error) {
	bat = gbat.New(true, attrs)
	var f common.IRWFile
	for i, colBlk := range bf.columns {
		if f, err = colBlk.OpenDataFile(); err != nil {
			return
		}
		defer f.Unref()
		size := f.Stat().Size()
		buf := make([]byte, size)
		if _, err = f.Read(buf); err != nil {
			return
		}
		vec := gvec.New(colTypes[i])
		if err = vec.Read(buf); err != nil {
			return
		}
		bat.Vecs[i] = vec
	}
	return
}

func (bf *blockFile) WriteColumnVec(ts uint64, colIdx int, vec *gvec.Vector) (err error) {
	cb, err := bf.OpenColumn(colIdx)
	if err != nil {
		return err
	}
	defer cb.Close()
	cb.WriteTS(ts)
	buf, err := vec.Show()
	if err != nil {
		return err
	}
	err = cb.WriteData(buf)
	return
}

func (bf *blockFile) WriteBatch(bat *gbat.Batch, ts uint64) (err error) {
	if err = bf.WriteTS(ts); err != nil {
		return
	}
	if err = bf.WriteRows(uint32(gvec.Length(bat.Vecs[0]))); err != nil {
		return
	}
	for colIdx := range bat.Attrs {
		if err = bf.WriteColumnVec(ts, colIdx, bat.Vecs[colIdx]); err != nil {
			return
		}
	}
	return
}

func (bf *blockFile) WriteIBatch(bat batch.IBatch, ts uint64, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]interface{}, deletes *roaring.Bitmap) (err error) {
	attrs := bat.GetAttrs()
	var w bytes.Buffer
	if deletes != nil {
		if _, err = deletes.WriteTo(&w); err != nil {
			return
		}
	}
	if err = bf.WriteTS(ts); err != nil {
		return err
	}
	for _, colIdx := range attrs {
		cb, err := bf.OpenColumn(colIdx)
		if err != nil {
			return err
		}
		defer cb.Close()
		cb.WriteTS(ts)
		vec, err := bat.GetVectorByAttr(colIdx)
		if err != nil {
			return err
		}
		updates := vals[uint16(colIdx)]
		if updates != nil {
			w.Reset()
			mask := masks[uint16(colIdx)]
			if _, err = mask.WriteTo(&w); err != nil {
				return err
			}
			col := gvec.New(vec.GetDataType())
			it := mask.Iterator()
			for it.HasNext() {
				row := it.Next()
				v := updates[row]
				if err = gvec.Append(col, v); err != nil {
					return err
				}
			}
			buf, err := col.Show()
			if err != nil {
				return err
			}
			w.Write(buf)
			if err = cb.WriteUpdates(w.Bytes()); err != nil {
				return err
			}
		}
		w.Reset()
		buf, err := vec.Marshal()
		if err != nil {
			return err
		}
		if err = cb.WriteData(buf); err != nil {
			return err
		}
	}
	return
}
