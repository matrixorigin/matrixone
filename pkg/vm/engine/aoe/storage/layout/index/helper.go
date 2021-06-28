package index

import (
	"bytes"
	"fmt"
	"io"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"os"

	"github.com/pilosa/pilosa/roaring"
	// log "github.com/sirupsen/logrus"
)

var (
	DefaultRWHelper = new(RWHelper)
)

type RWHelper struct{}

func (h *RWHelper) WriteIndexes(indexes []Index) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.Write(encoding.EncodeInt16(int16(len(indexes))))
	if err != nil {
		return nil, err
	}
	for _, i := range indexes {
		_, err := buf.Write(encoding.EncodeUint16(i.Type()))
		if err != nil {
			return nil, err
		}
	}
	for _, i := range indexes {
		_, err := buf.Write(encoding.EncodeInt16(i.GetCol()))
		if err != nil {
			return nil, err
		}
	}
	for _, i := range indexes {
		ibuf, _ := i.Marshall()
		buf.Write(encoding.EncodeInt32(int32(len(ibuf))))
		buf.Write(ibuf)
	}

	return buf.Bytes(), nil
}

func (h *RWHelper) ReadIndexes(f os.File) (indexes []Index, err error) {
	twoBytes := make([]byte, 2)
	fourBytes := make([]byte, 4)
	_, err = f.Read(twoBytes)
	if err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	indexCnt := encoding.DecodeInt16(twoBytes)
	for i := 0; i < int(indexCnt); i++ {
		_, err := f.Read(twoBytes)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		indexType := encoding.DecodeUint16(twoBytes)
		switch indexType {
		case base.ZoneMap:
			idx := new(ZoneMapIndex)
			indexes = append(indexes, idx)
		default:
			panic("unsupported")
		}
	}
	for i := 0; i < int(indexCnt); i++ {
		_, err := f.Read(twoBytes)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
	}
	for i := 0; i < int(indexCnt); i++ {
		_, err := f.Read(fourBytes)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		length := encoding.DecodeInt32(fourBytes)
		buf := make([]byte, int(length))
		_, err = f.Read(buf)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		indexes[i].Unmarshall(buf)
	}
	return indexes, err
}

func (h *RWHelper) ReadIndexesMeta(f os.File) (meta *base.IndexesMeta, err error) {
	twoBytes := make([]byte, 2)
	fourBytes := make([]byte, 4)
	_, err = f.Read(twoBytes)
	if err != nil {
		panic(fmt.Sprintf("unexpect error: %s", err))
	}
	indexCnt := encoding.DecodeInt16(twoBytes)
	if indexCnt > 0 {
		meta = base.NewIndexesMeta()
	}
	for i := 0; i < int(indexCnt); i++ {
		_, err := f.Read(twoBytes)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		indexType := encoding.DecodeUint16(twoBytes)
		im := new(base.IndexMeta)
		im.Type = indexType
		im.Ptr = new(base.Pointer)
		im.Cols = roaring.NewBitmap()
		meta.Data = append(meta.Data, im)
	}
	for i := 0; i < int(indexCnt); i++ {
		_, err := f.Read(twoBytes)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		col := encoding.DecodeInt16(twoBytes)
		meta.Data[i].Cols.Add(uint64(col))
	}
	for i := 0; i < int(indexCnt); i++ {
		_, err := f.Read(fourBytes)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		length := encoding.DecodeInt32(fourBytes)
		meta.Data[i].Ptr.Len = uint64(length)
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
		meta.Data[i].Ptr.Offset = offset
		_, err = f.Seek(int64(length), io.SeekCurrent)
		if err != nil {
			panic(fmt.Sprintf("unexpect error: %s", err))
		}
	}
	// log.Info(meta.String())
	return meta, nil
}
