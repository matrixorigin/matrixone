package objectio

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ColumnBlock struct {
	meta *ColumnMeta
}

func NewColumnBlock(idx uint16) *ColumnBlock {
	meta := &ColumnMeta{
		idx:         idx,
		zoneMap:     &index.ZoneMap{},
		bloomFilter: &Extent{},
	}
	block := &ColumnBlock{
		meta: meta,
	}
	return block
}

func (cb *ColumnBlock) ShowMeta() ([]byte, error) {
	var (
		err    error
		buffer bytes.Buffer
	)
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.typ); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.alg); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.idx); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.location.Offset()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.location.Length()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.location.OriginSize()); err != nil {
		return nil, err
	}
	/*if err = binary.Write(&buffer, binary.BigEndian, cb.meta.zoneMap.GetMin()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.zoneMap.GetMax()); err != nil {
		return nil, err
	}*/
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.bloomFilter.Offset()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.bloomFilter.Length()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.bloomFilter.OriginSize()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}
	reserved := make([]byte, 32)
	if err = binary.Write(&buffer, binary.BigEndian, reserved); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
