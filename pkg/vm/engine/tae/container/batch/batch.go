// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"bytes"
	"errors"
	"fmt"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
)

var (
	_ IBatch = (*Batch)(nil)
)

func NewBatch(attrs []int, vecs []vector.IVector) (IBatch, error) {
	if len(attrs) != len(vecs) || len(vecs) == 0 {
		return nil, errors.New(fmt.Sprintf("invalid attrs and vectors length: %d %d", len(attrs), len(vecs)))
	}
	bat := &Batch{
		ClosedMask: roaring.NewBitmap(),
		Vecs:       vecs,
		Attrs:      attrs,
		AttrsMap:   make(map[int]int),
	}

	for i, idx := range attrs {
		bat.AttrsMap[idx] = i
	}
	if len(bat.AttrsMap) != len(attrs) {
		return nil, errors.New(fmt.Sprintf("len(bat.AttrsMap) != len(attrs): %d %d", len(bat.Attrs), len(attrs)))
	}

	return bat, nil
}

func (bat *Batch) GetAttrs() []int {
	return bat.Attrs
}

func (bat *Batch) Length() int {
	return bat.Vecs[len(bat.Vecs)-1].Length()
}

func (bat *Batch) IsReadonly() bool {
	return bat.Vecs[len(bat.Vecs)-1].IsReadonly()
}

func (bat *Batch) GetReaderByAttr(attr int) (container.IVectorReader, error) {
	vec, err := bat.GetVectorByAttr(attr)
	if err != nil {
		return nil, err
	}
	return vec.(container.IVectorReader), nil
}

func (bat *Batch) GetVectorByAttr(attr int) (vector.IVector, error) {
	pos, ok := bat.AttrsMap[attr]
	if !ok {
		return nil, BatNotFoundErr
	}
	bat.RLock()
	if bat.ClosedMask.Contains(uint64(pos)) {
		bat.RUnlock()
		return nil, BatAlreadyClosedErr
	}
	bat.RUnlock()
	return bat.Vecs[pos], nil
}

func (bat *Batch) IsVectorClosed(attr int) (bool, error) {
	pos, ok := bat.AttrsMap[attr]
	if !ok {
		return false, BatNotFoundErr
	}
	bat.RLock()
	defer bat.RUnlock()
	return bat.ClosedMask.Contains(uint64(pos)), nil
}

func (bat *Batch) CloseVector(attr int) error {
	pos, ok := bat.AttrsMap[attr]
	if !ok {
		return BatNotFoundErr
	}
	bat.Lock()
	defer bat.Unlock()
	if bat.ClosedMask.Contains(uint64(pos)) {
		return BatAlreadyClosedErr
	}
	err := bat.Vecs[pos].Close()
	if err != nil {
		return err
	}
	bat.ClosedMask.Add(uint64(pos))
	return nil
}

func (bat *Batch) Close() error {
	bat.Lock()
	defer bat.Unlock()
	if bat.ClosedMask.GetCardinality() == uint64(len(bat.Attrs)) {
		return nil
	}
	var err error
	for i := 0; i < len(bat.Attrs); i++ {
		if !bat.ClosedMask.Contains(uint64(i)) {
			if err = bat.Vecs[i].Close(); err != nil {
				return err
			}
			bat.ClosedMask.Add(uint64(i))
		}
	}
	return nil
}

func (bat *Batch) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	attrs := bat.GetAttrs()
	length := len(attrs)
	buf.Write(encoding.EncodeUint16(uint16(length)))
	for _, attr := range attrs {
		buf.Write(encoding.EncodeUint64(uint64(attr)))
		vec, err := bat.GetVectorByAttr(attr)
		if err != nil {
			return nil, err
		}
		vecType := vec.GetType()
		buf.Write(encoding.EncodeUint8(uint8(vecType)))
		vecBuf, err := vec.Marshal()
		if err != nil {
			return nil, err
		}
		vecLength := len(vecBuf)
		buf.Write(encoding.EncodeUint64(uint64(vecLength)))
		buf.Write(vecBuf)
	}
	return buf.Bytes(), nil
}

func (bat *Batch) Unmarshal(buf []byte) error {
	pos := 0
	attrLength := encoding.DecodeUint16(buf[pos : pos+2])
	pos += 2
	bat.Attrs = make([]int, attrLength)
	bat.Vecs = make([]vector.IVector, attrLength)
	for i := 0; i < int(attrLength); i++ {
		bat.Attrs[i] = int(encoding.DecodeUint64(buf[pos : pos+8]))
		pos += 8
		vecType := encoding.DecodeUint8(buf[pos : pos+1])
		pos += 1
		vecLength := encoding.DecodeUint64(buf[pos : pos+8])
		pos += 8
		switch vecType {
		case uint8(container.StdVec):
			bat.Vecs[i] = vector.NewEmptyStdVector()
		case uint8(container.StrVec):
			bat.Vecs[i] = vector.NewEmptyStrVector()
		}
		err := bat.Vecs[i].Unmarshal(buf[pos : pos+int(vecLength)])
		pos += int(vecLength)
		if err != nil {
			return err
		}
	}
	bat.ClosedMask = roaring.NewBitmap()
	bat.AttrsMap = make(map[int]int)
	for i, idx := range bat.Attrs {
		bat.AttrsMap[idx] = i
	}
	return nil
}
