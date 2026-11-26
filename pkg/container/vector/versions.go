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

package vector

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// MarshalBinaryWithBufferV1 in version 1, vector.nulls.bitmap is v1
func (v *Vector) MarshalBinaryWithBufferV1(buf *bytes.Buffer) error {

	// write class
	buf.WriteByte(uint8(v.class))

	// write type
	data := types.EncodeType(&v.typ)
	buf.Write(data)

	// write length
	length := uint32(v.length)
	buf.Write(types.EncodeUint32(&length))

	// write dataLen, data
	dataLen := uint32(v.typ.TypeSize())
	if !v.IsConst() {
		dataLen *= uint32(v.length)
	} else if v.IsConstNull() {
		dataLen = 0
	}
	buf.Write(types.EncodeUint32(&dataLen))
	if dataLen > 0 {
		buf.Write(v.data[:dataLen])
	}

	// write areaLen, area
	areaLen := uint32(len(v.area))
	buf.Write(types.EncodeUint32(&areaLen))
	if areaLen > 0 {
		buf.Write(v.area)
	}

	// write nspLen, nsp
	nspData, err := v.nsp.ShowV1()
	if err != nil {
		return err
	}
	nspLen := uint32(len(nspData))
	buf.Write(types.EncodeUint32(&nspLen))
	if nspLen > 0 {
		buf.Write(nspData)
	}

	buf.Write(types.EncodeBool(&v.sorted))

	return nil
}

// UnmarshalBinaryV1 decodes the legacy encoding into memory allocated from
// v's pool, or references data without copying when v is read-only. v must
// not hold memory.
func (v *Vector) UnmarshalBinaryV1(data []byte) error {
	if len(data) > 0 && data[0] > CONSTANT {
		return moerr.NewInvalidInputNoCtx("invalid legacy vector representation")
	}
	if v.hasBackingStorage() {
		return moerr.NewInternalErrorNoCtx("cannot replace vector storage without Free")
	}
	// decoded aliases data and never escapes this call.
	var decoded Vector
	if err := decoded.unmarshalBinaryV1(data); err != nil {
		return err
	}
	if v.mp == nil {
		// Read-only: reference data without copying.
		*v = decoded
		return nil
	}
	return v.copyDecoded(&decoded)
}

// copyDecoded makes v an owned copy of decoded, a vector that aliases an
// encoding. v must be empty.
func (v *Vector) copyDecoded(decoded *Vector) (err error) {
	w := NewVec(decoded.typ, v.mp)
	defer func() {
		if err != nil {
			w.Free(v.mp)
		}
	}()
	w.class = decoded.class
	w.length = decoded.length
	w.sorted = decoded.sorted
	if len(decoded.data) > 0 {
		if w.data, err = w.alloc(v.mp, len(decoded.data)); err != nil {
			return err
		}
		copy(w.data, decoded.data)
	}
	if len(decoded.area) > 0 {
		if w.area, err = w.alloc(v.mp, len(decoded.area)); err != nil {
			return err
		}
		copy(w.area, decoded.area)
	}
	if err = w.nsp.InitWith(&decoded.nsp, v.mp); err != nil {
		return err
	}
	*v = *w
	return nil
}

func (v *Vector) unmarshalBinaryV1(data []byte) error {
	if len(data) < 1+types.TSize {
		return moerr.NewInternalErrorNoCtx("short legacy vector type")
	}
	typ, err := types.DecodeTypeChecked(data[1 : 1+types.TSize])
	if err != nil {
		return err
	}
	// read class
	v.class = int(data[0])
	data = data[1:]

	// read typ
	v.typ = typ
	data = data[types.TSize:]

	// read length
	v.length = int(types.DecodeUint32(data[:4]))
	// capacity is lenght, as this is a nocopy unmarshal.
	data = data[4:]

	// read data
	dataLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if dataLen > 0 {
		v.data = data[:dataLen]
		data = data[dataLen:]
	}

	// read area
	areaLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if areaLen > 0 {
		v.area = data[:areaLen]
		data = data[areaLen:]
	}

	// read nsp
	nspLen := types.DecodeUint32(data[:4])
	data = data[4:]
	if nspLen > 0 {
		// The null bitmap is private even for a read-only vector.
		if err := v.nsp.ReadNoCopyV1(bytes.Clone(data[:nspLen])); err != nil {
			return err
		}
		data = data[nspLen:]
	} else {
		v.nsp.Reset()
	}

	v.sorted = types.DecodeBool(data[:1])

	return nil
}
