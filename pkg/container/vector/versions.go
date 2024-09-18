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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func (v *Vector) MarshalBinaryV1() ([]byte, error) {
	var buf bytes.Buffer
	err := v.MarshalBinaryWithBuffer(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

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
	nspData, err := v.nsp.Show()
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
