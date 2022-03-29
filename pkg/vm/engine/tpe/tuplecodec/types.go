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

package tuplecodec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

type TupleKey []byte
type TupleValue []byte

// Range for [startKey, endKey)
type Range struct {
	startKey TupleKey
	endKey TupleKey
}

type TupleKeyEncoder struct {
	oe *orderedcodec.OrderedEncoder
	tenantPrefix *TupleKey
}

type TupleKeyDecoder struct {
	od *orderedcodec.OrderedDecoder
	tenantPrefix *TupleKey
}

type TupleCodecHandler struct {
	tke *TupleKeyEncoder
	tkd *TupleKeyDecoder
}

func NewTupleCodecHandler(tenantID uint64) *TupleCodecHandler {
	tch := &TupleCodecHandler{
		tke: NewTupleKeyEncoder(tenantID),
		tkd: NewTupleKeyDecoder(tenantID),
	}
	tch.tkd.tenantPrefix = tch.tke.tenantPrefix
	return tch
}

func (tch *TupleCodecHandler) GetEncoder() *TupleKeyEncoder {
	return tch.tke
}

func (tch *TupleCodecHandler) GetDecoder() *TupleKeyDecoder  {
	return tch.tkd
}

// Tuple denotes the row of the relation
type Tuple interface {
	GetAttributeCount() (uint32, error)

	GetAttribute(colIdx uint32)(types.Type,string,error)

	IsNull(colIdx uint32) (bool,error)

	GetValue(colIdx uint32) (interface{},error)

	GetInt(colIdx uint32)(int,error)

	//others data type
}

type Tuples []Tuple

const (
	SERIAL_TYPE_NULL byte = 0
	SERIAL_TYPE_STRING byte = SERIAL_TYPE_NULL + 1
	SERIAL_TYPE_BYTES byte = SERIAL_TYPE_STRING + 1
	SERIAL_TYPE_BOOL byte = SERIAL_TYPE_BYTES + 1
	SERIAL_TYPE_INT8 byte = SERIAL_TYPE_BOOL + 1
	SERIAL_TYPE_INT16 byte = SERIAL_TYPE_INT8 + 1
	SERIAL_TYPE_INT32 byte = SERIAL_TYPE_INT16 + 1
	SERIAL_TYPE_INT64 byte = SERIAL_TYPE_INT32 + 1
	SERIAL_TYPE_UINT8 byte = SERIAL_TYPE_INT64 + 1
	SERIAL_TYPE_UINT16 byte = SERIAL_TYPE_UINT8 + 1
	SERIAL_TYPE_UINT32 byte = SERIAL_TYPE_UINT16 + 1
	SERIAL_TYPE_UINT64 byte = SERIAL_TYPE_UINT32 + 1
	SERIAL_TYPE_FLOAT32 byte = SERIAL_TYPE_UINT64 + 1
	SERIAL_TYPE_FLOAT64 byte = SERIAL_TYPE_FLOAT32 + 1
	SERIAL_TYPE_DATE byte = SERIAL_TYPE_FLOAT64 + 1
	SERIAL_TYPE_DATETIME byte = SERIAL_TYPE_DATE + 1
	SERIAL_TYPE_UNKNOWN byte = 255
)