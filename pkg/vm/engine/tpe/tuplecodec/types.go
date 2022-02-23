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
	SERIAL_TYPE_UINT64 byte = 1
	SERIAL_TYPE_STRING byte = 2
	SERIAL_TYPE_BYTES byte = 3
	SERIAL_TYPE_UNKNOWN byte = 255
)

//ValueSerializer for serializing the value
type ValueSerializer interface {
	SerializeValue(data []byte,value interface{})([]byte,*orderedcodec.EncodedItem,error)

	DeserializeValue(data []byte)([]byte,*orderedcodec.DecodedItem,error)
}

type DefaultValueSerializer struct {
	ValueSerializer
}