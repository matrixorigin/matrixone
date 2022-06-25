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

package driver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
)

func EncodeTable(tbl aoe.TableInfo) ([]byte, error) {
	return encoding.Encode(tbl)
}

func DecodeTable(data []byte) (aoe.TableInfo, error) {
	var tbl aoe.TableInfo

	err := encoding.Decode(data, &tbl)
	return tbl, err
}
func EncodeIndex(idx aoe.IndexInfo) ([]byte, error) {
	return encoding.Encode(idx)
}

func DecodeIndex(data []byte) (aoe.IndexInfo, error) {
	var idx aoe.IndexInfo

	err := encoding.Decode(data, &idx)
	return idx, err
}

var pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 8)
	},
}

func EncodeKey(ks ...interface{}) []byte {
	var buf bytes.Buffer

	for i := 0; i < len(ks); i++ {
		k := ks[i]
		switch v := k.(type) {
		case uint8:
			buf.Write(encoding.EncodeUint8(v))
		case int:
			data := pool.Get().([]byte)
			binary.BigEndian.PutUint64(data, uint64(v))
			buf.Write(data)
			pool.Put(data)
		case uint64:
			data := pool.Get().([]byte)
			binary.BigEndian.PutUint64(data, v)
			buf.Write(data)
			pool.Put(data)
		case []byte:
			buf.Write(v)
		case string:
			buf.WriteString(v)
		}
	}

	return buf.Bytes()
}

func String2Bytes(v string) []byte {
	if v == "" {
		return nil
	}
	return unsafe.Slice(
		(*byte)(unsafe.Pointer(
			(*reflect.StringHeader)(unsafe.Pointer(&v)).Data,
		)),
		len(v),
	)
}

func Bytes2String(v []byte) string {
	return *(*string)(unsafe.Pointer(&v))
}

func Uint642Bytes(v uint64) []byte {
	var buf bytes.Buffer
	data := pool.Get().([]byte)
	binary.BigEndian.PutUint64(data, v)
	buf.Write(data)
	pool.Put(data)
	return buf.Bytes()
}

func Bytes2Uint64(v []byte) (b uint64, err error) {
	if len(v) != 8 {
		return b, fmt.Errorf("invalid data, must 8 bytes, but %d", len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}
