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

package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/encoding"
)

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

func Encode(ks ...interface{}) []byte {
	var buf bytes.Buffer

	for i := 0; i < len(ks); i++ {
		k := ks[i]
		switch v := k.(type) {
		case uint8:
			buf.WriteByte(byte(Uint8))
			buf.Write(encoding.EncodeUint8(v))
		case int:
			buf.WriteByte(byte(Int))
			data := pool.Get().([]byte)
			binary.BigEndian.PutUint64(data, uint64(v))
			buf.Write(data)
			pool.Put(data)
		case uint64:
			buf.WriteByte(byte(Uint64))
			data := pool.Get().([]byte)
			binary.BigEndian.PutUint64(data, uint64(v))
			buf.Write(data)
			pool.Put(data)
		case []byte:
			buf.WriteByte(byte(Bytes))
			buf.Write(encoding.EncodeUint32(uint32(len(v))))
			buf.Write(v)
		case string:
			buf.WriteByte(byte(String))
			buf.Write(encoding.EncodeUint32(uint32(len(v))))
			buf.WriteString(v)
		}
	}
	return buf.Bytes()
}

func Decode(data []byte) []interface{} {
	var ks []interface{}

	for len(data) > 0 {
		typ := data[0]
		data = data[1:]
		switch typ {
		case Int:
			ks = append(ks, int(binary.BigEndian.Uint64(data[:8])))
			data = data[8:]
		case Uint8:
			ks = append(ks, uint8(data[0]))
			data = data[1:]
		case Uint64:
			ks = append(ks, int(binary.BigEndian.Uint64(data[:8])))
			data = data[8:]
		case Bytes:
			n := encoding.DecodeUint32(data[:4])
			data = data[4:]
			ks = append(ks, data[:n])
			data = data[n:]
		case String:
			n := encoding.DecodeUint32(data[:4])
			data = data[4:]
			ks = append(ks, Bytes2String(data[:n]))
			data = data[n:]
		}
	}
	return ks
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

func Uint642String(v uint64) string {
	var buf bytes.Buffer
	data := pool.Get().([]byte)
	binary.BigEndian.PutUint64(data, v)
	buf.Write(data)
	pool.Put(data)
	return Bytes2String(buf.Bytes())
}

func String2Uint64(v string) (b uint64, err error) {
	if len(String2Bytes(v)) != 8 {
		return b, fmt.Errorf("invalid data, must 8 bytes, but %d", len(v))
	}
	return binary.BigEndian.Uint64(String2Bytes(v)), nil
}
