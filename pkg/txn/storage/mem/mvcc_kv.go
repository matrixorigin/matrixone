// Copyright 2021 - 2022 Matrix Origin
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

package mem

import (
	"bytes"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// MVCCKV mvcc kv based on KV
type MVCCKV struct {
	kv *KV
}

// NewMVCCKV create a mvcc based kv
func NewMVCCKV() *MVCCKV {
	return &MVCCKV{kv: NewKV()}
}

// Set set mvcc key value
func (mkv *MVCCKV) Set(rawKey []byte, ts timestamp.Timestamp, value []byte) {
	mkv.kv.Set(encodeMVCCKey(rawKey, ts), value)
}

// Get returns the value of a key in the specified version
func (mkv *MVCCKV) Get(rawKey []byte, ts timestamp.Timestamp) ([]byte, bool) {
	return mkv.kv.Get(encodeMVCCKey(rawKey, ts))
}

// Delete delete the value of a key in the specified version
func (mkv *MVCCKV) Delete(rawKey []byte, ts timestamp.Timestamp) bool {
	ok := mkv.kv.Delete(encodeMVCCKey(rawKey, ts))
	return ok
}

// AscendRange iter in [rawkey:from, rawkey:to)
func (mkv *MVCCKV) AscendRange(rawKey []byte, from timestamp.Timestamp, to timestamp.Timestamp,
	fn func([]byte, timestamp.Timestamp)) {
	fromMVCC := encodeMVCCKey(rawKey, from)
	toMVCC := encodeMVCCKey(rawKey, to)

	mkv.kv.AscendRange(fromMVCC, toMVCC, func(key, value []byte) bool {
		k, ts := decodeMVCCKey(key)
		if bytes.Equal(rawKey, k) {
			fn(value, ts)
		}
		return true
	})
}

func encodeMVCCKey(rawKey []byte, ts timestamp.Timestamp) []byte {
	buffer := buf.NewByteBuf(len(rawKey) + 12)
	buffer.MustWrite(rawKey)
	buffer.WriteInt64(ts.PhysicalTime)
	buffer.WriteUint32(ts.LogicalTime)
	_, v := buffer.ReadAll()
	return v
}

func decodeMVCCKey(mvccKey []byte) ([]byte, timestamp.Timestamp) {
	n := len(mvccKey)
	rawKey := mvccKey[:n-12]
	return rawKey, timestamp.Timestamp{
		PhysicalTime: buf.Byte2Int64(mvccKey[n-12:]),
		LogicalTime:  buf.Byte2Uint32(mvccKey[n-4:]),
	}
}
