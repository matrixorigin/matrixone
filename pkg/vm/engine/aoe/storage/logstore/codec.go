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

package logstore

import "encoding/binary"

func UnmarshallEntryType(buf []byte) EntryType {
	return binary.BigEndian.Uint16(buf)
}

func UnmarshallEntrySize(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func MarshallEntryTypeWithBuf(buf []byte, typ EntryType) {
	binary.BigEndian.PutUint16(buf, typ)
}

func MarshallEntrySizeWithBuf(buf []byte, size uint32) {
	binary.BigEndian.PutUint32(buf, size)
}

func MarshallEntryType(typ EntryType) []byte {
	buf := make([]byte, EntryTypeSize)
	binary.BigEndian.PutUint16(buf, typ)
	return buf
}

func MarshallEntrySize(size uint32) []byte {
	buf := make([]byte, EntrySizeSize)
	binary.BigEndian.PutUint32(buf, size)
	return buf
}
