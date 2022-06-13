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

package txnbase

import (
	"bytes"
	"encoding/binary"
)

type KeyT = byte

const (
	KeyT_DBEntry KeyT = iota
	KeyT_TableEntry
	KeyT_SegmentEntry
	KeyT_BlockEntry

	KeyT_DataRow
)

var KeyEncoder = keyEncoder{}

type keyEncoder struct{}

func (s *keyEncoder) EncodeDB(db uint64) []byte {
	var w bytes.Buffer
	// Should not fail. No need to check err
	w.WriteByte(KeyT_DBEntry)
	_ = binary.Write(&w, binary.BigEndian, db)
	return w.Bytes()
}

func (s *keyEncoder) EncodeTable(db uint64, tb uint64) []byte {
	var w bytes.Buffer
	// Should not fail. No need to check err
	w.WriteByte(KeyT_TableEntry)
	_ = binary.Write(&w, binary.BigEndian, db)
	_ = binary.Write(&w, binary.BigEndian, tb)
	return w.Bytes()
}

func (s *keyEncoder) EncodeSegment(db, tb, seg uint64) []byte {
	var w bytes.Buffer
	// Should not fail. No need to check err
	_ = w.WriteByte(KeyT_SegmentEntry)
	_ = binary.Write(&w, binary.BigEndian, db)
	_ = binary.Write(&w, binary.BigEndian, tb)
	_ = binary.Write(&w, binary.BigEndian, seg)
	return w.Bytes()
}

func (s *keyEncoder) EncodeBlock(db, tb, seg, blk uint64) []byte {
	var w bytes.Buffer
	// Should not fail. No need to check err
	_ = w.WriteByte(KeyT_BlockEntry)
	_ = binary.Write(&w, binary.BigEndian, db)
	_ = binary.Write(&w, binary.BigEndian, tb)
	_ = binary.Write(&w, binary.BigEndian, seg)
	_ = binary.Write(&w, binary.BigEndian, blk)
	return w.Bytes()
}

func (s *keyEncoder) Decode(buf []byte) (kt KeyT, db, tb, seg, blk uint64) {
	var err error
	r := bytes.NewBuffer(buf)
	if kt, err = r.ReadByte(); err != nil {
		panic(err)
	}
	switch kt {
	case KeyT_DBEntry:
		if err = binary.Read(r, binary.BigEndian, &db); err != nil {
			panic(err)
		}
	case KeyT_TableEntry:
		if err = binary.Read(r, binary.BigEndian, &db); err != nil {
			panic(err)
		}
		if err = binary.Read(r, binary.BigEndian, &tb); err != nil {
			panic(err)
		}
	case KeyT_SegmentEntry:
		if err = binary.Read(r, binary.BigEndian, &db); err != nil {
			panic(err)
		}
		if err = binary.Read(r, binary.BigEndian, &tb); err != nil {
			panic(err)
		}
		if err = binary.Read(r, binary.BigEndian, &seg); err != nil {
			panic(err)
		}
	case KeyT_BlockEntry:
		if err = binary.Read(r, binary.BigEndian, &db); err != nil {
			panic(err)
		}
		if err = binary.Read(r, binary.BigEndian, &tb); err != nil {
			panic(err)
		}
		if err = binary.Read(r, binary.BigEndian, &seg); err != nil {
			panic(err)
		}
		if err = binary.Read(r, binary.BigEndian, &blk); err != nil {
			panic(err)
		}
	}
	return
}

func (s *keyEncoder) EncodeRow(db, tb, seg, blk uint64, offset uint32) []byte {
	var w bytes.Buffer
	// Should not fail. No need to check err
	_ = w.WriteByte(KeyT_DataRow)
	_ = binary.Write(&w, binary.BigEndian, db)
	_ = binary.Write(&w, binary.BigEndian, tb)
	_ = binary.Write(&w, binary.BigEndian, seg)
	_ = binary.Write(&w, binary.BigEndian, blk)
	_ = binary.Write(&w, binary.BigEndian, offset)
	return w.Bytes()
}

func (s *keyEncoder) DecodeRow(buf []byte) (db, tb, seg, blk uint64, offset uint32) {
	var err error
	r := bytes.NewBuffer(buf)
	if _, err = r.ReadByte(); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &db); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &tb); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &seg); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &blk); err != nil {
		panic(err)
	}
	if err = binary.Read(r, binary.BigEndian, &offset); err != nil {
		panic(err)
	}
	return
}
