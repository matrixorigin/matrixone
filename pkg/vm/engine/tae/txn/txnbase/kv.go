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
	w.WriteByte(KeyT_DBEntry)
	binary.Write(&w, binary.BigEndian, db)
	return w.Bytes()
}

func (s *keyEncoder) EncodeTable(db uint64, tb uint64) []byte {
	var w bytes.Buffer
	w.WriteByte(KeyT_TableEntry)
	binary.Write(&w, binary.BigEndian, db)
	binary.Write(&w, binary.BigEndian, tb)
	return w.Bytes()
}

func (s *keyEncoder) EncodeSegment(db, tb, seg uint64) []byte {
	var w bytes.Buffer
	w.WriteByte(KeyT_SegmentEntry)
	binary.Write(&w, binary.BigEndian, db)
	binary.Write(&w, binary.BigEndian, tb)
	binary.Write(&w, binary.BigEndian, seg)
	return w.Bytes()
}

func (s *keyEncoder) EncodeBlock(db, tb, seg, blk uint64) []byte {
	var w bytes.Buffer
	w.WriteByte(KeyT_BlockEntry)
	binary.Write(&w, binary.BigEndian, db)
	binary.Write(&w, binary.BigEndian, tb)
	binary.Write(&w, binary.BigEndian, seg)
	binary.Write(&w, binary.BigEndian, blk)
	return w.Bytes()
}

func (s *keyEncoder) Decode(buf []byte) (kt KeyT, db, tb, seg, blk uint64) {
	r := bytes.NewBuffer(buf)
	kt, _ = r.ReadByte()
	switch kt {
	case KeyT_DBEntry:
		binary.Read(r, binary.BigEndian, &db)
	case KeyT_TableEntry:
		binary.Read(r, binary.BigEndian, &db)
		binary.Read(r, binary.BigEndian, &tb)
	case KeyT_SegmentEntry:
		binary.Read(r, binary.BigEndian, &db)
		binary.Read(r, binary.BigEndian, &tb)
		binary.Read(r, binary.BigEndian, &seg)
	case KeyT_BlockEntry:
		binary.Read(r, binary.BigEndian, &db)
		binary.Read(r, binary.BigEndian, &tb)
		binary.Read(r, binary.BigEndian, &seg)
		binary.Read(r, binary.BigEndian, &blk)
	}
	return
}

func (s *keyEncoder) EncodeRow(db, tb, seg, blk uint64, offset uint32) []byte {
	var w bytes.Buffer
	w.WriteByte(KeyT_DataRow)
	binary.Write(&w, binary.BigEndian, db)
	binary.Write(&w, binary.BigEndian, tb)
	binary.Write(&w, binary.BigEndian, seg)
	binary.Write(&w, binary.BigEndian, blk)
	binary.Write(&w, binary.BigEndian, offset)
	return w.Bytes()
}

func (s *keyEncoder) DecodeRow(buf []byte) (db, tb, seg, blk uint64, offset uint32) {
	r := bytes.NewBuffer(buf)
	r.ReadByte()
	binary.Read(r, binary.BigEndian, &db)
	binary.Read(r, binary.BigEndian, &tb)
	binary.Read(r, binary.BigEndian, &seg)
	binary.Read(r, binary.BigEndian, &blk)
	binary.Read(r, binary.BigEndian, &offset)
	return
}
