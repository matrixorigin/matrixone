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

package logservicedriver

import (
	"bytes"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

type CmdType uint8

const (
	Cmd_Invalid CmdType = iota
	Cmd_Normal
	Cmd_SkipDSN
)

const (
	IOET_WALRecord_V1 uint16 = 1
	IOET_WALRecord    uint16 = 1000

	IOET_WALRecord_CurrVer = IOET_WALRecord_V1
)

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALRecord,
			Version: IOET_WALRecord_V1,
		},
		func(a any) ([]byte, error) {
			return a.(*baseEntry).Marshal()
		},
		func(b []byte) (any, error) {
			record := new(baseEntry)
			err := record.Unmarshal(b)
			return record, err
		},
	)
}

type Meta struct {
	cmdType     CmdType
	appended    uint64
	addr        map[uint64]uint64
	payloadSize uint64
}

func newMeta() Meta {
	return Meta{addr: make(map[uint64]uint64), cmdType: Cmd_Normal}
}

func (m *Meta) GetAddr() map[uint64]uint64 {
	return m.addr
}

func (m *Meta) AddAddr(l uint64, s uint64) {
	if m.addr == nil {
		m.addr = make(map[uint64]uint64)
	}
	m.addr[l] = s
}

func (m *Meta) SetType(t CmdType) {
	m.cmdType = t
}
func (m *Meta) GetType() CmdType {
	return m.cmdType
}
func (m *Meta) SetAppended(appended uint64) {
	m.appended = appended
}
func (m *Meta) GetMinLsn() uint64 {
	min := uint64(0)
	min = math.MaxUint64
	for lsn := range m.addr {
		if lsn < min {
			min = lsn
		}
	}
	return min
}
func (m *Meta) GetMaxLsn() uint64 {
	max := uint64(0)
	for lsn := range m.addr {
		if lsn > max {
			max = lsn
		}
	}
	return max
}
func (m *Meta) WriteTo(w io.Writer) (n int64, err error) {
	cmdType := uint8(m.cmdType)
	if _, err = w.Write(types.EncodeUint8(&cmdType)); err != nil {
		return
	}
	n += 1
	if _, err = w.Write(types.EncodeUint64(&m.appended)); err != nil {
		return
	}
	n += 8
	length := uint16(len(m.addr))
	if _, err = w.Write(types.EncodeUint16(&length)); err != nil {
		return
	}
	n += 2
	for lsn, offset := range m.addr {
		if _, err = w.Write(types.EncodeUint64(&lsn)); err != nil {
			return
		}
		n += 8
		if _, err = w.Write(types.EncodeUint64(&offset)); err != nil {
			return
		}
		n += 8
	}
	if _, err = w.Write(types.EncodeUint64(&m.payloadSize)); err != nil {
		return
	}
	n += 8
	return
}

func (m *Meta) ReadFrom(r io.Reader) (n int64, err error) {
	cmdType := uint8(0)
	if _, err = r.Read(types.EncodeUint8(&cmdType)); err != nil {
		return
	}
	m.cmdType = CmdType(cmdType)
	n += 1
	if _, err = r.Read(types.EncodeUint64(&m.appended)); err != nil {
		return
	}
	n += 8
	length := uint16(0)
	if _, err = r.Read(types.EncodeUint16(&length)); err != nil {
		return
	}
	n += 2
	m.addr = make(map[uint64]uint64)
	for i := 0; i < int(length); i++ {
		lsn := uint64(0)
		if _, err = r.Read(types.EncodeUint64(&lsn)); err != nil {
			return
		}
		n += 8
		offset := uint64(0)
		if _, err = r.Read(types.EncodeUint64(&offset)); err != nil {
			return
		}
		n += 8
		m.addr[lsn] = offset
	}
	if _, err = r.Read(types.EncodeUint64(&m.payloadSize)); err != nil {
		return
	}
	n += 8
	return
}

func (m *Meta) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := m.ReadFrom(bbuf)
	return err
}

func (m *Meta) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = m.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

type baseEntry struct {
	Meta
	EntryType, Version uint16
	entries            []*entry.Entry
	cmd                ReplayCmd
	payload            []byte
}

func (r *baseEntry) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = r.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (r *baseEntry) WriteTo(w io.Writer) (n int64, err error) {
	r.EntryType = IOET_WALRecord
	r.Version = IOET_WALRecord_CurrVer
	if _, err = w.Write(types.EncodeUint16(&r.EntryType)); err != nil {
		return 0, err
	}
	n += 2
	if _, err = w.Write(types.EncodeUint16(&r.Version)); err != nil {
		return 0, err
	}
	n += 2
	n1, err := r.Meta.WriteTo(w)
	if err != nil {
		return n, err
	}
	n += n1
	switch r.Meta.cmdType {
	case Cmd_Normal:
		for _, e := range r.entries {
			n1, err = e.WriteTo(w)
			if err != nil {
				return
			}
			n += n1
		}
	case Cmd_SkipDSN:
		n1, err = r.cmd.WriteTo(w)
		if err != nil {
			return
		}
		n += n1
	default:
		panic("invalid type")
	}
	return
}

func (r *baseEntry) ReadFrom(reader io.Reader) (n int64, err error) {
	n1, err := r.Meta.ReadFrom(reader)
	if err != nil {
		return 0, err
	}
	n += n1
	if r.cmdType == Cmd_SkipDSN {
		r.cmd = NewEmptyReplayCmd()
		n1, err = r.cmd.ReadFrom(reader)
		if err != nil {
			return 0, err
		}
		n += n1
	}
	return
}

func (r *baseEntry) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := r.ReadFrom(bbuf)
	r.payload = buf[len(buf)-int(r.payloadSize):]
	return err
}

// read: logrecord -> meta+payload -> entry
// write: entries+meta -> payload -> record
type recordEntry struct {
	baseEntry
	payload     []byte
	unmarshaled atomic.Uint32
	mashalMu    sync.RWMutex
}

func newRecordEntry() *recordEntry {
	return &recordEntry{
		baseEntry: baseEntry{
			entries: make([]*entry.Entry, 0), Meta: newMeta(),
		},
	}
}

func newEmptyRecordEntry(r logservice.LogRecord) *recordEntry {
	return &recordEntry{
		payload: r.Payload(),
		baseEntry: baseEntry{
			Meta: newMeta(),
		},
	}
}

func (r *recordEntry) forEachLogEntry(fn func(*entry.Entry)) (err error) {
	if len(r.entries) > 0 {
		for _, e := range r.entries {
			fn(e)
		}
		return
	}

	var (
		offset int64
		n      int64
	)
	for range r.Meta.addr {
		e := entry.NewEmptyEntry()
		if n, err = e.UnmarshalBinary(r.baseEntry.payload[offset:]); err != nil {
			return
		}
		offset += n
		fn(e)
	}
	return
}

func (r *recordEntry) append(e *entry.Entry) {
	r.entries = append(r.entries, e)
	r.Meta.addr[e.DSN] = uint64(r.payloadSize)
	r.payloadSize += uint64(e.GetSize())
}

func (r *recordEntry) prepareRecord() (size int) {
	var err error
	r.payload, err = r.Marshal()
	if err != nil {
		panic(err)
	}
	return len(r.payload)
}

func (r *recordEntry) unmarshal() {
	if r.unmarshaled.Load() == 1 {
		return
	}
	r.mashalMu.Lock()
	defer r.mashalMu.Unlock()
	if r.unmarshaled.Load() == 1 {
		return
	}
	head := objectio.DecodeIOEntryHeader(r.payload[:4])
	codec := objectio.GetIOEntryCodec(*head)
	entry, err := codec.Decode(r.payload[4:])
	if err != nil {
		panic(err)
	}
	r.baseEntry = *(entry.(*baseEntry))
	r.payload = nil
	r.unmarshaled.Store(1)
}
