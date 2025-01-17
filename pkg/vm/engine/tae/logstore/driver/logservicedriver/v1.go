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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

type V1Meta struct {
	cmdType     CmdType
	appended    uint64
	addr        map[uint64]uint64
	payloadSize uint64
}

func newMeta() V1Meta {
	return V1Meta{addr: make(map[uint64]uint64), cmdType: Cmd_Normal}
}

func (m *V1Meta) GetPayloadSize() uint64 {
	return m.payloadSize
}

func (m *V1Meta) SetPayloadSize(s uint64) {
	m.payloadSize = s
}

func (m *V1Meta) GetAddr() map[uint64]uint64 {
	return m.addr
}

func (m *V1Meta) AddAddr(l uint64, s uint64) {
	if m.addr == nil {
		m.addr = make(map[uint64]uint64)
	}
	m.addr[l] = s
}

func (m *V1Meta) SetType(t CmdType) {
	m.cmdType = t
}
func (m *V1Meta) GetType() CmdType {
	return m.cmdType
}
func (m *V1Meta) SetAppended(appended uint64) {
	m.appended = appended
}
func (m *V1Meta) GetMinDSN() uint64 {
	min := uint64(0)
	min = math.MaxUint64
	for lsn := range m.addr {
		if lsn < min {
			min = lsn
		}
	}
	return min
}
func (m *V1Meta) GetMaxLsn() uint64 {
	max := uint64(0)
	for lsn := range m.addr {
		if lsn > max {
			max = lsn
		}
	}
	return max
}
func (m *V1Meta) WriteTo(w io.Writer) (n int64, err error) {
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

func (m *V1Meta) ReadFrom(r io.Reader) (n int64, err error) {
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

func (m *V1Meta) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := m.ReadFrom(bbuf)
	return err
}

func (m *V1Meta) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = m.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

type v1Entry struct {
	V1Meta
	EntryType, Version uint16
	entries            []*entry.Entry
	cmd                v1SkipCmd
	payload            []byte
}

func (r *v1Entry) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = r.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (r *v1Entry) WriteTo(w io.Writer) (n int64, err error) {
	r.EntryType = IOET_WALRecord
	r.Version = IOET_WALRecord_V1
	if _, err = w.Write(types.EncodeUint16(&r.EntryType)); err != nil {
		return 0, err
	}
	n += 2
	if _, err = w.Write(types.EncodeUint16(&r.Version)); err != nil {
		return 0, err
	}
	n += 2
	n1, err := r.V1Meta.WriteTo(w)
	if err != nil {
		return n, err
	}
	n += n1
	switch r.V1Meta.cmdType {
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

func (r *v1Entry) ReadFrom(reader io.Reader) (n int64, err error) {
	n1, err := r.V1Meta.ReadFrom(reader)
	if err != nil {
		return 0, err
	}
	n += n1
	if r.cmdType == Cmd_SkipDSN {
		r.cmd = newV1SkipCmd()
		n1, err = r.cmd.ReadFrom(reader)
		if err != nil {
			return 0, err
		}
		n += n1
	}
	return
}

func (r *v1Entry) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := r.ReadFrom(bbuf)
	r.payload = buf[len(buf)-int(r.payloadSize):]
	return err
}

// read: logrecord -> meta+payload -> entry
// write: entries+meta -> payload -> record
type v1Record struct {
	v1Entry
	payload []byte
}

func newRecordEntry() *v1Record {
	return &v1Record{
		v1Entry: v1Entry{
			entries: make([]*entry.Entry, 0), V1Meta: newMeta(),
		},
	}
}

func (r *v1Record) append(e *entry.Entry) {
	r.entries = append(r.entries, e)
	r.V1Meta.addr[e.DSN] = uint64(r.payloadSize)
	r.payloadSize += uint64(e.GetSize())
}

func (r *v1Record) prepareRecord() (size int) {
	var err error
	r.payload, err = r.Marshal()
	if err != nil {
		panic(err)
	}
	return len(r.payload)
}

type v1SkipCmd struct {
	// DSN->PSN mapping
	skipMap map[uint64]uint64
}

func newV1SkipCmd() v1SkipCmd {
	return v1SkipCmd{
		skipMap: make(map[uint64]uint64),
	}
}

func (c *v1SkipCmd) WriteTo(w io.Writer) (n int64, err error) {
	length := uint16(len(c.skipMap))
	if _, err = w.Write(types.EncodeUint16(&length)); err != nil {
		return
	}
	n += 2
	for dsn, psn := range c.skipMap {
		if _, err = w.Write(types.EncodeUint64(&dsn)); err != nil {
			return
		}
		n += 8
		if _, err = w.Write(types.EncodeUint64(&psn)); err != nil {
			return
		}
		n += 8
	}
	return
}

func (c *v1SkipCmd) ReadFrom(r io.Reader) (n int64, err error) {
	length := uint16(0)
	if _, err = r.Read(types.EncodeUint16(&length)); err != nil {
		return
	}
	n += 2
	for i := 0; i < int(length); i++ {
		dsn := uint64(0)
		lsn := uint64(0)
		if _, err = r.Read(types.EncodeUint64(&dsn)); err != nil {
			return
		}
		n += 8
		if _, err = r.Read(types.EncodeUint64(&lsn)); err != nil {
			return
		}
		n += 8
		c.skipMap[dsn] = lsn
	}
	return
}

func (c *v1SkipCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := c.ReadFrom(bbuf)
	return err
}

func (c *v1SkipCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
