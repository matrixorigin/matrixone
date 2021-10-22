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

import (
	"fmt"
	"io"
	"matrixone/pkg/vm/engine/aoe/common/util"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
	"unsafe"
)

var (
	_entPool = sync.Pool{New: func() interface{} {
		return newBaseEntry()
	}}
)

type Entry interface {
	IsAsync() bool
	GetMeta() *EntryMeta
	SetMeta(*EntryMeta)
	GetPayload() []byte
	Unmarshal([]byte) error
	ReadFrom(io.Reader) (int, error)
	WriteTo(StoreFileWriter, sync.Locker) (int, error)
	GetAuxilaryInfo() interface{}
	SetAuxilaryInfo(info interface{})
	Free()
}

type EntryType = uint16

const (
	ETInvalid EntryType = iota
	ETFlush
	ETCheckpoint
	ETCustomizeStart
)

var (
	EntryTypeSize     = int(unsafe.Sizeof(ETFlush))
	EntrySizeSize     = int(unsafe.Sizeof(uint32(0)))
	EntryReservedSize = int(unsafe.Sizeof(uint64(0))) * 8
	EntryMetaSize     = EntryTypeSize + EntrySizeSize + EntryReservedSize

	FlushEntry *BaseEntry
)

func init() {
	meta := &EntryMeta{
		Buf: make([]byte, EntryMetaSize),
	}
	meta.SetType(ETFlush)
	meta.SetPayloadSize(uint32(0))
	FlushEntry = &BaseEntry{
		Meta:    meta,
		Payload: make([]byte, 0),
	}
}

type EntryMeta struct {
	Buf []byte
}

func NewEntryMeta() *EntryMeta {
	meta := &EntryMeta{
		Buf: make([]byte, EntryMetaSize),
	}
	return meta
}

func (meta *EntryMeta) reset() {
	util.MemsetRepeatByte(meta.Buf, byte(0))
}

func (meta *EntryMeta) SetType(typ EntryType) {
	MarshallEntryTypeWithBuf(meta.Buf[:EntryTypeSize], typ)
}

func (meta *EntryMeta) SetPayloadSize(size uint32) {
	MarshallEntrySizeWithBuf(meta.Buf[EntryTypeSize:EntryTypeSize+EntrySizeSize], size)
}

func (meta *EntryMeta) GetType() EntryType {
	return UnmarshallEntryType(meta.Buf[:EntryTypeSize])
}

func (meta *EntryMeta) PayloadSize() uint32 {
	return UnmarshallEntrySize(meta.Buf[EntryTypeSize : EntrySizeSize+EntryTypeSize])
}

func (meta *EntryMeta) Size() uint32 {
	return uint32(EntryMetaSize)
}

func (meta *EntryMeta) GetReservedBuf() []byte {
	return meta.Buf[EntryTypeSize+EntrySizeSize:]
}

func (meta *EntryMeta) IsFlush() bool {
	typ := meta.GetType()
	return typ == ETFlush
}

func (meta *EntryMeta) IsCheckpoint() bool {
	typ := meta.GetType()
	return typ == ETCheckpoint
}

func (meta *EntryMeta) WriteTo(w io.Writer) (int, error) {
	// logutil.Info(meta.String())
	return w.Write(meta.Buf)
}

func (meta *EntryMeta) String() string {
	s := fmt.Sprintf("<EntryMeta(%d,%d)>", meta.GetType(), meta.PayloadSize())
	return s
}

func (meta *EntryMeta) ReadFrom(r io.Reader) (int, error) {
	if meta.Buf == nil {
		meta.Buf = make([]byte, EntryMetaSize)
	}
	return r.Read(meta.Buf)
}

type BaseEntry struct {
	Meta     *EntryMeta
	Payload  []byte
	Auxilary interface{}
	p        *sync.Pool
}

func newBaseEntry() *BaseEntry {
	e := &BaseEntry{
		Meta:    NewEntryMeta(),
		Payload: make([]byte, 0),
	}
	return e
}

func NewBaseEntryWithMeta(meta *EntryMeta) *BaseEntry {
	e := &BaseEntry{
		Meta:    meta,
		Payload: make([]byte, 0),
	}
	return e
}

func (e *BaseEntry) Clone(o Entry) {
	e.Meta.SetType(o.GetMeta().GetType())
	e.Meta.SetPayloadSize(o.GetMeta().PayloadSize())
	e.Payload = o.GetPayload()
	e.Auxilary = o.GetAuxilaryInfo()
}

func (e *BaseEntry) reset() {
	e.Meta.reset()
	e.Payload = e.Payload[:0]
	e.Auxilary = nil
	e.p = nil
}

func (e *BaseEntry) Free() {
	if e.p == nil {
		return
	}
	e.reset()
	_entPool.Put(e)
}

func (e *BaseEntry) IsAsync() bool                    { return false }
func (e *BaseEntry) GetAuxilaryInfo() interface{}     { return e.Auxilary }
func (e *BaseEntry) SetAuxilaryInfo(info interface{}) { e.Auxilary = info }
func (e *BaseEntry) GetMeta() *EntryMeta              { return e.Meta }
func (e *BaseEntry) SetMeta(meta *EntryMeta)          { e.Meta = meta }
func (e *BaseEntry) GetPayload() []byte               { return e.Payload }
func (e *BaseEntry) Unmarshal(buf []byte) error {
	// e.Payload = make([]byte, len(buf))
	// copy(e.Payload, buf)
	e.Payload = buf
	e.Meta.SetPayloadSize(uint32(len(buf)))
	return nil
}

func (e *BaseEntry) ReadFrom(r io.Reader) (int, error) {
	size := e.Meta.PayloadSize()
	e.Payload = make([]byte, size)
	return r.Read(e.Payload)
}

func (e *BaseEntry) WriteTo(w StoreFileWriter, locker sync.Locker) (int, error) {
	locker.Lock()
	defer locker.Unlock()
	if err := w.PrepareWrite(EntryMetaSize + int(e.Meta.PayloadSize())); err != nil {
		return 0, err
	}
	n1, err := e.Meta.WriteTo(w)
	if err != nil {
		return n1, err
	}
	n2, err := w.Write(e.Payload)
	if err != nil {
		return n2, err
	}
	auxilary := e.GetAuxilaryInfo()
	if auxilary == nil {
		return n1 + n2, nil
	}

	if e.Meta.IsCheckpoint() {
		r := auxilary.(*common.Range)
		w.ApplyCheckpoint(*r)
	} else {
		id := auxilary.(uint64)
		w.ApplyCommit(id)
	}
	return n1 + n2, err
}
