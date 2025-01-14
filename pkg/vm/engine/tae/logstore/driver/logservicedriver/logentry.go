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
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"go.uber.org/zap"
)

var emptyLogEntry = make([]byte, EmptyLogEntrySize)
var logEntryBuffer = make([]byte, EmptyLogEntrySize)
var skipCmdBuffer = make([]byte, EmptyLogEntrySize)

const (
	TypeSize          = 2
	VersionSize       = 2
	CmdTypeSize       = 2
	ReservedSize      = 32
	EntryCountSize    = 4
	EntryStartDSNSize = 8
	FooterOffsetSize  = 4

	OneEntryFooterSize = 8

	TypeOffset          = 0
	VersionOffset       = TypeOffset + TypeSize
	CmdTypeOffset       = VersionOffset + VersionSize
	ReservedOffset      = CmdTypeOffset + CmdTypeSize
	EntryCountOffset    = ReservedOffset + ReservedSize
	EntryStartDSNOffset = EntryCountOffset + EntryCountSize
	FooterOffsetOffset  = EntryStartDSNOffset + EntryStartDSNSize

	EmptyLogEntrySize = FooterOffsetOffset + FooterOffsetSize
)

func init() {
	e := LogEntry(logEntryBuffer)
	e.SetHeader(IOET_WALRecord, IOET_WALRecord_CurrVer, uint16(Cmd_Normal))
	e = LogEntry(skipCmdBuffer)
	e.SetHeader(IOET_WALRecord, IOET_WALRecord_CurrVer, uint16(Cmd_SkipDSN))
}

type LogEntryWriter struct {
	Entry  LogEntry
	Footer LogEntryFooter
}

func NewLogEntryWriter() *LogEntryWriter {
	w := &LogEntryWriter{
		Entry:  NewLogEntry(),
		Footer: make([]byte, 0),
	}
	copy(w.Entry, logEntryBuffer)
	return w
}

func (w *LogEntryWriter) Reset() {
	if w.Entry.Capacity() >= int(mpool.MB)*2 {
		w.Entry = NewLogEntry()
	} else {
		w.Entry.Reset()
	}
	copy(w.Entry, logEntryBuffer)
	if len(w.Footer) >= int(mpool.MB)*2 {
		w.Footer = make([]byte, 0)
	} else {
		w.Footer.Reset()
	}
}

func (w *LogEntryWriter) Close() {
	w.Entry = nil
	w.Footer = nil
}

func (w *LogEntryWriter) Capacity() int {
	return w.Entry.Capacity() + cap(w.Footer)
}

func (w *LogEntryWriter) Append(buf []byte) {
	offset, length := w.Entry.AppendEntry(buf)
	w.Footer.AppendEntry(offset, length)
}

func (w *LogEntryWriter) Finish(startDSN uint64) LogEntry {
	w.Entry.SetFooter(startDSN, w.Footer)
	return w.Entry
}

func (w *LogEntryWriter) IsFinished() bool {
	return w.Entry.GetFooterOffset() != 0 && w.Entry.GetCmdType() != uint16(Cmd_Invalid)
}

type LogEntryFooter []byte
type LogEntry []byte

func NewLogEntry() LogEntry {
	return make([]byte, EmptyLogEntrySize)
}

func (footer LogEntryFooter) String() string {
	var buf bytes.Buffer
	cnt := footer.GetEntryCount()
	buf.WriteString(fmt.Sprintf("Footer[%d](", cnt))
	for i := uint32(0); i < cnt; i++ {
		offset, length := footer.GetEntry(int(i))
		buf.WriteString(fmt.Sprintf("%d,%d;", offset, length))
	}
	buf.WriteString(")")
	return buf.String()
}

func (footer LogEntryFooter) ShortString() string {
	return fmt.Sprintf("Footer[%d]", footer.GetEntryCount())
}

func (footer *LogEntryFooter) Reset() {
	*footer = (*footer)[:0]
}

func (footer *LogEntryFooter) AppendEntry(offset, length uint32) {
	*footer = append(*footer, types.EncodeUint32(&offset)...)
	*footer = append(*footer, types.EncodeUint32(&length)...)
}

func (footer LogEntryFooter) GetEntryCount() uint32 {
	return uint32(len(footer) / OneEntryFooterSize)
}

func (footer LogEntryFooter) GetEntry(i int) (offset, length uint32) {
	offset = types.DecodeUint32(footer[i*OneEntryFooterSize:])
	length = types.DecodeUint32(footer[i*OneEntryFooterSize+4:])
	return
}

func (e LogEntry) String() string {
	dsn := e.GetStartDSN()
	return fmt.Sprintf("LogEntry[%d:%d][%d][%s]", e.GetType(), e.GetVersion(), dsn, e.GetFooter().String())
}

func (e LogEntry) GetType() uint16 {
	return types.DecodeUint16(e[TypeOffset:])
}

func (e LogEntry) GetVersion() uint16 {
	return types.DecodeUint16(e[VersionOffset:])
}

func (e LogEntry) GetCmdType() uint16 {
	return types.DecodeUint16(e[CmdTypeOffset:])
}

func (e LogEntry) GetFooter() LogEntryFooter {
	footerOffset := e.GetFooterOffset()
	if footerOffset == 0 {
		return nil
	}
	return LogEntryFooter(e[footerOffset:])
}

func (e LogEntry) GetEntry(i int) []byte {
	footer := e.GetFooter()
	offset, length := footer.GetEntry(i)
	return e[offset : offset+length]
}

func (e LogEntry) GetFooterOffset() uint32 {
	return types.DecodeUint32(e[FooterOffsetOffset:])
}

func (e LogEntry) GetEntryCount() uint32 {
	return types.DecodeUint32(e[EntryCountOffset:])
}

func (e LogEntry) GetStartDSN() uint64 {
	return types.DecodeUint64(e[EntryStartDSNOffset:])
}

func (e LogEntry) SetHeader(
	typ uint16,
	version uint16,
	cmdType uint16,
) {
	copy(e[TypeOffset:], types.EncodeUint16(&typ))
	copy(e[VersionOffset:], types.EncodeUint16(&version))
	copy(e[CmdTypeOffset:], types.EncodeUint16(&cmdType))
}

func (e LogEntry) SetEntryCount(count uint32) {
	copy(e[EntryCountOffset:], types.EncodeUint32(&count))
}

func (e LogEntry) SetStartDSN(startDSN uint64) {
	copy(e[EntryStartDSNOffset:], types.EncodeUint64(&startDSN))
}

func (e LogEntry) SetFooterOffset(offset uint32) {
	copy(e[FooterOffsetOffset:], types.EncodeUint32(&offset))
}

func (e *LogEntry) Reset() {
	*e = (*e)[:EmptyLogEntrySize]
	copy(*e, emptyLogEntry)
}

func (e *LogEntry) Capacity() int {
	return cap(*e)
}

func (e *LogEntry) SetFooter(
	startDSN uint64,
	footer LogEntryFooter,
) {
	filledOffset := e.GetFooterOffset()
	filledCount := e.GetEntryCount()
	filledStartDSN := e.GetStartDSN()
	if filledOffset != 0 || filledCount != 0 || filledStartDSN != 0 {
		logutil.Fatal(
			"Wal-LogEntry",
			zap.Uint32("filled-offset", filledOffset),
			zap.Uint32("filled-count", filledCount),
			zap.Uint64("filled-start-dsn", filledStartDSN),
		)
	}
	offset := len(*e)
	*e = append(*e, footer...)
	e.SetFooterOffset(uint32(offset))
	e.SetEntryCount(uint32(footer.GetEntryCount()))
	e.SetStartDSN(startDSN)
}

func (e *LogEntry) AppendEntry(buf []byte) (offset, length uint32) {
	offset = uint32(len(*e))
	length = uint32(len(buf))
	*e = append(*e, buf...)
	return
}

// PXU TODO: codec?
func (e LogEntry) ForEachEntry(
	fn func(entry *entry.Entry),
) (err error) {
	for i, end := 0, int(e.GetEntryCount()); i < end; i++ {
		buf := e.GetEntry(i)
		entry := entry.NewEmptyEntry()
		if _, err = entry.UnmarshalBinary(buf[:]); err != nil {
			return
		}
		fn(entry)
	}
	return
}

type SkipCmd []byte

func NewSkipCmd(cnt int) SkipCmd {
	return make([]byte, 16*cnt)
}

func (s SkipCmd) GetDSNBuf() []byte {
	return s[:len(s)/2]
}

func (s SkipCmd) GetPSNBuf() []byte {
	return s[len(s)/2:]
}

func (s SkipCmd) GetDSNSlice() []uint64 {
	return types.DecodeSlice[uint64](s.GetDSNBuf())
}

func (s SkipCmd) GetPSNSlice() []uint64 {
	return types.DecodeSlice[uint64](s.GetPSNBuf())
}

func (s SkipCmd) ElementCount() int {
	return len(s) / 16
}

func (s SkipCmd) Set(i int, dsn, psn uint64) {
	dsns := s.GetDSNSlice()
	psns := s.GetPSNSlice()
	dsns[i] = dsn
	psns[i] = psn
}

func (s *SkipCmd) Reset(n int) {
	if s.ElementCount() > 1000 || n > s.ElementCount() {
		*s = make([]byte, 16*n)
		return
	}
	for i := 0; i < s.ElementCount(); i++ {
		s.Set(i, 0, 0)
	}
	*s = (*s)[:16*n]
}

func (s SkipCmd) Sort() {
	dsns := s.GetDSNSlice()
	psns := s.GetPSNSlice()
	sort.Slice(dsns, func(i, j int) bool {
		less := dsns[i] < dsns[j]
		if less {
			psns[i], psns[j] = psns[j], psns[i]
		}
		return less
	})
}
