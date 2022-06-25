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

package entry

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	_basePool = sync.Pool{New: func() any {
		return &Base{
			descriptor: newDescriptor(),
		}
	}}
)

type Base struct {
	*descriptor
	node      *common.MemNode
	payload   []byte
	info      any
	infobuf   []byte
	wg        sync.WaitGroup
	t0        time.Time
	printTime bool
	err       error
}

type Info struct {
	Group uint32
	// CommitId    uint64 //0 for RollBack
	TxnId       uint64 //0 for entrys not in txn
	Checkpoints []CkpRanges
	Uncommits   []Tid

	GroupLSN uint64

	PostCommitVersion int
	Info              any
}

func (info *Info) Marshal() []byte {
	buf := make([]byte, 128)
	pos := 0
	binary.BigEndian.PutUint32(buf[pos:pos+4], info.Group)
	pos += 4
	binary.BigEndian.PutUint64(buf[pos:pos+8], info.TxnId)
	pos += 8

	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(info.PostCommitVersion))
	pos += 8

	length := uint64(len(info.Checkpoints))
	binary.BigEndian.PutUint64(buf[pos:pos+8], length)
	pos += 8
	for _, ckps := range info.Checkpoints {
		if len(buf) < pos+12 {
			buf = append(buf, make([]byte, 128)...)
		}
		binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(ckps.Group))
		pos += 4
		if ckps.Ranges == nil {
			length := uint64(0)
			binary.BigEndian.PutUint64(buf[pos:pos+8], length)
			pos += 8
		} else {
			length := uint64(len(ckps.Ranges.Intervals))
			binary.BigEndian.PutUint64(buf[pos:pos+8], length)
			pos += 8
			for _, interval := range ckps.Ranges.Intervals {
				if len(buf) < pos+16 {
					buf = append(buf, make([]byte, 128)...)
				}
				binary.BigEndian.PutUint64(buf[pos:pos+8], interval.Start)
				pos += 8
				binary.BigEndian.PutUint64(buf[pos:pos+8], interval.End)
				pos += 8
			}
		}
		if len(buf) < pos+8 {
			buf = append(buf, make([]byte, 128)...)
		}
		length = uint64(len(ckps.Command))
		binary.BigEndian.PutUint64(buf[pos:pos+8], length)
		pos += 8
		for lsn, cmd := range ckps.Command {
			if len(buf) < pos+16 {
				buf = append(buf, make([]byte, 128)...)
			}
			binary.BigEndian.PutUint64(buf[pos:pos+8], lsn)
			pos += 8
			length = uint64(len(cmd.CommandIds))
			binary.BigEndian.PutUint64(buf[pos:pos+8], length)
			pos += 8
			for _, commandId := range cmd.CommandIds {
				if len(buf) < pos+4 {
					buf = append(buf, make([]byte, 128)...)
				}
				binary.BigEndian.PutUint32(buf[pos:pos+4], commandId)
				pos += 4
			}
			if len(buf) < pos+4 {
				buf = append(buf, make([]byte, 128)...)
			}
			binary.BigEndian.PutUint32(buf[pos:pos+4], cmd.Size)
			pos += 4
		}
	}

	length = uint64(len(info.Uncommits))
	if len(buf) < pos+8 {
		buf = append(buf, make([]byte, 128)...)
	}
	binary.BigEndian.PutUint64(buf[pos:pos+8], length)
	pos += 8
	for _, tidInfo := range info.Uncommits {
		if len(buf) < pos+12 {
			buf = append(buf, make([]byte, 128)...)
		}
		binary.BigEndian.PutUint32(buf[pos:pos+4], tidInfo.Group)
		pos += 4
		binary.BigEndian.PutUint64(buf[pos:pos+8], tidInfo.Tid)
		pos += 8
	}

	if len(buf) < pos+8 {
		buf = append(buf, make([]byte, 128)...)
	}
	binary.BigEndian.PutUint64(buf[pos:pos+8], info.GroupLSN)
	pos += 8

	buf = buf[:pos]
	return buf
}
func Unmarshal(buf []byte) *Info {
	info := &Info{}
	pos := 0
	info.Group = binary.BigEndian.Uint32(buf[pos : pos+4])
	pos += 4
	info.TxnId = binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	id := binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	info.PostCommitVersion = int(id)

	length := binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	info.Checkpoints = make([]CkpRanges, 0, length)
	for i := 0; i < int(length); i++ {
		ckps := CkpRanges{}
		ckps.Group = binary.BigEndian.Uint32(buf[pos : pos+4])
		pos += 4
		intervalLength := binary.BigEndian.Uint64(buf[pos : pos+8])
		pos += 8
		ckps.Ranges = &common.ClosedIntervals{
			Intervals: make([]*common.ClosedInterval, 0, intervalLength),
		}
		for j := 0; j < int(intervalLength); j++ {
			interval := &common.ClosedInterval{}
			interval.Start = binary.BigEndian.Uint64(buf[pos : pos+8])
			pos += 8
			interval.End = binary.BigEndian.Uint64(buf[pos : pos+8])
			pos += 8
			ckps.Ranges.Intervals = append(ckps.Ranges.Intervals, interval)
		}
		cmdInfoLength := binary.BigEndian.Uint64(buf[pos : pos+8])
		pos += 8
		ckps.Command = make(map[uint64]CommandInfo)
		for j := 0; j < int(cmdInfoLength); j++ {
			cmd := CommandInfo{}
			lsn := binary.BigEndian.Uint64(buf[pos : pos+8])
			pos += 8
			cmdIdsLength := binary.BigEndian.Uint64(buf[pos : pos+8])
			pos += 8
			cmd.CommandIds = make([]uint32, cmdIdsLength)
			for k := 0; k < int(cmdIdsLength); k++ {
				cmd.CommandIds[k] = binary.BigEndian.Uint32(buf[pos : pos+4])
				pos += 4
			}
			cmd.Size = binary.BigEndian.Uint32(buf[pos : pos+4])
			pos += 4
			ckps.Command[lsn] = cmd
		}
		info.Checkpoints = append(info.Checkpoints, ckps)
	}

	length = binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	info.Uncommits = make([]Tid, 0, length)
	for i := 0; i < int(length); i++ {
		tidInfo := Tid{}
		tidInfo.Group = binary.BigEndian.Uint32(buf[pos : pos+4])
		pos += 4
		tidInfo.Tid = binary.BigEndian.Uint64(buf[pos : pos+8])
		pos += 8
		info.Uncommits = append(info.Uncommits, tidInfo)
	}
	info.GroupLSN = binary.BigEndian.Uint64(buf[pos : pos+8])
	return info
}

func (info *Info) ToString() string {
	switch info.Group {
	case GTCKp:
		s := "checkpoint entry"
		for _, ranges := range info.Checkpoints {
			s = fmt.Sprintf("%s%s", s, ranges)
		}
		s = fmt.Sprintf("%s\n", s)
		return s
	case GTUncommit:
		s := "uncommit entry"
		for _, tid := range info.Uncommits {
			s = fmt.Sprintf("%s G%d-%d", s, tid.Group, tid.Tid)
		}
		s = fmt.Sprintf("%s\n", s)
		return s
	default:
		s := fmt.Sprintf("customized entry G%d<%d>{T%d}", info.Group, info.GroupLSN, info.TxnId)
		s = fmt.Sprintf("%s\n", s)
		return s
	}
}

type Tid struct {
	Group uint32
	Tid   uint64
}

type CkpRanges struct {
	Group   uint32
	Ranges  *common.ClosedIntervals
	Command map[uint64]CommandInfo
}

func (r CkpRanges) String() string {
	s := fmt.Sprintf("G%d-%v", r.Group, r.Ranges)
	for lsn, cmd := range r.Command {
		s = fmt.Sprintf("%s[%d-%v/%d]", s, lsn, cmd.CommandIds, cmd.Size)
	}
	return s
}

type CommandInfo struct {
	CommandIds []uint32
	Size       uint32
}

// type CommitInfo struct {
// 	Group    uint32
// 	CommitId uint64
// 	Addr interface{}
// }

// func (info *CommitInfo) ToString() string {
// 	return fmt.Sprintf("commit entry <%d>-%d\n", info.Group, info.CommitId)
// }

// type UncommitInfo struct {
// 	Tids map[uint32][]uint64
// 	Addr interface{}
// }

// func (info *UncommitInfo) ToString() string {
// 	return fmt.Sprintf("uncommit entry %v\n", info.Tids)
// }

// type TxnInfo struct {
// 	Group    uint32
// 	Tid      uint64
// 	CommitId uint64
// 	Addr     interface{}
// }

// func (info *TxnInfo) ToString() string {
// 	return fmt.Sprintf("txn entry <%d> %d-%d\n", info.Group, info.Tid, info.CommitId)
// }

func GetBase() *Base {
	b := _basePool.Get().(*Base)
	if b.GetPayloadSize() != 0 {
		logutil.Infof("payload size is %d", b.GetPayloadSize())
		panic("wrong payload size")
	}
	b.wg.Add(1)
	return b
}
func (b *Base) StartTime() {
	b.t0 = time.Now()
}
func (b *Base) Duration() time.Duration {
	return time.Since(b.t0)
}
func (b *Base) PrintTime() {
	b.printTime = true
}
func (b *Base) IsPrintTime() bool {
	return b.printTime
}
func (b *Base) reset() {
	b.descriptor.reset()
	if b.node != nil {
		common.GPool.Free(b.node)
		b.node = nil
	}
	b.payload = nil
	b.info = nil
	b.infobuf = nil
	b.wg = sync.WaitGroup{}
	b.t0 = time.Time{}
	b.printTime = false
	b.err = nil
}
func (b *Base) GetInfoBuf() []byte {
	return b.infobuf
}
func (b *Base) SetInfoBuf(buf []byte) {
	b.infobuf = buf
}
func (b *Base) GetError() error {
	return b.err
}

func (b *Base) WaitDone() error {
	b.wg.Wait()
	return b.err
}

func (b *Base) DoneWithErr(err error) {
	b.err = err
	b.wg.Done()
}

func (b *Base) Free() {
	b.reset()
	if b.GetPayloadSize() != 0 {
		logutil.Infof("payload size is %d", b.GetPayloadSize())
		panic("wrong payload size")
	}
	_basePool.Put(b)
}

func (b *Base) GetPayload() []byte {
	if b.node != nil {
		return b.node.Buf[:b.GetPayloadSize()]
	}
	return b.payload
}

func (b *Base) SetInfo(info any) {
	b.info = info
}

func (b *Base) GetInfo() any {
	return b.info
}

func (b *Base) UnmarshalFromNode(n *common.MemNode, own bool) error {
	if b.node != nil {
		common.GPool.Free(b.node)
		b.node = nil
	}
	if own {
		b.node = n
		b.payload = b.node.GetBuf()
	} else {
		copy(b.payload, n.GetBuf())
	}
	b.SetPayloadSize(len(b.payload))
	return nil
}

func (b *Base) Unmarshal(buf []byte) error {
	if b.node != nil {
		common.GPool.Free(b.node)
		b.node = nil
	}
	b.payload = buf
	b.SetPayloadSize(len(buf))
	return nil
}

func (b *Base) ReadFrom(r io.Reader) (int64, error) {
	if b.node == nil {
		b.node = common.GPool.Alloc(uint64(b.GetPayloadSize()))
		b.payload = b.node.Buf[:b.GetPayloadSize()]
	}
	if b.GetType() == ETCheckpoint && b.GetPayloadSize() != 0 {
		logutil.Infof("payload %d", b.GetPayloadSize())
		panic("wrong payload size")
	}
	n1 := 0
	if b.GetInfoSize() != 0 {
		infoBuf := make([]byte, b.GetInfoSize())
		n, err := r.Read(infoBuf)
		n1 = n
		if err != nil {
			return int64(n1), err
		}
		b.SetInfoBuf(infoBuf)
		if len(infoBuf) != 0 {
			info := Unmarshal(infoBuf)
			b.SetInfo(info)
		}
	}
	n2, err := r.Read(b.payload)
	if err != nil {
		return int64(n2), err
	}
	return int64(n1 + n2), nil
}

func (b *Base) ReadAt(r *os.File, offset int) (int, error) {
	if b.node == nil {
		b.node = common.GPool.Alloc(uint64(b.GetPayloadSize()))
		b.payload = b.node.Buf[:b.GetPayloadSize()]
	}
	offset += len(b.GetMetaBuf())
	infoBuf := make([]byte, b.GetInfoSize())
	n1, err := r.ReadAt(infoBuf, int64(offset))
	if err != nil {
		return n1, err
	}
	offset += n1
	b.SetInfoBuf(infoBuf)
	info := Unmarshal(infoBuf)
	b.SetInfo(info)
	n2, err := r.ReadAt(b.payload, int64(offset))
	if err != nil {
		return n2, err
	}
	return n1 + n2, nil
}

func (b *Base) WriteTo(w io.Writer) (int64, error) {
	n1, err := b.descriptor.WriteTo(w)
	if err != nil {
		return n1, err
	}
	n2, err := w.Write(b.GetInfoBuf())
	if err != nil {
		return int64(n2), err
	}
	n3, err := w.Write(b.payload)
	if err != nil {
		return int64(n3), err
	}
	return n1 + int64(n2) + int64(n3), err
}
