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
	"bytes"
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
type Info struct {
	Group       uint32
	TxnId       string
	Checkpoints []*CkpRanges
	Uncommits   string
	// PrepareEntryLsn uint64

	GroupLSN uint64

	TargetLsn uint64
	Info      any
}

func NewEmptyInfo() *Info {
	return &Info{}
}
func (info *Info) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, info.Group); err != nil {
		return
	}
	n += 4
	if err = binary.Write(w, binary.BigEndian, info.GroupLSN); err != nil {
		return
	}
	n += 8
	var sn int64
	if sn, err = common.WriteString(info.TxnId, w); err != nil {
		return
	}
	n += sn
	if sn, err = common.WriteString(info.Uncommits, w); err != nil {
		return
	}
	n += sn
	if err = binary.Write(w, binary.BigEndian, info.TargetLsn); err != nil {
		return
	}
	n += 8
	length := uint64(len(info.Checkpoints))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 8
	for _, ckps := range info.Checkpoints {
		if err = binary.Write(w, binary.BigEndian, ckps.Group); err != nil {
			return
		}
		n += 4
		var n2 int64
		n2, err = ckps.Ranges.WriteTo(w)
		if err != nil {
			return
		}
		n += n2
		cmdLength := uint64(len(ckps.Command))
		if err = binary.Write(w, binary.BigEndian, cmdLength); err != nil {
			return
		}
		n += 8
		for lsn, cmd := range ckps.Command {
			if err = binary.Write(w, binary.BigEndian, lsn); err != nil {
				return
			}
			n += 4
			cmdIdxLength := uint32(len(cmd.CommandIds))
			if err = binary.Write(w, binary.BigEndian, cmdIdxLength); err != nil {
				return
			}
			n += 4
			for _, id := range cmd.CommandIds {
				if err = binary.Write(w, binary.BigEndian, id); err != nil {
					return
				}
				n += 4
			}
			if err = binary.Write(w, binary.BigEndian, cmd.Size); err != nil {
				return
			}
			n += 4
		}
	}
	return
}
func (info *Info) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = info.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (info *Info) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &info.Group); err != nil {
		return
	}
	n += 4
	if err = binary.Read(r, binary.BigEndian, &info.GroupLSN); err != nil {
		return
	}
	n += 8
	var sn int64
	if info.TxnId, sn, err = common.ReadString(r); err != nil {
		return
	}
	n += sn
	if info.Uncommits, sn, err = common.ReadString(r); err != nil {
		return
	}
	n += sn
	if err = binary.Read(r, binary.BigEndian, &info.TargetLsn); err != nil {
		return
	}
	n += 8
	length := uint64(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 8
	info.Checkpoints = make([]*CkpRanges, length)
	for i := 0; i < int(length); i++ {
		ckps := &CkpRanges{}
		if err = binary.Read(r, binary.BigEndian, &ckps.Group); err != nil {
			return
		}
		n += 4
		ckps.Ranges = common.NewClosedIntervals()
		var n2 int64
		n2, err = ckps.Ranges.ReadFrom(r)
		if err != nil {
			return
		}
		n += n2
		cmdLength := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &cmdLength); err != nil {
			return
		}
		n += 8
		ckps.Command = make(map[uint64]CommandInfo)
		for i := 0; i < int(cmdLength); i++ {
			lsn := uint64(0)
			if err = binary.Read(r, binary.BigEndian, &lsn); err != nil {
				return
			}
			n += 8
			cmd := &CommandInfo{}
			cmdIdxLength := uint32(0)
			if err = binary.Read(r, binary.BigEndian, &cmdIdxLength); err != nil {
				return
			}
			n += 4
			cmd.CommandIds = make([]uint32, cmdIdxLength)
			for i := 0; i < int(cmdIdxLength); i++ {
				if err = binary.Read(r, binary.BigEndian, &cmd.CommandIds[i]); err != nil {
					return
				}
				n += 4
			}
			if err = binary.Read(r, binary.BigEndian, &cmd.Size); err != nil {
				return
			}
			n += 4
			ckps.Command[lsn] = *cmd
		}
		info.Checkpoints[i] = ckps
	}
	return
}
func (info *Info) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := info.ReadFrom(bbuf)
	return err
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
	default:
		s := fmt.Sprintf("customized entry G%d<%d>{T%s}", info.Group, info.GroupLSN, info.TxnId)
		s = fmt.Sprintf("%s\n", s)
		return s
	}
}

type Base struct {
	*descriptor
	node      []byte
	payload   []byte
	info      any
	infobuf   []byte
	wg        sync.WaitGroup
	t0        time.Time
	printTime bool
	err       error
}

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
		common.TAEGPool.Free(b.node)
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
	b.SetInfoSize(len(buf))
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
		return b.node[:b.GetPayloadSize()]
	}
	return b.payload
}

func (b *Base) SetInfo(info any) {
	b.info = info
}

func (b *Base) GetInfo() any {
	return b.info
}

func (b *Base) UnmarshalFromNode(n []byte, own bool) error {
	if b.node != nil {
		common.TAEGPool.Free(b.node)
		b.node = nil
	}
	if own {
		b.node = n
		b.payload = b.node
	} else {
		copy(b.payload, n)
	}
	b.SetPayloadSize(len(b.payload))
	return nil
}

func (b *Base) SetPayload(buf []byte) error {
	if b.node != nil {
		common.TAEGPool.Free(b.node)
		b.node = nil
	}
	b.payload = buf
	b.SetPayloadSize(len(buf))
	return nil
}

func (b *Base) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := b.ReadFrom(bbuf)
	return err
}
func (b *Base) GetLsn() (gid uint32, lsn uint64) {
	v := b.GetInfo()
	if v == nil {
		return
	}
	info := v.(*Info)
	gid = info.Group
	lsn = info.GroupLSN
	return
}
func (b *Base) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = b.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (b *Base) ReadFrom(r io.Reader) (int64, error) {
	metaBuf := b.GetMetaBuf()
	_, err := r.Read(metaBuf)
	if err != nil {
		return 0, err
	}

	if b.node == nil {
		b.node, err = common.TAEGPool.Alloc(b.GetPayloadSize())
		if err != nil {
			panic(err)
		}
		b.payload = b.node[:b.GetPayloadSize()]
	}
	if b.GetType() == ETCheckpoint && b.GetPayloadSize() != 0 {
		logutil.Infof("payload %d", b.GetPayloadSize())
		panic("wrong payload size")
	}
	n1 := 0
	if b.GetInfoSize() != 0 {
		infoBuf := make([]byte, b.GetInfoSize())
		n, err := r.Read(infoBuf)
		n1 += n
		if err != nil {
			return int64(n1), err
		}
		info := NewEmptyInfo()
		err = info.Unmarshal(infoBuf)
		if err != nil {
			return int64(n1), err
		}
		b.SetInfo(info)
	}
	n2, err := r.Read(b.payload)
	if err != nil {
		return int64(n2), err
	}
	return int64(n1 + n2), nil
}

func (b *Base) ReadAt(r *os.File, offset int) (int, error) {
	metaBuf := b.GetMetaBuf()
	n, err := r.ReadAt(metaBuf, int64(offset))
	if err != nil {
		return n, err
	}
	if b.node == nil {
		b.node, err = common.TAEGPool.Alloc(b.GetPayloadSize())
		if err != nil {
			panic(err)
		}
		b.payload = b.node[:b.GetPayloadSize()]
	}

	offset += len(b.GetMetaBuf())
	infoBuf := make([]byte, b.GetInfoSize())
	n1, err := r.ReadAt(infoBuf, int64(offset))
	if err != nil {
		return n + n1, err
	}

	offset += n1
	b.SetInfoBuf(infoBuf)
	info := NewEmptyInfo()
	err = info.Unmarshal(infoBuf)
	if err != nil {
		return n + n1, err
	}
	b.SetInfo(info)
	n2, err := r.ReadAt(b.payload, int64(offset))
	if err != nil {
		return n2, err
	}
	return n + n1 + n2, nil
}

func (b *Base) PrepareWrite() {
	if b.info == nil {
		return
	}
	buf, err := b.info.(*Info).Marshal()
	if err != nil {
		panic(err)
	}
	b.SetInfoBuf(buf)
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
