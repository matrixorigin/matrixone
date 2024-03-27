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
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
	Checkpoints []*CkpRanges
	GroupLSN    uint64

	TargetLsn uint64
	Info      any
}

func NewEmptyInfo() *Info {
	return &Info{}
}
func (info *Info) WriteTo(w io.Writer) (n int64, err error) {
	if _, err = w.Write(types.EncodeUint32(&info.Group)); err != nil {
		return
	}
	n += 4
	if _, err = w.Write(types.EncodeUint64(&info.GroupLSN)); err != nil {
		return
	}
	n += 8
	if _, err = w.Write(types.EncodeUint64(&info.TargetLsn)); err != nil {
		return
	}
	n += 8
	length := uint64(len(info.Checkpoints))
	if _, err = w.Write(types.EncodeUint64(&length)); err != nil {
		return
	}
	n += 8
	for _, ckps := range info.Checkpoints {
		if _, err = w.Write(types.EncodeUint32(&ckps.Group)); err != nil {
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
		if _, err = w.Write(types.EncodeUint64(&cmdLength)); err != nil {
			return
		}
		n += 8
		for lsn, cmd := range ckps.Command {
			if _, err = w.Write(types.EncodeUint64(&lsn)); err != nil {
				return
			}
			n += 8
			cmdIdxLength := uint32(len(cmd.CommandIds))
			if _, err = w.Write(types.EncodeUint32(&cmdIdxLength)); err != nil {
				return
			}
			n += 4
			for _, id := range cmd.CommandIds {
				if _, err = w.Write(types.EncodeUint32(&id)); err != nil {
					return
				}
				n += 4
			}
			if _, err = w.Write(types.EncodeUint32(&cmd.Size)); err != nil {
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
	if _, err = r.Read(types.EncodeUint32(&info.Group)); err != nil {
		return
	}
	n += 4
	if _, err = r.Read(types.EncodeUint64(&info.GroupLSN)); err != nil {
		return
	}
	n += 8
	if _, err = r.Read(types.EncodeUint64(&info.TargetLsn)); err != nil {
		return
	}
	n += 8
	length := uint64(0)
	if _, err = r.Read(types.EncodeUint64(&length)); err != nil {
		return
	}
	n += 8
	info.Checkpoints = make([]*CkpRanges, length)
	for i := 0; i < int(length); i++ {
		ckps := &CkpRanges{}
		if _, err = r.Read(types.EncodeUint32(&ckps.Group)); err != nil {
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
		if _, err = r.Read(types.EncodeUint64(&cmdLength)); err != nil {
			return
		}
		n += 8
		ckps.Command = make(map[uint64]CommandInfo)
		for i := 0; i < int(cmdLength); i++ {
			lsn := uint64(0)
			if _, err = r.Read(types.EncodeUint64(&lsn)); err != nil {
				return
			}
			n += 8
			cmd := &CommandInfo{}
			cmdIdxLength := uint32(0)
			if _, err = r.Read(types.EncodeUint32(&cmdIdxLength)); err != nil {
				return
			}
			n += 4
			cmd.CommandIds = make([]uint32, cmdIdxLength)
			for i := 0; i < int(cmdIdxLength); i++ {
				if _, err = r.Read(types.EncodeUint32(&cmd.CommandIds[i])); err != nil {
					return
				}
				n += 4
			}
			if _, err = r.Read(types.EncodeUint32(&cmd.Size)); err != nil {
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
		s := fmt.Sprintf("customized entry G%d<%d>", info.Group, info.GroupLSN)
		s = fmt.Sprintf("%s\n", s)
		return s
	}
}

type Base struct {
	*descriptor

	payload   []byte
	info      any
	infobuf   []byte
	wg        sync.WaitGroup
	t0        time.Time
	printTime bool
	err       error

	preCallbacks []func() error
}

func GetBase() *Base {
	b := _basePool.Get().(*Base)
	if b.GetPayloadSize() != 0 {
		logutil.Debugf("payload size is %d", b.GetPayloadSize())
		panic("wrong payload size")
	}
	b.wg.Add(1)
	return b
}

func (b *Base) RegisterPreCallback(cb func() error) {
	b.preCallbacks = append(b.preCallbacks, cb)
}

func (b *Base) ExecutePreCallbacks() error {
	for _, cb := range b.preCallbacks {
		if err := cb(); err != nil {
			return err
		}
	}
	return nil
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
	b.payload = nil
	b.info = nil
	b.infobuf = nil
	b.wg = sync.WaitGroup{}
	b.t0 = time.Time{}
	b.printTime = false
	b.err = nil
	b.preCallbacks = nil
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
		logutil.Debugf("payload size is %d", b.GetPayloadSize())
		panic("wrong payload size")
	}
	_basePool.Put(b)
}

func (b *Base) GetPayload() []byte {
	return b.payload
}

func (b *Base) SetInfo(info any) {
	b.info = info
}

func (b *Base) GetInfo() any {
	return b.info
}

func (b *Base) UnmarshalFromNode(n []byte, own bool) error {
	if own {
		b.payload = n
	} else {
		copy(b.payload, n)
	}
	b.SetPayloadSize(len(b.payload))
	return nil
}

func (b *Base) SetPayload(buf []byte) error {
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

	b.payload = make([]byte, b.GetPayloadSize())
	n1 := 0
	if b.GetInfoSize() != 0 {
		infoBuf := make([]byte, b.GetInfoSize())
		n, err := r.Read(infoBuf)
		n1 += n
		if err != nil {
			return int64(n1), err
		}
		head := objectio.DecodeIOEntryHeader(b.descBuf)
		codec := objectio.GetIOEntryCodec(*head)
		vinfo, err := codec.Decode(infoBuf)
		info := vinfo.(*Info)
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

func (b *Base) UnmarshalBinary(buf []byte) (n int64, err error) {
	copy(b.GetMetaBuf(), buf[:b.GetMetaSize()])
	n += int64(b.GetMetaSize())

	infoSize := b.GetInfoSize()
	if infoSize != 0 {
		head := objectio.DecodeIOEntryHeader(b.descBuf)
		codec := objectio.GetIOEntryCodec(*head)
		vinfo, err := codec.Decode(buf[n : n+int64(infoSize)])
		info := vinfo.(*Info)
		if err != nil {
			return n, err
		}
		b.SetInfo(info)
		n += int64(infoSize)
	}

	payloadSize := b.GetPayloadSize()
	b.payload = buf[n : n+int64(payloadSize)]
	n += int64(payloadSize)
	return
}

func (b *Base) ReadAt(r *os.File, offset int) (int, error) {
	metaBuf := b.GetMetaBuf()
	n, err := r.ReadAt(metaBuf, int64(offset))
	if err != nil {
		return n, err
	}
	b.payload = make([]byte, b.GetPayloadSize())

	offset += len(b.GetMetaBuf())
	infoBuf := make([]byte, b.GetInfoSize())
	n1, err := r.ReadAt(infoBuf, int64(offset))
	if err != nil {
		return n + n1, err
	}
	offset += n1
	b.SetInfoBuf(infoBuf)

	head := objectio.DecodeIOEntryHeader(b.descBuf)
	codec := objectio.GetIOEntryCodec(*head)
	vinfo, err := codec.Decode(infoBuf)
	info := vinfo.(*Info)
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
	// delay and parallelize the marshal call
	b.RegisterPreCallback(func() error {
		if b.info == nil {
			return nil
		}
		buf, err := b.info.(*Info).Marshal()
		if err != nil {
			panic(err)
		}
		b.SetInfoBuf(buf)

		return nil
	})
}

func (b *Base) WriteTo(w io.Writer) (int64, error) {
	b.descriptor.SetVersion(IOET_WALEntry_CurrVer)
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
