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
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// type    uint16
// version uint16
// payload size uint32
// info size uint32
const (
	VersionOffset     = int(unsafe.Sizeof(uint16(0)))
	PayloadSizeOffset = VersionOffset + int(unsafe.Sizeof(IOET_WALEntry_Invalid))
	InfoSizeOffset    = VersionOffset + int(unsafe.Sizeof(IOET_WALEntry_Invalid)+unsafe.Sizeof(uint32(0)))
	DescriptorSize    = VersionOffset + int(unsafe.Sizeof(IOET_WALEntry_Invalid)+2*unsafe.Sizeof(uint32(0)))
)

const (
	IOET_WALEntry_V1              uint16 = 1
	IOET_WALEntry_Invalid         uint16 = 2000
	IOET_WALEntry_Checkpoint      uint16 = 2001
	IOET_WALEntry_PostCommit      uint16 = 2002
	IOET_WALEntry_Uncommitted     uint16 = 2003
	IOET_WALEntry_Txn             uint16 = 2004
	IOET_WALEntry_Test            uint16 = 2005
	IOET_WALEntry_CustomizedStart uint16 = 2006

	IOET_WALEntry_CurrVer = IOET_WALEntry_V1
)

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALEntry_Checkpoint,
			Version: IOET_WALEntry_V1,
		}, nil, UnmarshalEntry,
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALEntry_PostCommit,
			Version: IOET_WALEntry_V1,
		}, nil, UnmarshalEntry,
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALEntry_Uncommitted,
			Version: IOET_WALEntry_V1,
		}, nil, UnmarshalEntry,
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALEntry_Test,
			Version: IOET_WALEntry_V1,
		}, nil, UnmarshalEntry,
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALEntry_Txn,
			Version: IOET_WALEntry_V1,
		}, nil, UnmarshalEntry,
	)
}

func UnmarshalEntry(b []byte) (any, error) {
	info := NewEmptyInfo()
	err := info.Unmarshal(b)
	return info, err
}

// type u16, version u16, payloadsize u32, infosize u32
type descriptor struct {
	descBuf []byte
}

func newDescriptor() *descriptor {
	return &descriptor{
		descBuf: make([]byte, DescriptorSize),
	}
}

func (desc *descriptor) SetVersion(t uint16) {
	copy(desc.descBuf[VersionOffset:PayloadSizeOffset], types.EncodeUint16(&t))
}

func (desc *descriptor) SetType(t Type) {
	copy(desc.descBuf[:VersionOffset], types.EncodeUint16(&t))
}

func (desc *descriptor) SetPayloadSize(size int) {
	s := uint32(size)
	copy(desc.descBuf[PayloadSizeOffset:InfoSizeOffset], types.EncodeUint32(&s))
}

func (desc *descriptor) SetInfoSize(size int) {
	s := uint32(size)
	copy(desc.descBuf[InfoSizeOffset:], types.EncodeUint32(&s))
}

func (desc *descriptor) reset() {
	desc.SetType(IOET_WALEntry_Invalid)
	desc.SetPayloadSize(0)
	desc.SetInfoSize(0)
}

func (desc *descriptor) GetMetaBuf() []byte {
	return desc.descBuf
}

func (desc *descriptor) GetVersion() uint16 {
	return types.DecodeUint16(desc.descBuf[VersionOffset:PayloadSizeOffset])
}

func (desc *descriptor) GetType() Type {
	return types.DecodeUint16(desc.descBuf[:VersionOffset])
}

func (desc *descriptor) GetPayloadSize() int {
	return int(types.DecodeUint32(desc.descBuf[PayloadSizeOffset:InfoSizeOffset]))
}

func (desc *descriptor) GetInfoSize() int {
	return int(types.DecodeUint32(desc.descBuf[InfoSizeOffset:]))
}

func (desc *descriptor) GetMetaSize() int {
	return DescriptorSize
}
func (desc *descriptor) TotalSize() int {
	return DescriptorSize + desc.GetPayloadSize() + desc.GetInfoSize()
}

func (desc *descriptor) TotalSizeExpectMeta() int {
	return desc.GetPayloadSize() + desc.GetInfoSize()
}

func (desc *descriptor) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(desc.descBuf)
	return int64(n), err
}

func (desc *descriptor) ReadMeta(r io.Reader) (int, error) {
	return r.Read(desc.descBuf)
}
