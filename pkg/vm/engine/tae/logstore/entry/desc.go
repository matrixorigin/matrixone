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
)

const (
	PayloadSizeOffset = int(unsafe.Sizeof(ETInvalid))
	InfoSizeOffset    = int(unsafe.Sizeof(ETInvalid) + unsafe.Sizeof(uint32(0)))
	DescriptorSize    = int(unsafe.Sizeof(ETInvalid) + 2*unsafe.Sizeof(uint32(0)))
)

// type u16, payloadsize u32, infosize u32
type descriptor struct {
	descBuf []byte
}

func newDescriptor() *descriptor {
	return &descriptor{
		descBuf: make([]byte, DescriptorSize),
	}
}

func (desc *descriptor) IsFlush() bool {
	return desc.GetType() == ETFlush
}

func (desc *descriptor) IsCheckpoint() bool {
	return desc.GetType() == ETCheckpoint
}

func (desc *descriptor) SetType(t Type) {
	copy(desc.descBuf[:PayloadSizeOffset], types.EncodeUint16(&t))
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
	desc.SetType(ETInvalid)
	desc.SetPayloadSize(0)
	desc.SetInfoSize(0)
}

func (desc *descriptor) GetMetaBuf() []byte {
	return desc.descBuf
}

func (desc *descriptor) GetType() Type {
	return types.DecodeUint16(desc.descBuf[:PayloadSizeOffset])
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
