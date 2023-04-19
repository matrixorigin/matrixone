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

package objectio

import "fmt"

const (
	IOET_Empty   = 0
	IOET_ObjMeta = 1
	IOET_ColData = 2
	IOET_BF      = 3
	IOET_ZM      = 4
)

type IOEntryHeader struct {
	Type, Version uint16
}

func (h IOEntryHeader) String() string {
	return fmt.Sprintf("IOEntry[%d,%d]", h.Type, h.Version)
}

type IOEntry interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
}

type IOEncodeFunc = func(any) ([]byte, error)
type IODecodeFunc = func([]byte) (any, error)

type ioEntryCodec struct {
	// if encFn is nil, no need to encode
	encFn IOEncodeFunc

	// if decFn is nil, no need to decode
	decFn IODecodeFunc
}

func (codec ioEntryCodec) NoMarshal() bool {
	return codec.encFn == nil
}

func (codec ioEntryCodec) NoUnmarshal() bool {
	return codec.decFn == nil
}

var ioEntryCodecs = map[IOEntryHeader]ioEntryCodec{}

func RegisterIOEnrtyCodec(h IOEntryHeader, encFn IOEncodeFunc, decFn IODecodeFunc) {
	_, ok := ioEntryCodecs[h]
	if ok {
		panic(fmt.Sprintf("duplicate io entry codec found: %s", h.String()))
	}
	ioEntryCodecs[h] = ioEntryCodec{
		encFn: encFn,
		decFn: decFn,
	}
}

func GetIOEntryCodec(h IOEntryHeader) (codec ioEntryCodec) {
	var ok bool
	codec, ok = ioEntryCodecs[h]
	if !ok {
		panic(fmt.Sprintf("no codec found for: %s", h.String()))
	}
	return
}
