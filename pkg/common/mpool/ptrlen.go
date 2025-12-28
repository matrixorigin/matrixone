// Copyright 2021 - 2022 Matrix Origin
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

package mpool

import (
	"bytes"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/util"
)

// PtrLen is a known size slice.
type PtrLen struct {
	ptr unsafe.Pointer
	len int32
}

const PtrLenSize = int(unsafe.Sizeof(PtrLen{}))

func (p *PtrLen) Len() int {
	return int(p.len)
}

func (p *PtrLen) Ptr() unsafe.Pointer {
	return p.ptr
}

func (p *PtrLen) FromByteSlice(bs []byte) {
	p.ptr = unsafe.Pointer(&bs[0])
	p.len = int32(len(bs))
}

func (p *PtrLen) ToByteSlice() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(p.ptr)), int(p.len))
}

func PtrLenToSlice[T any](p *PtrLen) []T {
	var v T
	n := int(p.len) / int(unsafe.Sizeof(v))
	return unsafe.Slice((*T)(unsafe.Pointer(p.ptr)), n)
}

func PtrLenFromSlice[T any](p *PtrLen, slice []T) {
	p.ptr = unsafe.Pointer(&slice[0])
	p.len = int32(len(slice)) * int32(unsafe.Sizeof(slice[0]))
}

func AppendFixed[T any](mp *MPool, p *PtrLen, v T) error {
	bs := p.ToByteSlice()
	oldNb := len(bs)
	nb := oldNb + int(unsafe.Sizeof(v))
	bs, err := mp.ReallocZero(bs, int(nb), true)
	if err != nil {
		return err
	}
	copy(bs[oldNb:], util.UnsafeToBytes(&v))
	p.FromByteSlice(bs)
	return nil
}

func AppendFixedList[T comparable](mp *MPool, p *PtrLen, vs []T, distinct bool) error {
	if len(vs) == 0 {
		return nil
	}

	bs := p.ToByteSlice()
	oldNb := len(bs)
	var nb int
	var flags []bool

	if distinct {
		cnt := 0
		flags = make([]bool, len(vs))
		for i, v := range vs {
			flags[i] = PtrLenFindFixed(p, v) == -1
			if flags[i] {
				cnt++
			}
		}
		nb = oldNb + int(unsafe.Sizeof(vs[0]))*cnt
	} else {
		nb = oldNb + int(unsafe.Sizeof(vs[0]))*len(vs)
	}

	bs, err := mp.ReallocZero(bs, int(nb), true)
	if err != nil {
		return err
	}

	if distinct {
		for i, v := range vs {
			if flags[i] {
				copy(bs[oldNb:], util.UnsafeToBytes(&v))
				oldNb += int(unsafe.Sizeof(vs[0]))
			}
		}
	} else {
		copy(bs[oldNb:], util.UnsafeSliceToBytes(vs))
	}
	p.FromByteSlice(bs)
	return nil
}

func (p *PtrLen) AppendRawBytes(mp *MPool, bs []byte) error {
	if len(bs) == 0 {
		return nil
	}

	pbs := p.ToByteSlice()
	oldLen := len(pbs)
	nb := oldLen + len(bs)
	pbs, err := mp.ReallocZero(pbs, int(nb), true)
	if err != nil {
		return err
	}
	copy(pbs[oldLen:], bs)
	p.FromByteSlice(pbs)
	return nil
}

func (p *PtrLen) AppendBytes(mp *MPool, bs []byte) error {
	pbs := p.ToByteSlice()
	oldLen := len(pbs)
	nb := oldLen + 4 + len(bs)

	pbs, err := mp.ReallocZero(pbs, int(nb), true)
	if err != nil {
		return err
	}

	bssz := int32(len(bs))
	copy(pbs[oldLen:oldLen+4], util.UnsafeToBytes(&bssz))
	copy(pbs[oldLen+4:oldLen+4+len(bs)], bs)
	p.FromByteSlice(pbs)
	return nil
}

func (p *PtrLen) AppendBytesList(mp *MPool, other *PtrLen, distinct bool) error {
	pbs := p.ToByteSlice()
	otherBs := other.ToByteSlice()
	var addSz int32
	var toAdd [][]byte
	for i := 0; i < len(otherBs); {
		bssz := decodeI32(otherBs[i : i+4])
		val := otherBs[i+4 : i+4+int(bssz)]
		i += 4 + int(bssz)
		if distinct {
			if p.FindBytes(val) != -1 {
				continue
			}
		}
		toAdd = append(toAdd, val)
		addSz += int32(4 + bssz)
	}

	if len(toAdd) == 0 {
		return nil
	}

	oldNb := len(pbs)
	nb := oldNb + int(addSz)
	pbs, err := mp.ReallocZero(pbs, int(nb), true)
	if err != nil {
		return err
	}

	for _, val := range toAdd {
		bssz := int32(len(val))
		copy(pbs[oldNb:oldNb+4], util.UnsafeToBytes(&bssz))
		copy(pbs[oldNb+4:oldNb+4+int(bssz)], val)
		oldNb += int(4 + bssz)
	}
	p.FromByteSlice(pbs)
	return nil
}

func PtrLenFindFixed[T comparable](p *PtrLen, v T) int {
	ts := PtrLenToSlice[T](p)
	for i, t := range ts {
		if t == v {
			return i
		}
	}
	return -1
}

func decodeI32(v []byte) int32 {
	return *(*int32)(unsafe.Pointer(&v[0]))
}

func (p *PtrLen) FindBytes(bs []byte) int {
	pbs := p.ToByteSlice()
	for i := 0; i < len(pbs); {
		bssz := decodeI32(pbs[i : i+4])
		if bssz == int32(len(bs)) {
			if bytes.Equal(pbs[i+4:i+4+int(bssz)], bs) {
				return i
			}
		}
		i += 4 + int(bssz)
	}
	return -1
}

func (p *PtrLen) NumberOfVarlenElements() int {
	pbs := p.ToByteSlice()
	var cnt int
	for i := 0; i < len(pbs); {
		bssz := decodeI32(pbs[i : i+4])
		cnt += 1
		i += 4 + int(bssz)
	}
	return cnt
}
