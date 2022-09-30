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

package types

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

const (
	VarlenaInlineSize = 23
	VarlenaSize       = 24
	MaxStringSize     = 10485760
	VarlenaBigHdr     = 0xffffffff
)

func (v *Varlena) unsafePtr() unsafe.Pointer {
	return unsafe.Pointer(&v[0])
}

func (v *Varlena) byteSlice() []byte {
	svlen := (*v)[0]
	ptr := unsafe.Add(unsafe.Pointer(&v[0]), 1)
	return unsafe.Slice((*byte)(ptr), svlen)
}

func (v *Varlena) u32Slice() []uint32 {
	ptr := (*uint32)(v.unsafePtr())
	return unsafe.Slice(ptr, 6)
}

func (v *Varlena) offsetLen() (uint32, uint32) {
	s := v.u32Slice()
	return s[1], s[2]
}
func (v *Varlena) setOffsetLen(voff, vlen uint32) {
	s := v.u32Slice()
	s[0] = VarlenaBigHdr
	s[1] = voff
	s[2] = vlen
}

func BuildVarlena(bs []byte, area []byte, m *mpool.MPool) (Varlena, []byte, error) {
	var err error
	var v Varlena
	vlen := len(bs)
	if vlen <= VarlenaInlineSize {
		v[0] = byte(vlen)
		copy(v[1:1+vlen], bs)
		return v, area, nil
	} else {
		voff := len(area)
		if voff+vlen < cap(area) || m == nil {
			area = append(area, bs...)
		} else {
			area, err = m.Grow2(area, bs, voff+vlen)
			if err != nil {
				return v, nil, err
			}
		}
		v.setOffsetLen(uint32(voff), uint32(vlen))
		return v, area, nil
	}
}

func (v *Varlena) IsSmall() bool {
	return (*v)[0] <= VarlenaInlineSize
}

// For short slice, this one will return a slice stored internal
// in the varlena.   Caller must ensure that v has a longer life
// span than the returned byte slice.
//
// Main user of Varlena is vector.  v that comes from vector.Data
// will be fine.
func (v *Varlena) GetByteSlice(area []byte) []byte {
	svlen := (*v)[0]
	if svlen <= VarlenaInlineSize {
		return v.byteSlice()
	}
	voff, vlen := v.offsetLen()
	return area[voff : voff+vlen]
}

// See the lifespan comment above.
func (v *Varlena) GetString(area []byte) string {
	return string(v.GetByteSlice(area))
}

func (v *Varlena) Reset() {
	var vzero Varlena
	*v = vzero
}
