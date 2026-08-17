// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mpool

import (
	"encoding/binary"
	"math"
)

// AccountedBuffer is a non-copyable, allocation-accounted off-heap byte
// buffer. Reset retains physical capacity; Free is its terminal release.
type AccountedBuffer struct {
	data []byte
	mp   *MPool

	account       *AllocationAccount
	owner         AllocationOwner
	site          AllocationSite
	capacityClass AllocationCapacityClass
}

func NewAccountedBuffer(
	mp *MPool,
	account *AllocationAccount,
	owner AllocationOwner,
	site AllocationSite,
) (*AccountedBuffer, error) {
	return NewAccountedBufferWithCapacityClass(
		mp,
		account,
		owner,
		site,
		AllocationCapacityClassDefault,
	)
}

// NewAccountedBufferWithCapacityClass creates a reusable buffer whose physical
// allocations borrow the selected execution-local capacity.
func NewAccountedBufferWithCapacityClass(
	mp *MPool,
	account *AllocationAccount,
	owner AllocationOwner,
	site AllocationSite,
	capacityClass AllocationCapacityClass,
) (*AccountedBuffer, error) {
	if mp == nil {
		return nil, ErrAllocationAccountInvalid
	}
	request := allocationAccountRequest{
		account:       account,
		owner:         owner,
		site:          site,
		capacityClass: capacityClass,
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	return &AccountedBuffer{
		mp:            mp,
		account:       account,
		owner:         owner,
		site:          site,
		capacityClass: capacityClass,
	}, nil
}

func (b *AccountedBuffer) Bytes() []byte {
	if b == nil {
		return nil
	}
	return b.data
}

func (b *AccountedBuffer) Len() int {
	if b == nil {
		return 0
	}
	return len(b.data)
}

func (b *AccountedBuffer) Cap() int {
	if b == nil {
		return 0
	}
	return cap(b.data)
}

// EnsureCapacity admits and allocates an absolute retained capacity.
func (b *AccountedBuffer) EnsureCapacity(required int) error {
	if b == nil || b.mp == nil || b.account == nil || required < 0 {
		return ErrAllocationAccountInvalid
	}
	if required <= cap(b.data) {
		return nil
	}
	if int64(required) > maxAllocationSize() {
		return ErrAllocationAllocatorLimit
	}

	oldLength := len(b.data)
	if cap(b.data) == 0 {
		capacity, ok := GrowCapacity(0, int64(required))
		if !ok || capacity > int64(math.MaxInt) {
			return ErrAllocationAllocatorLimit
		}
		data, err := b.mp.AllocAccountedWithCapacityClass(
			int(capacity),
			b.account,
			b.owner,
			b.site,
			b.capacityClass,
		)
		if err != nil {
			return err
		}
		b.data = data[:oldLength]
		return nil
	}

	// Grow owns the capacity policy. Pass the caller's requirement instead of
	// applying GrowCapacity a second time to the already rounded capacity.
	data, err := b.mp.Grow(b.data, required, true)
	if err != nil {
		return err
	}
	b.data = data[:oldLength]
	return nil
}

// Resize changes the logical length after admitting any required retained
// capacity. Existing bytes are preserved.
func (b *AccountedBuffer) Resize(length int) error {
	if b == nil || length < 0 {
		return ErrAllocationAccountInvalid
	}
	if err := b.EnsureCapacity(length); err != nil {
		return err
	}
	b.data = b.data[:length]
	return nil
}

func (b *AccountedBuffer) Write(value []byte) (int, error) {
	if b == nil {
		return 0, ErrAllocationAccountInvalid
	}
	if len(value) > math.MaxInt-len(b.data) {
		return 0, ErrAllocationAccountInvalid
	}
	oldLength := len(b.data)
	required := oldLength + len(value)
	if err := b.EnsureCapacity(required); err != nil {
		return 0, err
	}
	b.data = b.data[:required]
	copy(b.data[oldLength:], value)
	return len(value), nil
}

func (b *AccountedBuffer) WriteString(value string) (int, error) {
	if b == nil || len(value) > math.MaxInt-len(b.data) {
		return 0, ErrAllocationAccountInvalid
	}
	oldLength := len(b.data)
	required := oldLength + len(value)
	if err := b.EnsureCapacity(required); err != nil {
		return 0, err
	}
	b.data = b.data[:required]
	copy(b.data[oldLength:], value)
	return len(value), nil
}

func (b *AccountedBuffer) appendSpace(length int) ([]byte, error) {
	if b == nil || length < 0 || length > math.MaxInt-len(b.data) {
		return nil, ErrAllocationAccountInvalid
	}
	start := len(b.data)
	if err := b.Resize(start + length); err != nil {
		return nil, err
	}
	return b.data[start:], nil
}

func (b *AccountedBuffer) WriteByte(value byte) error {
	dst, err := b.appendSpace(1)
	if err != nil {
		return err
	}
	dst[0] = value
	return nil
}

func (b *AccountedBuffer) WriteUint32(value uint32) error {
	dst, err := b.appendSpace(4)
	if err != nil {
		return err
	}
	binary.NativeEndian.PutUint32(dst, value)
	return nil
}

func (b *AccountedBuffer) WriteInt32(value int32) error {
	return b.WriteUint32(uint32(value))
}

func (b *AccountedBuffer) WriteUint64(value uint64) error {
	dst, err := b.appendSpace(8)
	if err != nil {
		return err
	}
	binary.NativeEndian.PutUint64(dst, value)
	return nil
}

func (b *AccountedBuffer) WriteInt64(value int64) error {
	return b.WriteUint64(uint64(value))
}

func (b *AccountedBuffer) SetUint32(offset int, value uint32) error {
	if b == nil || offset < 0 || offset > len(b.data)-4 {
		return ErrAllocationAccountInvalid
	}
	binary.NativeEndian.PutUint32(b.data[offset:offset+4], value)
	return nil
}

func (b *AccountedBuffer) SetInt64(offset int, value int64) error {
	if b == nil || offset < 0 || offset > len(b.data)-8 {
		return ErrAllocationAccountInvalid
	}
	binary.NativeEndian.PutUint64(b.data[offset:offset+8], uint64(value))
	return nil
}

func (b *AccountedBuffer) Reset() {
	if b != nil {
		b.data = b.data[:0]
	}
}

// Detach transfers the physical allocation to a caller that will release it
// through the returned MPool. The allocation header keeps its immutable
// account/owner/site provenance, so no counter or capacity ownership changes
// at handoff.
func (b *AccountedBuffer) Detach() ([]byte, *MPool, error) {
	if b == nil || b.mp == nil || b.account == nil {
		return nil, nil, ErrAllocationAccountInvalid
	}
	data, mp := b.data, b.mp
	b.data = nil
	b.mp = nil
	b.account = nil
	b.owner = 0
	b.site = 0
	b.capacityClass = AllocationCapacityClassDefault
	return data, mp, nil
}

func (b *AccountedBuffer) Free() {
	if b == nil {
		return
	}
	if cap(b.data) > 0 {
		b.mp.Free(b.data)
	}
	b.data = nil
	b.mp = nil
	b.account = nil
	b.owner = 0
	b.site = 0
	b.capacityClass = AllocationCapacityClassDefault
}
