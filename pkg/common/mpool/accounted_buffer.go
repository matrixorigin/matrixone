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
	"math"
)

// AccountedBuffer is a non-copyable, allocation-accounted off-heap byte
// buffer. Reset retains physical capacity; Free is its terminal release.
type AccountedBuffer struct {
	data []byte
	mp   *MPool

	account *AllocationAccount
	owner   AllocationOwner
	site    AllocationSite
}

func NewAccountedBuffer(
	mp *MPool,
	account *AllocationAccount,
	owner AllocationOwner,
	site AllocationSite,
) (*AccountedBuffer, error) {
	if mp == nil {
		return nil, ErrAllocationAccountInvalid
	}
	request := allocationAccountRequest{
		account: account,
		owner:   owner,
		site:    site,
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	return &AccountedBuffer{
		mp:      mp,
		account: account,
		owner:   owner,
		site:    site,
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
		return ErrAllocationAccountInvalid
	}

	oldLength := len(b.data)
	capacity, ok := GrowCapacity(int64(cap(b.data)), int64(required))
	if !ok || capacity > int64(math.MaxInt) {
		return ErrAllocationAccountInvalid
	}
	if cap(b.data) == 0 {
		data, err := b.mp.AllocAccounted(
			int(capacity),
			b.account,
			b.owner,
			b.site,
		)
		if err != nil {
			return err
		}
		b.data = data[:oldLength]
		return nil
	}

	data, err := b.mp.Grow(b.data, int(capacity), true)
	if err != nil {
		return err
	}
	b.data = data[:oldLength]
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

func (b *AccountedBuffer) Reset() {
	if b != nil {
		b.data = b.data[:0]
	}
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
}
