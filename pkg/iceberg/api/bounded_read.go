// Copyright 2026 Matrix Origin
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

package api

import (
	"io"
	"math"
)

// ErrMaterializationLimitExceeded is returned when ReadAllBounded observes
// more input than the caller's retained-memory allowance.
var ErrMaterializationLimitExceeded = NewError(ErrPlanningLimitExceeded, "Iceberg materialization limit exceeded", nil)

// ReadAllBounded materializes a stream without allowing either the returned
// slice or a make+copy growth peak to exceed maxBytes. A fragmented stream may
// therefore be rejected before its visible length reaches maxBytes: the old and
// new backing arrays coexist during growth. This conservative meaning is
// intentional for callers using maxBytes as an OOM safety boundary.
func ReadAllBounded(reader io.Reader, maxBytes int64) ([]byte, error) {
	if reader == nil {
		return nil, NewError(ErrInternal, "Iceberg bounded reader is nil", nil)
	}
	if maxBytes <= 0 {
		return nil, ErrMaterializationLimitExceeded
	}
	if maxBytes > int64(math.MaxInt) {
		maxBytes = int64(math.MaxInt)
	}
	buffer := boundedReadBuffer{maxBytes: int(maxBytes)}
	if _, err := io.Copy(&buffer, reader); err != nil {
		return nil, err
	}
	return buffer.data, nil
}

type boundedReadBuffer struct {
	data     []byte
	maxBytes int
}

func (b *boundedReadBuffer) Write(payload []byte) (int, error) {
	if len(payload) > b.maxBytes-len(b.data) {
		return 0, ErrMaterializationLimitExceeded
	}
	required := len(b.data) + len(payload)
	if required > cap(b.data) {
		nextCapacity := cap(b.data) * 2
		if nextCapacity < 512 {
			nextCapacity = 512
		}
		if nextCapacity < required {
			nextCapacity = required
		}
		if nextCapacity > b.maxBytes {
			nextCapacity = b.maxBytes
		}
		if cap(b.data) > b.maxBytes-nextCapacity {
			return 0, ErrMaterializationLimitExceeded
		}
		next := make([]byte, len(b.data), nextCapacity)
		copy(next, b.data)
		b.data = next
	}
	start := len(b.data)
	b.data = b.data[:required]
	copy(b.data[start:], payload)
	return len(payload), nil
}
