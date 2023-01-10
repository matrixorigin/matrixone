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

package timestamp

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

/*
// String returns the string representation of the HLC timestamp.
func (lhs Timestamp) String() string {
	panic("not implemented")
}
*/

// IsEmpty returns a boolean value indicating whether the current timestamp
// is an empty value.
func (m Timestamp) IsEmpty() bool {
	return m.PhysicalTime == 0 && m.LogicalTime == 0
}

// ToStdTime converts the HLC timestamp to a regular golang stdlib UTC
// timestamp. The logical time component of the HLC is lost after the
// conversion.
func (m Timestamp) ToStdTime() time.Time {
	return time.Unix(0, m.PhysicalTime).UTC()
}

// Equal returns a boolean value indicating whether the lhs timestamp equals
// to the rhs timestamp.
func (m Timestamp) Equal(rhs Timestamp) bool {
	return m.PhysicalTime == rhs.PhysicalTime &&
		m.LogicalTime == rhs.LogicalTime
}

// Less returns a boolean value indicating whether the lhs timestamp is less
// than the rhs timestamp value.
func (m Timestamp) Less(rhs Timestamp) bool {
	return m.PhysicalTime < rhs.PhysicalTime ||
		(m.PhysicalTime == rhs.PhysicalTime && m.LogicalTime < rhs.LogicalTime)
}

// Greater returns a boolean value indicating whether the lhs timestamp is
// greater than the rhs timestamp value.
func (m Timestamp) Greater(rhs Timestamp) bool {
	return m.PhysicalTime > rhs.PhysicalTime ||
		(m.PhysicalTime == rhs.PhysicalTime && m.LogicalTime > rhs.LogicalTime)
}

// LessEq returns a boolean value indicating whether the lhs timestamp is
// less than or equal to the rhs timestamp value.
func (m Timestamp) LessEq(rhs Timestamp) bool {
	return m.Less(rhs) || m.Equal(rhs)
}

// GreaterEq returns a boolean value indicating whether the lhs timestamp is
// greater than or equal to the rhs timestamp value.
func (m Timestamp) GreaterEq(rhs Timestamp) bool {
	return m.Greater(rhs) || m.Equal(rhs)
}

// Next returns the smallest timestamp that is greater than the current
// timestamp.
func (m Timestamp) Next() Timestamp {
	if m.LogicalTime == math.MaxUint32 {
		return Timestamp{PhysicalTime: m.PhysicalTime + 1}
	}

	return Timestamp{
		PhysicalTime: m.PhysicalTime,
		LogicalTime:  m.LogicalTime + 1,
	}
}

// Prev returns the smallest timestamp that is less than the current
// timestamp.
func (m Timestamp) Prev() Timestamp {
	if m.LogicalTime == 0 {
		return Timestamp{PhysicalTime: m.PhysicalTime - 1, LogicalTime: math.MaxUint32}
	}

	return Timestamp{
		PhysicalTime: m.PhysicalTime,
		LogicalTime:  m.LogicalTime - 1,
	}
}

// DebugString returns debug string
func (m Timestamp) DebugString() string {
	return fmt.Sprintf("%d-%d", m.PhysicalTime, m.LogicalTime)
}

func (m Timestamp) ProtoSize() int {
	return m.Size()
}

// ParseTimestamp parse timestamp from debug string
func ParseTimestamp(value string) (Timestamp, error) {
	values := strings.Split(value, "-")
	if len(values) != 2 {
		return Timestamp{}, moerr.NewInvalidInputNoCtx("invalid debug timestamp string: %s", value)
	}

	p, err := format.ParseStringInt64(values[0])
	if err != nil {
		return Timestamp{}, moerr.NewInvalidInputNoCtx("invalid debug timestamp string: %s", value)
	}
	l, err := format.ParseStringUint32(values[1])
	if err != nil {
		return Timestamp{}, moerr.NewInvalidInputNoCtx("invalid debug timestamp string: %s", value)
	}
	return Timestamp{PhysicalTime: p, LogicalTime: l}, nil
}
