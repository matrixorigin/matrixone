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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimestampIsEmpty(t *testing.T) {
	tests := []struct {
		ts      Timestamp
		isEmpty bool
	}{
		{Timestamp{}, true},
		{Timestamp{PhysicalTime: 1}, false},
		{Timestamp{LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
	}

	for _, tt := range tests {
		result := tt.ts.IsEmpty()
		assert.Equal(t, tt.isEmpty, result)
	}
}

func TestTimestampToStdTime(t *testing.T) {
	now := time.Now().UTC()
	ts := Timestamp{PhysicalTime: now.UnixNano()}
	result := ts.ToStdTime()
	assert.Equal(t, now, result)
}

func TestTimestampEqual(t *testing.T) {
	tests := []struct {
		lhs   Timestamp
		rhs   Timestamp
		equal bool
	}{
		{Timestamp{}, Timestamp{}, true},

		{Timestamp{}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{}, false},

		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 2}, false},
		{Timestamp{PhysicalTime: 2}, Timestamp{PhysicalTime: 1}, false},

		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 1}, true},
		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 2}, false},
		{Timestamp{LogicalTime: 2}, Timestamp{LogicalTime: 1}, false},

		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 1}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 2}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 2}, false},

		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 2}, false},
		{Timestamp{PhysicalTime: 2, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 2, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.equal, tt.lhs.Equal(tt.rhs))
	}
}

func TestTimestampLess(t *testing.T) {
	tests := []struct {
		lhs  Timestamp
		rhs  Timestamp
		less bool
	}{
		{Timestamp{}, Timestamp{}, false},
		{Timestamp{}, Timestamp{PhysicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1}, Timestamp{}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 2}, true},
		{Timestamp{PhysicalTime: 2}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 1}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 2}, true},
		{Timestamp{LogicalTime: 2}, Timestamp{LogicalTime: 1}, false},

		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 1}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 2}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 2}, true},

		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 2}, true},
		{Timestamp{PhysicalTime: 2, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 2, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.less, tt.lhs.Less(tt.rhs))
	}
}

func TestTimestampGreater(t *testing.T) {
	tests := []struct {
		lhs     Timestamp
		rhs     Timestamp
		greater bool
	}{
		{Timestamp{}, Timestamp{}, false},
		{Timestamp{}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{}, true},
		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 2}, false},
		{Timestamp{PhysicalTime: 2}, Timestamp{PhysicalTime: 1}, true},
		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 1}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 2}, false},
		{Timestamp{LogicalTime: 2}, Timestamp{LogicalTime: 1}, true},

		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 1}, true},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 2}, true},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 2}, false},

		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 2}, false},
		{Timestamp{PhysicalTime: 2, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, true},
		{Timestamp{PhysicalTime: 2, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, true},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.greater, tt.lhs.Greater(tt.rhs))
	}
}

func TestTimestampLessEq(t *testing.T) {
	tests := []struct {
		lhs    Timestamp
		rhs    Timestamp
		lessEq bool
	}{
		{Timestamp{}, Timestamp{}, true},
		{Timestamp{}, Timestamp{PhysicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1}, Timestamp{}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 2}, true},
		{Timestamp{PhysicalTime: 2}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 1}, true},
		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 2}, true},
		{Timestamp{LogicalTime: 2}, Timestamp{LogicalTime: 1}, false},

		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 1}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 2}, false},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 2}, true},

		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 2}, true},
		{Timestamp{PhysicalTime: 2, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 2, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, false},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.lessEq, tt.lhs.LessEq(tt.rhs))
	}
}

func TestTimestampGreaterEq(t *testing.T) {
	tests := []struct {
		lhs       Timestamp
		rhs       Timestamp
		greaterEq bool
	}{
		{Timestamp{}, Timestamp{}, true},
		{Timestamp{}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{}, true},
		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1}, Timestamp{PhysicalTime: 2}, false},
		{Timestamp{PhysicalTime: 2}, Timestamp{PhysicalTime: 1}, true},
		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 1}, true},
		{Timestamp{LogicalTime: 1}, Timestamp{LogicalTime: 2}, false},
		{Timestamp{LogicalTime: 2}, Timestamp{LogicalTime: 1}, true},

		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 1}, true},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1}, Timestamp{LogicalTime: 2}, true},
		{Timestamp{LogicalTime: 1}, Timestamp{PhysicalTime: 2}, false},

		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 1}, false},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 2, LogicalTime: 2}, false},
		{Timestamp{PhysicalTime: 2, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, true},
		{Timestamp{PhysicalTime: 1, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, true},
		{Timestamp{PhysicalTime: 2, LogicalTime: 2}, Timestamp{PhysicalTime: 1, LogicalTime: 1}, true},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.greaterEq, tt.lhs.GreaterEq(tt.rhs))
	}
}

func TestTimestampNext(t *testing.T) {
	tests := []struct {
		ts     Timestamp
		result Timestamp
	}{
		{Timestamp{}, Timestamp{LogicalTime: 1}},
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 2}},
		{Timestamp{PhysicalTime: 1234, LogicalTime: 234}, Timestamp{PhysicalTime: 1234, LogicalTime: 235}},
		{Timestamp{PhysicalTime: 123, LogicalTime: math.MaxUint32}, Timestamp{PhysicalTime: 124}},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.result, tt.ts.Next())
	}
}

func TestTimestampPrev(t *testing.T) {
	tests := []struct {
		ts     Timestamp
		result Timestamp
	}{
		{Timestamp{PhysicalTime: 1, LogicalTime: 1}, Timestamp{PhysicalTime: 1, LogicalTime: 0}},
		{Timestamp{PhysicalTime: 1234, LogicalTime: 234}, Timestamp{PhysicalTime: 1234, LogicalTime: 233}},
		{Timestamp{PhysicalTime: 123, LogicalTime: 0}, Timestamp{PhysicalTime: 122, LogicalTime: math.MaxUint32}},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.result, tt.ts.Prev())
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		value  string
		result Timestamp
		err    bool
	}{
		{"", Timestamp{}, true},
		{"1", Timestamp{}, true},
		{"a", Timestamp{}, true},
		{"1-", Timestamp{}, true},
		{"1-a", Timestamp{}, true},
		{"1-2", Timestamp{PhysicalTime: 1, LogicalTime: 2}, false},
	}

	for _, tt := range tests {
		ts, err := ParseTimestamp(tt.value)
		if !tt.err {
			assert.NoError(t, err)
			assert.Equal(t, tt.result, ts)
			continue
		}
		assert.Error(t, err)
	}
}
