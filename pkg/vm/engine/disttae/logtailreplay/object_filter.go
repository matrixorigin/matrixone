// Copyright 2024 Matrix Origin
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

package logtailreplay

import (
	"fmt"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
)

type Filter interface {
	Filter([]ObjectInfo) []ObjectInfo
}

func NewSmall(threshold uint32) Filter {
	return &small{threshold}
}

func NewOverlap(maxObjects int) Filter {
	return &overlap{maxEntries: maxObjects}
}

type small struct {
	threshold uint32
}

func (s *small) Filter(objs []ObjectInfo) []ObjectInfo {
	n := 0
	for _, obj := range objs {
		if obj.OriginSize() > s.threshold {
			continue
		}
		objs[n] = obj
		n++
	}
	objs = objs[:n]
	return objs
}

type overlap struct {
	t          types.T
	intervals  []entryInterval
	maxEntries int
}

func (o *overlap) Filter(objs []ObjectInfo) []ObjectInfo {
	if len(objs) == 0 {
		return nil
	}
	o.t = objs[0].SortKeyZoneMap().GetType()
	for _, obj := range objs {
		o.intervals = append(o.intervals, entryInterval{
			min:   obj.SortKeyZoneMap().GetMin(),
			max:   obj.SortKeyZoneMap().GetMax(),
			entry: obj,
		})
	}
	slices.SortFunc(o.intervals, func(a, b entryInterval) int {
		if c := compute.CompareGeneric(a.min, b.min, o.t); c != 0 {
			return c
		}
		return compute.CompareGeneric(a.max, b.max, o.t)
	})

	set := entrySet{entries: make([]ObjectInfo, 0), maxValue: minValue(o.t)}
	for _, interval := range o.intervals {
		if len(set.entries) == 0 || compute.CompareGeneric(set.maxValue, interval.min, o.t) > 0 {
			set.add(o.t, interval)
		} else if len(set.entries) == 1 {
			set.reset(o.t)
			set.add(o.t, interval)
		} else {
			break
		}
	}
	if len(set.entries) > o.maxEntries {
		return set.entries[:o.maxEntries]
	}
	return set.entries
}

func minValue(t types.T) any {
	switch t {
	case types.T_bit:
		return 0
	case types.T_int8:
		return int8(math.MinInt8)
	case types.T_int16:
		return int16(math.MinInt16)
	case types.T_int32:
		return int32(math.MinInt32)
	case types.T_int64:
		return int64(math.MinInt64)
	case types.T_uint8:
		return uint8(0)
	case types.T_uint16:
		return uint16(0)
	case types.T_uint32:
		return uint32(0)
	case types.T_uint64:
		return uint64(0)
	case types.T_float32:
		return float32(-math.MaxFloat32)
	case types.T_float64:
		return float64(-math.MaxFloat64)
	case types.T_date:
		return types.Date(math.MinInt32)
	case types.T_time:
		return types.Time(math.MinInt64)
	case types.T_datetime:
		return types.Datetime(math.MinInt64)
	case types.T_timestamp:
		return types.Timestamp(math.MinInt64)
	case types.T_enum:
		return types.Enum(0)
	case types.T_decimal64:
		return types.Decimal64Min
	case types.T_decimal128:
		return types.Decimal128Min
	case types.T_uuid:
		return types.Uuid{}
	case types.T_TS:
		return types.TS{}
	case types.T_Rowid:
		return types.Rowid{}
	case types.T_Blockid:
		return types.Blockid{}
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		return []byte{}
	default:
		panic(fmt.Sprintf("unsupported type: %v", t))
	}
}

type entryInterval struct {
	min, max any
	entry    ObjectInfo
}

type entrySet struct {
	entries  []ObjectInfo
	maxValue any
}

func (s *entrySet) reset(t types.T) {
	s.entries = s.entries[:0]
	s.maxValue = minValue(t)
}

func (s *entrySet) add(t types.T, entry entryInterval) {
	s.entries = append(s.entries, entry.entry)
	if compute.CompareGeneric(s.maxValue, entry.max, t) < 0 {
		s.maxValue = entry.max
	}
}
