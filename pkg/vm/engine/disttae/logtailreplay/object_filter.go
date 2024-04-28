package logtailreplay

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type Filter interface {
	Filter([]ObjectInfo) []ObjectInfo
}

func NewSmall() Filter {
	return &small{110 * common.Const1MBytes}
}

func NewOverlap() Filter {
	return &overlap{}
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
	t         types.T
	intervals []entryInterval
}

func (o *overlap) Filter(objs []ObjectInfo) []ObjectInfo {
	if len(objs) == 0 {
		return nil
	}
	o.t = objs[0].SortKeyZoneMap().GetType()
	for _, obj := range objs {
		obj := obj
		o.intervals = append(o.intervals, entryInterval{
			min:   obj.SortKeyZoneMap().GetMin(),
			max:   obj.SortKeyZoneMap().GetMax(),
			entry: obj,
		})

	}
	slices.SortFunc(o.intervals, func(a, b entryInterval) int {
		if c := compare(o.t, a.min, b.min); c != 0 {
			return c
		}
		return compare(o.t, a.max, b.max)
	})

	set := entrySet{entries: make([]ObjectInfo, 0), maxValue: minValue(o.t)}
	for _, interval := range o.intervals {
		interval := interval
		logutil.Infof("Mergeblocks %v %v", interval.min, interval.max)
		if len(set.entries) == 0 || compare(o.t, set.maxValue, interval.min) > 0 {
			set.add(o.t, interval)
		} else if len(set.entries) == 1 {
			set.reset(o.t)
			set.add(o.t, interval)
		} else {
			return set.entries
		}
	}
	return nil
}

func compare(t types.T, a, b any) int {
	switch t {
	case types.T_bit:
		return cmp.Compare(a.(uint64), b.(uint64))
	case types.T_int8:
		return cmp.Compare(a.(int8), b.(int8))
	case types.T_int16:
		return cmp.Compare(a.(int16), b.(int16))
	case types.T_int32:
		return cmp.Compare(a.(int32), b.(int32))
	case types.T_int64:
		return cmp.Compare(a.(int64), b.(int64))
	case types.T_uint8:
		return cmp.Compare(a.(uint8), b.(uint8))
	case types.T_uint16:
		return cmp.Compare(a.(uint16), b.(uint16))
	case types.T_uint32:
		return cmp.Compare(a.(uint32), b.(uint32))
	case types.T_uint64:
		return cmp.Compare(a.(uint64), b.(uint64))
	case types.T_float32:
		return cmp.Compare(a.(float32), b.(float32))
	case types.T_float64:
		return cmp.Compare(a.(float64), b.(float64))
	case types.T_date:
		return cmp.Compare(a.(types.Date), b.(types.Date))
	case types.T_time:
		return cmp.Compare(a.(types.Time), b.(types.Time))
	case types.T_datetime:
		return cmp.Compare(a.(types.Datetime), b.(types.Datetime))
	case types.T_timestamp:
		return cmp.Compare(a.(types.Timestamp), b.(types.Timestamp))
	case types.T_enum:
		return cmp.Compare(a.(types.Enum), b.(types.Enum))
	case types.T_decimal64:
		return a.(types.Decimal64).Compare(b.(types.Decimal64))
	case types.T_decimal128:
		return a.(types.Decimal128).Compare(b.(types.Decimal128))
	case types.T_uuid:
		return a.(types.Uuid).Compare(b.(types.Uuid))
	case types.T_TS:
		ts1 := a.(types.TS)
		ts2 := b.(types.TS)
		return ts1.Compare(&ts2)
	case types.T_Rowid:
		return a.(types.Rowid).Compare(b.(types.Rowid))
	case types.T_Blockid:
		return a.(types.Blockid).Compare(b.(types.Blockid))
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		return bytes.Compare(a.([]byte), b.([]byte))
	default:
		panic(fmt.Sprintf("unsupported type: %v", t))
	}
}

func minValue(t types.T) any {
	switch t {
	case types.T_bit:
		return 0
	case types.T_int8:
		return math.MinInt8
	case types.T_int16:
		return math.MinInt16
	case types.T_int32:
		return math.MinInt32
	case types.T_int64:
		return math.MinInt64
	case types.T_uint8:
		return 0
	case types.T_uint16:
		return 0
	case types.T_uint32:
		return 0
	case types.T_uint64:
		return 0
	case types.T_float32:
		return -math.MaxFloat32
	case types.T_float64:
		return -math.MaxFloat64
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
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
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
	if compare(t, s.maxValue, entry.max) < 0 {
		s.maxValue = entry.max
	}
}
