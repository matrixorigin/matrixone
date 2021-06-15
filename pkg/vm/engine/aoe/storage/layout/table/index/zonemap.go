package index

import (
	"bytes"
	"matrixone/pkg/container/types"
)

// TODO: Just for temp test
type ZoneMapIndex struct {
	T    types.Type
	MinV interface{}
	MaxV interface{}
}

func NewZoneMap(t types.Type, minv, maxv interface{}) Index {
	return &ZoneMapIndex{
		T:    t,
		MinV: minv,
		MaxV: maxv,
	}
}

func (i *ZoneMapIndex) Type() Type {
	return ZoneMap
}

func (i *ZoneMapIndex) Eq(v interface{}) bool {
	switch i.T.Oid {
	case types.T_int8:
		return v.(int8) >= i.MinV.(int8) && v.(int8) <= i.MaxV.(int8)
	case types.T_int16:
		return v.(int16) >= i.MinV.(int16) && v.(int16) <= i.MaxV.(int16)
	case types.T_int32:
		return v.(int32) >= i.MinV.(int32) && v.(int32) <= i.MaxV.(int32)
	case types.T_int64:
		return v.(int64) >= i.MinV.(int64) && v.(int64) <= i.MaxV.(int64)
	case types.T_uint8:
		return v.(uint8) >= i.MinV.(uint8) && v.(uint8) <= i.MaxV.(uint8)
	case types.T_uint16:
		return v.(uint16) >= i.MinV.(uint16) && v.(uint16) <= i.MaxV.(uint16)
	case types.T_uint32:
		return v.(uint32) >= i.MinV.(uint32) && v.(uint32) <= i.MaxV.(uint32)
	case types.T_uint64:
		return v.(uint64) >= i.MinV.(uint64) && v.(uint64) <= i.MaxV.(uint64)
	case types.T_decimal:
		panic("not supported")
	case types.T_float32:
		return v.(float32) >= i.MinV.(float32) && v.(float32) <= i.MaxV.(float32)
	case types.T_float64:
		return v.(float64) >= i.MinV.(float64) && v.(float64) <= i.MaxV.(float64)
	case types.T_date:
		panic("not supported")
	case types.T_datetime:
		panic("not supported")
	case types.T_sel:
		return v.(int64) >= i.MinV.(int64) && v.(int64) <= i.MaxV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		if bytes.Compare(v.([]byte), i.MinV.([]byte)) < 0 {
			return false
		}
		if bytes.Compare(v.([]byte), i.MaxV.([]byte)) > 0 {
			return false
		}
		return true
	}
	panic("not supported")
}

func (i *ZoneMapIndex) Ne(v interface{}) bool {
	return !i.Eq(v)
}

func (i *ZoneMapIndex) Lt(v interface{}) bool {
	panic("TODO")
}

func (i *ZoneMapIndex) Le(v interface{}) bool {
	panic("TODO")
}

func (i *ZoneMapIndex) Gt(v interface{}) bool {
	panic("TODO")
}

func (i *ZoneMapIndex) Ge(v interface{}) bool {
	panic("TODO")
}

func (i *ZoneMapIndex) Btw(v interface{}) bool {
	panic("TODO")
}
