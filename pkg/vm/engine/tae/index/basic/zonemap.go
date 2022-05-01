package basic

import (
	"bytes"
	"strconv"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
)

type ZoneMap struct {
	mu          *sync.RWMutex
	typ         types.Type
	min         interface{}
	max         interface{}
	initialized bool
}

func NewZoneMap(typ types.Type, mutex *sync.RWMutex) *ZoneMap {
	if mutex == nil {
		mutex = new(sync.RWMutex)
	}
	zm := &ZoneMap{typ: typ, mu: mutex, initialized: false}
	return zm
}

func NewZoneMapFromSource(data []byte) (*ZoneMap, error) {
	zm := ZoneMap{}
	if err := zm.Unmarshal(data); err != nil {
		return nil, err
	}
	return &zm, nil
}

func (zm *ZoneMap) GetType() types.Type {
	return zm.typ
}

func (zm *ZoneMap) Initialized() bool {
	zm.mu.RLock()
	defer zm.mu.RUnlock()
	return zm.initialized
}

func (zm *ZoneMap) Update(v interface{}) error {
	zm.mu.Lock()
	defer zm.mu.Unlock()
	return zm.UpdateLocked(v)
}

func (zm *ZoneMap) UpdateLocked(v interface{}) error {
	if !zm.initialized {
		zm.min = v
		zm.max = v
		zm.initialized = true
		return nil
	}
	if common.CompareGeneric(v, zm.max, zm.typ) > 0 {
		zm.max = v
	}
	if common.CompareGeneric(v, zm.min, zm.typ) < 0 {
		zm.min = v
	}
	return nil
}

func (zm *ZoneMap) BatchUpdate(vec *vector.Vector, offset uint32, length int) error {
	if !zm.typ.Eq(vec.Typ) {
		return errors.ErrTypeMismatch
	}
	zm.mu.Lock()
	defer zm.mu.Unlock()
	if err := common.ProcessVector(vec, offset, length, zm.UpdateLocked, nil); err != nil {
		return err
	}
	return nil
}

func (zm *ZoneMap) Query(key interface{}) (int, error) {
	// TODO: mismatch error
	zm.mu.RLock()
	defer zm.mu.RUnlock()
	if !zm.initialized {
		return 1, nil
	}
	max := zm.GetMaxLocked()
	min := zm.GetMinLocked()
	gt := common.CompareGeneric(key, max, zm.typ) > 0
	lt := common.CompareGeneric(key, min, zm.typ) < 0
	if gt {
		return 1, nil
	}
	if lt {
		return -1, nil
	}
	return 0, nil
}

func (zm *ZoneMap) MayContainsKey(key interface{}) (bool, error) {
	// TODO: mismatch error
	zm.mu.RLock()
	defer zm.mu.RUnlock()
	if !zm.initialized {
		return false, nil
	}
	max := zm.GetMaxLocked()
	min := zm.GetMinLocked()
	if common.CompareGeneric(key, max, zm.typ) > 0 || common.CompareGeneric(key, min, zm.typ) < 0 {
		return false, nil
	}
	return true, nil
}

func (zm *ZoneMap) MayContainsAnyKeys(keys *vector.Vector) (bool, *roaring.Bitmap, error) {
	// TODO: mismatch error
	zm.mu.RLock()
	defer zm.mu.RUnlock()
	ans := roaring.NewBitmap()
	res := false
	if !zm.initialized {
		return false, ans, nil
	}
	max := zm.GetMaxLocked()
	min := zm.GetMinLocked()
	row := uint32(0)
	process := func(key interface{}) error {
		if common.CompareGeneric(key, max, zm.typ) <= 0 && common.CompareGeneric(key, min, zm.typ) >= 0 {
			ans.Add(row)
		}
		row++
		return nil
	}
	if err := common.ProcessVector(keys, 0, -1, process, nil); err != nil {
		return false, ans, err
	}
	if ans.GetCardinality() != 0 {
		res = true
	}
	return res, ans, nil
}

func (zm *ZoneMap) GetMax() interface{} {
	zm.mu.RLock()
	defer zm.mu.RUnlock()
	return zm.GetMaxLocked()
}

func (zm *ZoneMap) SetMax(v interface{}) {
	zm.mu.Lock()
	defer zm.mu.Unlock()
	if !zm.initialized {
		zm.min = v
		zm.max = v
		zm.initialized = true
		return
	}
	if common.CompareGeneric(v, zm.max, zm.typ) > 0 {
		zm.max = v
	}
}

func (zm *ZoneMap) GetMaxLocked() interface{} {
	return zm.max
}

func (zm *ZoneMap) GetMin() interface{} {
	zm.mu.RLock()
	defer zm.mu.RUnlock()
	return zm.GetMinLocked()
}

func (zm *ZoneMap) SetMin(v interface{}) {
	zm.mu.Lock()
	defer zm.mu.Unlock()
	if !zm.initialized {
		zm.min = v
		zm.max = v
		zm.initialized = true
		return
	}
	if common.CompareGeneric(v, zm.min, zm.typ) < 0 {
		zm.min = v
	}
}

func (zm *ZoneMap) GetMinLocked() interface{} {
	return zm.min
}

func (zm *ZoneMap) Print() string {
	// TODO: support all types
	zm.mu.RLock()
	defer zm.mu.RUnlock()
	// default int32
	s := "<ZM>\n["
	s += strconv.Itoa(int(zm.min.(int32)))
	s += ","
	s += strconv.Itoa(int(zm.max.(int32)))
	s += "]\n"
	s += "</ZM>"
	return s
}

func (zm *ZoneMap) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeType(zm.typ))
	if !zm.initialized {
		buf.Write(encoding.EncodeInt8(0))
		return buf.Bytes(), nil
	}
	buf.Write(encoding.EncodeInt8(1))
	switch zm.typ.Oid {
	case types.T_int8:
		buf.Write(encoding.EncodeInt8(zm.min.(int8)))
		buf.Write(encoding.EncodeInt8(zm.max.(int8)))
		return buf.Bytes(), nil
	case types.T_int16:
		buf.Write(encoding.EncodeInt16(zm.min.(int16)))
		buf.Write(encoding.EncodeInt16(zm.max.(int16)))
		return buf.Bytes(), nil
	case types.T_int32:
		buf.Write(encoding.EncodeInt32(zm.min.(int32)))
		buf.Write(encoding.EncodeInt32(zm.max.(int32)))
		return buf.Bytes(), nil
	case types.T_int64:
		buf.Write(encoding.EncodeInt64(zm.min.(int64)))
		buf.Write(encoding.EncodeInt64(zm.max.(int64)))
		return buf.Bytes(), nil
	case types.T_uint8:
		buf.Write(encoding.EncodeUint8(zm.min.(uint8)))
		buf.Write(encoding.EncodeUint8(zm.max.(uint8)))
		return buf.Bytes(), nil
	case types.T_uint16:
		buf.Write(encoding.EncodeUint16(zm.min.(uint16)))
		buf.Write(encoding.EncodeUint16(zm.max.(uint16)))
		return buf.Bytes(), nil
	case types.T_uint32:
		buf.Write(encoding.EncodeUint32(zm.min.(uint32)))
		buf.Write(encoding.EncodeUint32(zm.max.(uint32)))
		return buf.Bytes(), nil
	case types.T_uint64:
		buf.Write(encoding.EncodeUint64(zm.min.(uint64)))
		buf.Write(encoding.EncodeUint64(zm.max.(uint64)))
		return buf.Bytes(), nil
	case types.T_float32:
		buf.Write(encoding.EncodeFloat32(zm.min.(float32)))
		buf.Write(encoding.EncodeFloat32(zm.max.(float32)))
		return buf.Bytes(), nil
	case types.T_float64:
		buf.Write(encoding.EncodeFloat64(zm.min.(float64)))
		buf.Write(encoding.EncodeFloat64(zm.max.(float64)))
		return buf.Bytes(), nil
	case types.T_date:
		buf.Write(encoding.EncodeDate(zm.min.(types.Date)))
		buf.Write(encoding.EncodeDate(zm.max.(types.Date)))
		return buf.Bytes(), nil
	case types.T_datetime:
		buf.Write(encoding.EncodeDatetime(zm.min.(types.Datetime)))
		buf.Write(encoding.EncodeDatetime(zm.max.(types.Datetime)))
		return buf.Bytes(), nil
	case types.T_char, types.T_varchar, types.T_json:
		minv := zm.min.([]byte)
		maxv := zm.max.([]byte)
		buf.Write(encoding.EncodeInt16(int16(len(minv))))
		buf.Write(minv)
		buf.Write(encoding.EncodeInt16(int16(len(maxv))))
		buf.Write(maxv)
		return buf.Bytes(), nil
	}
	panic("unsupported")
}

func (zm *ZoneMap) Unmarshal(buf []byte) error {
	zm.typ = encoding.DecodeType(buf[:encoding.TypeSize])
	buf = buf[encoding.TypeSize:]
	init := encoding.DecodeInt8(buf[:1])
	buf = buf[1:]
	zm.mu = new(sync.RWMutex)
	if init == 0 {
		zm.initialized = false
		return nil
	}
	zm.initialized = true
	switch zm.typ.Oid {
	case types.T_int8:
		zm.min = encoding.DecodeInt8(buf[:1])
		buf = buf[1:]
		zm.max = encoding.DecodeInt8(buf[:1])
		return nil
	case types.T_int16:
		zm.min = encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		zm.max = encoding.DecodeInt16(buf[:2])
		return nil
	case types.T_int32:
		zm.min = encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeInt32(buf[:4])
		return nil
	case types.T_int64:
		zm.min = encoding.DecodeInt64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeInt64(buf[:8])
		return nil
	case types.T_uint8:
		zm.min = encoding.DecodeUint8(buf[:1])
		buf = buf[1:]
		zm.max = encoding.DecodeUint8(buf[:1])
		return nil
	case types.T_uint16:
		zm.min = encoding.DecodeUint16(buf[:2])
		buf = buf[2:]
		zm.max = encoding.DecodeUint16(buf[:2])
		return nil
	case types.T_uint32:
		zm.min = encoding.DecodeUint32(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeUint32(buf[:4])
		return nil
	case types.T_uint64:
		zm.min = encoding.DecodeUint64(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeUint64(buf[:8])
		return nil
	case types.T_float32:
		zm.min = encoding.DecodeFloat32(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeFloat32(buf[:4])
		return nil
	case types.T_float64:
		zm.min = encoding.DecodeFloat64(buf[:8])
		buf = buf[4:]
		zm.max = encoding.DecodeFloat64(buf[:8])
		return nil
	case types.T_date:
		zm.min = encoding.DecodeDate(buf[:4])
		buf = buf[4:]
		zm.max = encoding.DecodeDate(buf[:4])
		buf = buf[4:]
		return nil
	case types.T_datetime:
		zm.min = encoding.DecodeDatetime(buf[:8])
		buf = buf[8:]
		zm.max = encoding.DecodeDatetime(buf[:8])
		buf = buf[8:]
		return nil
	case types.T_char, types.T_varchar, types.T_json:
		lenminv := encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		minBuf := make([]byte, int(lenminv))
		copy(minBuf, buf[:int(lenminv)])
		buf = buf[int(lenminv):]

		lenmaxv := encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		maxBuf := make([]byte, int(lenmaxv))
		copy(maxBuf, buf[:int(lenmaxv)])
		zm.min = minBuf
		zm.max = maxBuf
		return nil
	}
	return nil
}

func (zm *ZoneMap) GetMemoryUsage() uint64 {
	buf, err := zm.Marshal()
	if err != nil {
		panic(err)
	}
	return uint64(len(buf))
}
