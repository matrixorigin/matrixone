package adaptor

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"

func MakeVector(typ types.Type, opts ...*Options) (vec Vector) {
	switch typ.Oid {
	case types.Type_BOOL:
		vec = NewVector[bool](typ, opts...)
	case types.Type_INT8:
		vec = NewVector[int8](typ, opts...)
	case types.Type_INT16:
		vec = NewVector[int16](typ, opts...)
	case types.Type_INT32:
		vec = NewVector[int32](typ, opts...)
	case types.Type_INT64:
		vec = NewVector[int64](typ, opts...)
	case types.Type_UINT8:
		vec = NewVector[uint8](typ, opts...)
	case types.Type_UINT16:
		vec = NewVector[uint16](typ, opts...)
	case types.Type_UINT32:
		vec = NewVector[uint32](typ, opts...)
	case types.Type_UINT64:
		vec = NewVector[uint64](typ, opts...)
	case types.Type_DECIMAL64:
		vec = NewVector[types.Decimal64](typ, opts...)
	case types.Type_DECIMAL128:
		vec = NewVector[types.Decimal128](typ, opts...)
	case types.Type_FLOAT32:
		vec = NewVector[float32](typ, opts...)
	case types.Type_FLOAT64:
		vec = NewVector[float64](typ, opts...)
	case types.Type_DATE:
		vec = NewVector[types.Date](typ, opts...)
	case types.Type_TIMESTAMP:
		vec = NewVector[types.Timestamp](typ, opts...)
	case types.Type_DATETIME:
		vec = NewVector[types.Datetime](typ, opts...)
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		vec = NewVector[[]byte](typ, opts...)
	default:
		panic("not support")
	}
	return
}

func BuildBatch(attrs []types.Attr, capacity int) *Batch {
	opts := new(Options)
	opts.Capacity = capacity
	bat := NewBatch()
	for _, attr := range attrs {
		vec := MakeVector(attr.GetType(), opts)
		bat.AddVector(attr, vec)
	}
	return bat
}
