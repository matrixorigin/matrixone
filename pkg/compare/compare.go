package compare

import (
	afloat32s "matrixone/pkg/compare/asc/float32s"
	afloat64s "matrixone/pkg/compare/asc/float64s"
	aint16s "matrixone/pkg/compare/asc/int16s"
	aint32s "matrixone/pkg/compare/asc/int32s"
	aint64s "matrixone/pkg/compare/asc/int64s"
	aint8s "matrixone/pkg/compare/asc/int8s"
	auint16s "matrixone/pkg/compare/asc/uint16s"
	auint32s "matrixone/pkg/compare/asc/uint32s"
	auint64s "matrixone/pkg/compare/asc/uint64s"
	auint8s "matrixone/pkg/compare/asc/uint8s"
	avarchar "matrixone/pkg/compare/asc/varchar"
	dfloat32s "matrixone/pkg/compare/desc/float32s"
	dfloat64s "matrixone/pkg/compare/desc/float64s"
	dint16s "matrixone/pkg/compare/desc/int16s"
	dint32s "matrixone/pkg/compare/desc/int32s"
	dint64s "matrixone/pkg/compare/desc/int64s"
	dint8s "matrixone/pkg/compare/desc/int8s"
	duint16s "matrixone/pkg/compare/desc/uint16s"
	duint32s "matrixone/pkg/compare/desc/uint32s"
	duint64s "matrixone/pkg/compare/desc/uint64s"
	duint8s "matrixone/pkg/compare/desc/uint8s"
	dvarchar "matrixone/pkg/compare/desc/varchar"
	"matrixone/pkg/container/types"
)

func New(typ types.T, desc bool) Compare {
	switch typ {
	case types.T_int8:
		if desc {
			return dint8s.New()
		}
		return aint8s.New()
	case types.T_int16:
		if desc {
			return dint16s.New()
		}
		return aint16s.New()
	case types.T_int32:
		if desc {
			return dint32s.New()
		}
		return aint32s.New()
	case types.T_int64:
		if desc {
			return dint64s.New()
		}
		return aint64s.New()
	case types.T_uint8:
		if desc {
			return duint8s.New()
		}
		return auint8s.New()
	case types.T_uint16:
		if desc {
			return duint16s.New()
		}
		return auint16s.New()
	case types.T_uint32:
		if desc {
			return duint32s.New()
		}
		return auint32s.New()
	case types.T_uint64:
		if desc {
			return duint64s.New()
		}
		return auint64s.New()
	case types.T_float32:
		if desc {
			return dfloat32s.New()
		}
		return afloat32s.New()
	case types.T_float64:
		if desc {
			return dfloat64s.New()
		}
		return afloat64s.New()
	case types.T_varchar:
		if desc {
			return dvarchar.New()
		}
		return avarchar.New()
	}
	return nil
}
