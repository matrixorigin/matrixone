package compare

import (
	abools "matrixbase/pkg/compare/asc/bools"
	abytes "matrixbase/pkg/compare/asc/bytes"
	afloats "matrixbase/pkg/compare/asc/floats"
	aints "matrixbase/pkg/compare/asc/ints"
	dbools "matrixbase/pkg/compare/desc/bools"
	dbytes "matrixbase/pkg/compare/desc/bytes"
	dfloats "matrixbase/pkg/compare/desc/floats"
	dints "matrixbase/pkg/compare/desc/ints"
	"matrixbase/pkg/container/types"
)

func New(typ types.T, desc bool) Compare {
	switch typ {
	case types.T_int:
		if desc {
			return dints.New()
		}
		return aints.New()
	case types.T_bool:
		if desc {
			return dbools.New()
		}
		return abools.New()
	case types.T_float:
		if desc {
			return dfloats.New()
		}
		return afloats.New()
	case types.T_bytes, types.T_json:
		if desc {
			return dbytes.New()
		}
		return abytes.New()
	}
	return nil
}
