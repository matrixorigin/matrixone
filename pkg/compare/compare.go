// Copyright 2021 Matrix Origin
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

package compare

import (
	adates "github.com/matrixorigin/matrixone/pkg/compare/asc/dates"
	adatetimes "github.com/matrixorigin/matrixone/pkg/compare/asc/datetimes"
	afloat32s "github.com/matrixorigin/matrixone/pkg/compare/asc/float32s"
	afloat64s "github.com/matrixorigin/matrixone/pkg/compare/asc/float64s"
	aint16s "github.com/matrixorigin/matrixone/pkg/compare/asc/int16s"
	aint32s "github.com/matrixorigin/matrixone/pkg/compare/asc/int32s"
	aint64s "github.com/matrixorigin/matrixone/pkg/compare/asc/int64s"
	aint8s "github.com/matrixorigin/matrixone/pkg/compare/asc/int8s"
	auint16s "github.com/matrixorigin/matrixone/pkg/compare/asc/uint16s"
	auint32s "github.com/matrixorigin/matrixone/pkg/compare/asc/uint32s"
	auint64s "github.com/matrixorigin/matrixone/pkg/compare/asc/uint64s"
	auint8s "github.com/matrixorigin/matrixone/pkg/compare/asc/uint8s"
	avarchar "github.com/matrixorigin/matrixone/pkg/compare/asc/varchar"
	ddates "github.com/matrixorigin/matrixone/pkg/compare/desc/dates"
	ddatetimes "github.com/matrixorigin/matrixone/pkg/compare/desc/datetimes"
	dfloat32s "github.com/matrixorigin/matrixone/pkg/compare/desc/float32s"
	dfloat64s "github.com/matrixorigin/matrixone/pkg/compare/desc/float64s"
	dint16s "github.com/matrixorigin/matrixone/pkg/compare/desc/int16s"
	dint32s "github.com/matrixorigin/matrixone/pkg/compare/desc/int32s"
	dint64s "github.com/matrixorigin/matrixone/pkg/compare/desc/int64s"
	dint8s "github.com/matrixorigin/matrixone/pkg/compare/desc/int8s"
	duint16s "github.com/matrixorigin/matrixone/pkg/compare/desc/uint16s"
	duint32s "github.com/matrixorigin/matrixone/pkg/compare/desc/uint32s"
	duint64s "github.com/matrixorigin/matrixone/pkg/compare/desc/uint64s"
	duint8s "github.com/matrixorigin/matrixone/pkg/compare/desc/uint8s"
	dvarchar "github.com/matrixorigin/matrixone/pkg/compare/desc/varchar"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	case types.T_char, types.T_json, types.T_varchar:
		if desc {
			return dvarchar.New()
		}
		return avarchar.New()
	case types.T_date:
		if desc {
			return ddates.New()
		}
		return adates.New()
	case types.T_datetime:
		if desc {
			return ddatetimes.New()
		}
		return adatetimes.New()
	}
	return nil
}
