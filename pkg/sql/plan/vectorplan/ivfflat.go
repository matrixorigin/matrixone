// Copyright 2026 Matrix Origin
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

package vectorplan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// IVF-FLAT table-function metadata. Referenced from both
// pkg/sql/plan/ivfflat.go (table-function builder, still in plan) and
// pkg/vectorindex/ivfflat/plugin/plan/plan.go (the lifted ANN rewrite).
//
// Mirrors the IVFPQ pattern in this package (ivfpq.go).
const IVFFLATSearchFuncName = "ivf_search"

// IVFFLATSearchColDefs is the column shape of an ivf_search FUNCTION_SCAN:
// (pkid, score). pkid type gets rewritten at plan time to the parent
// table's actual PK type.
var IVFFLATSearchColDefs = []*plan.ColDef{
	{
		Name: "pkid",
		Typ: plan.Type{
			Id:          int32(types.T_any),
			NotNullable: false,
		},
	},
	{
		Name: "score",
		Typ: plan.Type{
			Id:          int32(types.T_float64),
			NotNullable: false,
			Width:       8,
		},
	},
}
