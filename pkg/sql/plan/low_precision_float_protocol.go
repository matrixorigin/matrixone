// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// LowPrecisionFloatProtocolVersion is the MORPC version that carries
// bf16/float16/float8/float4/vecf8/vecf4 and VECTOR_MATMUL, VECBLOCK_JSON,
// VECBLOCK_BINARY.
const LowPrecisionFloatProtocolVersion = defines.MORPCVersion117

func init() {
	plan.SetLowPrecisionFloatContract(plan.LowPrecisionFloatContract{
		IsType:     isLowPrecisionFloatContractType,
		IsFunction: isLowPrecisionFloatContractFunction,
	})
}

func isLowPrecisionFloatContractType(typeID int32) bool {
	t := types.T(typeID)
	return t.IsLowPrecisionFloat() || t.IsBlockScaledArray()
}

func isLowPrecisionFloatContractFunction(obj int64) bool {
	fid, _ := function.DecodeOverloadID(int64(uint64(obj) &^ function.Distinct))
	switch fid {
	case function.VECTOR_MATMUL, function.VECBLOCK_JSON, function.VECBLOCK_BINARY:
		return true
	}
	return false
}
