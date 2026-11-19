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

package fulltext

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ZeroRelevanceGuardExpr names the IndexSearchScan algorithm expression that
// carries the zero-relevance guard of a MATCH score threshold known only at
// execution.
const ZeroRelevanceGuardExpr = "zero_relevance_guard"

// BuildMembershipFilter converts the serialized primary-key vector of a
// unique-join-keys runtime filter into a docfilter membership payload.
func BuildMembershipFilter(proc *process.Process, keys *keycodec.KeyPayload) ([]byte, error) {
	if keys.Rows() == 0 {
		return nil, nil
	}
	return docfilter.BuildKeysWithMemoryAdmission(proc.Ctx, keys, docfilter.AdmissionForService(proc.GetService()))
}

// CheckZeroRelevanceGuard evaluates the folded zero-relevance guard of a MATCH
// score threshold known only at execution. True means a document with
// relevance 0 -- one the index never returns -- satisfies the predicate, which
// raises the error the planner raises for the literal threshold. A missing or
// NULL guard is not a violation.
func CheckZeroRelevanceGuard(ctx context.Context, guard *plan.Literal) error {
	if guard == nil || guard.Isnull {
		return nil
	}
	v, ok := guard.Value.(*plan.Literal_Bval)
	if !ok {
		return moerr.NewInvalidInput(ctx, "fulltext score-threshold guard must be bool")
	}
	if v.Bval {
		return moerr.NewNotSupported(ctx,
			"MATCH() AGAINST() function cannot be replaced by FULLTEXT INDEX and full table scan with fulltext search is not supported yet.")
	}
	return nil
}
