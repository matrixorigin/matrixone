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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ZeroRelevanceGuardExpr names the IndexSearchScan algorithm expression that
// carries the zero-relevance guard of a MATCH score threshold known only at
// execution.
const ZeroRelevanceGuardExpr = "zero_relevance_guard"

// BuildMembershipFilter converts the serialized primary-key vector of a
// unique-join-keys runtime filter into a docfilter membership payload.
func BuildMembershipFilter(proc *process.Process, vecbytes []byte) ([]byte, error) {
	if len(vecbytes) == 0 {
		return nil, nil
	}
	keyvec := new(vector.Vector)
	if err := keyvec.UnmarshalBinary(vecbytes); err != nil {
		return nil, err
	}
	// No keyvec.Free here on purpose: UnmarshalBinary aliases vecbytes (it sets
	// cantFreeData/cantFreeArea), so keyvec owns no mpool memory — the struct and
	// the aliased bytes are reclaimed by GC. Calling Free(mp) would be a no-op for
	// this zero-copy path, and tying its release to a specific mpool would be a
	// cross-pool free hazard if the deserialization ever became owning.

	// docfilter picks and tags the doc_id filter structure (exact set for integer
	// PKs, CBloomFilter otherwise); the reader reconstructs it at the allocation
	// site. The caller need not know which structure is used.
	return docfilter.BuildWithMemoryAdmission(
		keyvec,
		docfilter.AdmissionForService(proc.GetService()),
	)
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
