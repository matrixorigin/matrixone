// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databranchutils

import (
	"context"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// BranchDeleteTarget carries frontend's successful branch-only authorization
// into synchronous nested DROP. Table=="" denotes a database; its TableIDs are
// the complete ordinary-table set (possibly empty), not hidden/index tables.
// Compile must recheck identity, membership and active branch rows after its
// own full D/K admission, before any effect. No admission is granted by this
// receipt itself.
type BranchDeleteTarget struct {
	AccountID       uint32
	Database, Table string
	DatabaseID      uint64
	TableIDs        []uint64
	MembershipSQL   string
}

type branchDeleteKey struct{}
type branchDeleteScope struct {
	owner  client.TxnOperator
	target BranchDeleteTarget
	closed bool
}

func WithBranchDeleteTarget(ctx context.Context, owner client.TxnOperator, target BranchDeleteTarget) (context.Context, func()) {
	target.TableIDs = slices.Clone(target.TableIDs)
	slices.Sort(target.TableIDs)
	scope := &branchDeleteScope{owner: owner, target: target}
	return context.WithValue(ctx, branchDeleteKey{}, scope), func() { scope.closed = true }
}

func BranchDeleteTargetFromContext(ctx context.Context, owner client.TxnOperator) (*BranchDeleteTarget, error) {
	scope, ok := ctx.Value(branchDeleteKey{}).(*branchDeleteScope)
	if !ok {
		return nil, nil
	}
	if scope.closed || owner == nil || scope.owner != owner {
		return nil, moerr.NewInternalError(ctx, "expired DATA BRANCH DELETE owner")
	}
	result := scope.target
	result.TableIDs = slices.Clone(result.TableIDs)
	return &result, nil
}
