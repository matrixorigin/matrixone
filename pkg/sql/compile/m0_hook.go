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

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// An absent carrier permits consideration of ordinary metadata aggregates;
// it does not authorize missing columns. The live relation is checked before
// optimization and each referenced physical column is checked before use.
func metadataAggregateAllowed(node *plan.Node) bool {
	if !activeM0DisableCountNotNullRewrite {
		return true
	}
	if node == nil || node.TableDef == nil {
		return false
	}
	contract, err := m0read.PlanEvolution(node.TableDef)
	return err == nil && contract == nil
}

// In an M0 build a starcount with a column argument could have come from a
// non-null rewrite. Only a non-null literal can skip evaluating that column.
func metadataStarCountAllowed(fn *plan.Function) bool {
	if !activeM0DisableCountNotNullRewrite {
		return true
	}
	if fn == nil || len(fn.Args) != 1 || fn.Args[0] == nil {
		return false
	}
	lit := fn.Args[0].GetLit()
	return lit != nil && !lit.Isnull && lit.Value != nil
}

// Ranges has checked the live relation. A stale plan must not conceal its
// evolution carrier: leave that relation to the strict reader instead.
func metadataAggregateRelationAllowed(rel engine.Relation) bool {
	if !activeM0DisableCountNotNullRewrite {
		return true
	}
	if rel == nil {
		return false
	}
	extra := rel.GetExtraInfo()
	return extra == nil || extra.M0Evolution == nil
}

var (
	activeM0CaptureStatementView       = func(*Compile, *plan.Plan) (m0read.StatementReadView, error) { return nil, nil }
	activeM0CloneCreateContext         = func(c *Compile, _ *plan.CloneTable) (context.Context, error) { return c.proc.Ctx, nil }
	activeM0Enabled                    bool
	activeM0DisableCountNotNullRewrite bool
	activeM0ValidateRemoteReadBinding  = func(context.Context, *Source, *plan.TableDef, timestamp.Timestamp, plan2.LegacyTinyTextTableResolver) error {
		return nil
	}
	activeM0CaptureRemoteReadGeneration = func(client.TxnOperator) (func() error, error) { return func() error { return nil }, nil }
	activeM0WaitRemoteReadSnapshot      = func(context.Context, client.TxnClient, timestamp.Timestamp) error { return nil }
)
