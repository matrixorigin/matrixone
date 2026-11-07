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

package engine

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

const (
	MaxIndexPlanningNameBytes   = 256
	MaxIndexPlanningParts       = 16
	MaxIndexPlanningDefinitions = 64
	MaxIndexPlanningColumns     = 1024
	MaxIndexPlanningWork        = 65536
)

// IndexCostStats owns only copied scalar cost estimates, with no authority over
// schema, execution rows, permissions or the lifetime of the observed table.
type IndexCostStats struct {
	Rows, Min, Max, NDV float64
	Nulls               uint64
	Kind                types.T
	Blocks              int64
}

type IndexPairCheck struct {
	BaseRef, HiddenRef             *plan.ObjectRef
	BaseFull, BaseScan, HiddenFull *plan.TableDef
	Index                          *plan.IndexDef
	Column                         string
	Hidden                         Relation
	HiddenRead                     client.CatalogReadStamp
}

// IndexBlockInputRequest observes the original reader predicate, not the
// wider necessary index range. All inputs belong to one planning attempt.
type IndexBlockInputRequest struct {
	Column          string
	Observed        *statsinfo.StatsInfo
	Stats           IndexCostStats
	Bound           []byte
	BoundType       types.Type
	ComparisonScale int32
	Op              types.ComparisonOp
	Zone            *time.Location
}

type IndexBlockInput struct {
	Rows, Blocks uint64
}

// IndexPlanningRelation is optional metadata access on the actual relation
// already returned by ordinary Resolve. It opens no relation or subscription.
type IndexPlanningRelation interface {
	ReadIndexCostStats(context.Context, client.CatalogReadStamp, *statsinfo.StatsInfo, string) (IndexCostStats, bool, error)
	ObserveIndexBlockInput(context.Context, client.CatalogReadStamp, IndexBlockInputRequest, *int) (IndexBlockInput, bool, error)
	VerifyIndexPair(context.Context, client.CatalogReadStamp, IndexPairCheck, *int) (bool, error)
}
