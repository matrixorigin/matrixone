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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

const (
	MaxTemporalNameBytes        = 256
	MaxTemporalIndexParts       = 16
	MaxTemporalIndexDefinitions = 64
	MaxTemporalTableColumns     = 1024
	MaxTemporalBindingWork      = 65536
)

// TemporalRead borrows the current ordinary resolver's transaction domain.
// It is used synchronously by one root statement, never retained by a plan.
// A revision detects transitions; it does not permit concurrent workspace use.
type TemporalRead struct {
	Op         client.TxnOperator
	Stamp      client.CatalogReadStamp
	Visibility uint64
}

// TemporalStats owns only copied scalar cost estimates, with no authority over
// schema, execution rows, permissions or the lifetime of the observed table.
type TemporalStats struct {
	Rows, Min, Max, NDV float64
	Nulls               uint64
	Kind                types.T
	Blocks              int64
	Overlap             float64
	HasOverlap          bool
}

type TemporalPairCheck struct {
	BaseRef, HiddenRef             *plan.ObjectRef
	BaseFull, BaseScan, HiddenFull *plan.TableDef
	Index                          *plan.IndexDef
	Column                         string
	Hidden                         Relation
	HiddenRead                     TemporalRead
}

// TemporalIndexRelation is optional metadata access on the actual relation
// already returned by ordinary Resolve. It opens no relation or subscription.
type TemporalIndexRelation interface {
	ValidateTemporalRead(context.Context, TemporalRead) bool
	ReadTemporalCostStats(context.Context, TemporalRead, *statsinfo.StatsInfo, string) (TemporalStats, bool, error)
	VerifyTemporalIndexPair(context.Context, TemporalRead, TemporalPairCheck, *int) (bool, error)
}
