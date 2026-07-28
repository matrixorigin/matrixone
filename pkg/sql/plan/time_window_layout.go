// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// TimeWindowSlotNone marks an AggList entry that occupies no output slot.
const TimeWindowSlotNone = int32(-1)

// TimeWindowLayout describes the output batch a TIME_WINDOW operator produces.
//
// The operator emits, in this order:
//
//	[ aggregate results (AggList order) ..., _wstart?, _wend?, partition keys... ]
//
// Every `_wstart` entry in AggList maps to the single WStartSlot, and likewise
// for `_wend`, because the operator only carries one boolean per boundary
// rather than one column per reference.
//
// Partition keys go last so the aggregates stay a prefix: the fill operator
// addresses its input positionally as Vecs[0..ColLen).
//
// This is the single source of truth for that layout: the planner uses it to
// address slots from a TIME_WINDOW ProjectList, and the compiler uses it to
// build the operator that fills them. Deriving it in two places is what let
// the two drift apart and read each other's columns.
type TimeWindowLayout struct {
	// Slot maps an AggList index to its output position, or TimeWindowSlotNone
	// for entries that occupy none (every `_wstart`/`_wend` entry shares one
	// slot, so only the boundary slots below are authoritative for them).
	Slot []int32
	// AggIdx lists the AggList indices holding real aggregates, in slot order.
	AggIdx []int32
	// WStartSlot / WEndSlot are the boundary positions, or TimeWindowSlotNone
	// when the boundary is not produced.
	WStartSlot int32
	WEndSlot   int32
	// PartitionSlot maps a TimeWindowPartitionBy index to its output position.
	PartitionSlot []int32
	// ColCnt is the number of vectors in the output batch.
	ColCnt int32
}

// isTimeWindowBoundary reports whether an AggList entry is a `_wstart`/`_wend`
// carrier rather than an aggregate. It mirrors constructTimeWindow: any column
// reference is a carrier, and only the two known names produce a boundary.
func isTimeWindowBoundary(expr *plan.Expr) (isBoundary bool, name string) {
	e, ok := expr.Expr.(*plan.Expr_Col)
	if !ok {
		return false, ""
	}
	return true, e.Col.Name
}

// BuildTimeWindowLayout derives the output layout of the TIME_WINDOW operator
// for the given node.
func BuildTimeWindowLayout(node *plan.Node) TimeWindowLayout {
	layout := buildTimeWindowAggLayout(node.AggList)
	for range node.TimeWindowPartitionBy {
		layout.PartitionSlot = append(layout.PartitionSlot, layout.ColCnt)
		layout.ColCnt++
	}
	return layout
}

// buildTimeWindowAggLayout lays out the aggregate and boundary slots, which
// occupy the front of the batch regardless of partitioning.
func buildTimeWindowAggLayout(aggList []*plan.Expr) TimeWindowLayout {
	layout := TimeWindowLayout{
		Slot:       make([]int32, len(aggList)),
		WStartSlot: TimeWindowSlotNone,
		WEndSlot:   TimeWindowSlotNone,
	}

	hasWStart, hasWEnd := false, false
	for i, expr := range aggList {
		layout.Slot[i] = TimeWindowSlotNone
		isBoundary, name := isTimeWindowBoundary(expr)
		if !isBoundary {
			layout.Slot[i] = int32(len(layout.AggIdx))
			layout.AggIdx = append(layout.AggIdx, int32(i))
			continue
		}
		switch name {
		case TimeWindowStart:
			hasWStart = true
		case TimeWindowEnd:
			hasWEnd = true
		}
	}

	layout.ColCnt = int32(len(layout.AggIdx))
	if hasWStart {
		layout.WStartSlot = layout.ColCnt
		layout.ColCnt++
	}
	if hasWEnd {
		layout.WEndSlot = layout.ColCnt
		layout.ColCnt++
	}

	for i, expr := range aggList {
		isBoundary, name := isTimeWindowBoundary(expr)
		if !isBoundary {
			continue
		}
		switch name {
		case TimeWindowStart:
			layout.Slot[i] = layout.WStartSlot
		case TimeWindowEnd:
			layout.Slot[i] = layout.WEndSlot
		}
	}

	return layout
}
