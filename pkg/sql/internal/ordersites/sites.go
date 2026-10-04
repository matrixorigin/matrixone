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

// Package ordersites is the append-only allocation-site catalog shared by
// Order operators. Sites are interpreted within AllocationOwnerOrder.
package ordersites

import "github.com/matrixorigin/matrixone/pkg/common/mpool"

const (
	// Sites 32-43 and 60 are reserved by colexec/spillutil.
	MergeOrderRetainedData mpool.AllocationSite = iota + 64
	MergeOrderRetainedArea
	MergeOrderRetainedNulls
	MergeOrderRetainedGrouping
	MergeOrderExpressionData
	MergeOrderExpressionArea
	MergeOrderExpressionNulls
	MergeOrderExpressionGrouping
	MergeOrderOutputData
	MergeOrderOutputArea
	MergeOrderOutputNulls
	MergeOrderOutputGrouping
	MergeOrderSpillWriteBuffer
)

const (
	OrderRetainedData mpool.AllocationSite = iota + 80
	OrderRetainedArea
	OrderRetainedNulls
	OrderRetainedGrouping
	OrderExpressionData
	OrderExpressionArea
	OrderExpressionNulls
	OrderExpressionGrouping
	OrderSelections
	OrderSortPartitions
	OrderSortDiffs
	OrderAppendCheckpoints
)
