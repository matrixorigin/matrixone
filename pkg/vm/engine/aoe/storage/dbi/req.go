// Copyright 2021 Matrix Origin
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

package dbi

import (
	"matrixone/pkg/container/batch"
)

type OnTableDroppedCB = func(error)

type TableOpCtx struct {
	OpIndex   uint64
	TableName string
}

type GetSnapshotCtx struct {
	OpIndex    uint64
	TableName  string
	SegmentIds []uint64
	ScanAll    bool
	Cols       []int
}

type DropTableCtx struct {
	OpIndex    uint64
	TableName  string
	OnFinishCB OnTableDroppedCB
}

type GetSegmentsCtx struct {
	OpIndex   uint64
	TableName string
}

type AppendCtx struct {
	OpIndex   uint64
	OpOffset  int
	OpSize    int
	TableName string
	Data      *batch.Batch
}

type MatchType uint8

const (
	MTPrefix MatchType = iota
	MTFull
	MTRegex
)

type StringMatcher struct {
	Type    MatchType
	Pattern string
}

type GetSegmentedIdCtx struct {
	Matchers []*StringMatcher
}

func NewTabletSegmentedIdCtx(tablet string) *GetSegmentedIdCtx {
	ctx := &GetSegmentedIdCtx{
		Matchers: make([]*StringMatcher, 1),
	}
	ctx.Matchers[0] = &StringMatcher{
		Pattern: tablet,
	}
	return ctx
}

type IDS struct {
	Version uint64
	Ids     []uint64
}
