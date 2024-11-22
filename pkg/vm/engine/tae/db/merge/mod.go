// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var StopMerge atomic.Bool

type taskHostKind int

const (
	taskHostCN taskHostKind = iota
	taskHostDN
)

func CleanUpUselessFiles(entry *api.MergeCommitEntry, fs fileservice.FileService) {
	if entry == nil {
		return
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*time.Minute, moerr.CauseCleanUpUselessFiles)
	defer cancel()
	for _, filepath := range entry.BookingLoc {
		_ = fs.Delete(ctx, filepath)
	}
	if len(entry.CreatedObjs) != 0 {
		for _, obj := range entry.CreatedObjs {
			if len(obj) == 0 {
				continue
			}
			s := objectio.ObjectStats(obj)
			_ = fs.Delete(ctx, s.ObjectName().String())
		}
	}
}

const (
	constMaxMemCap     = 12 * common.Const1GBytes // max original memory for an object
	constSmallMergeGap = 3 * time.Minute
)

type policy interface {
	onObject(*catalog.ObjectEntry, *BasicPolicyConfig) bool
	revise(*resourceController, *BasicPolicyConfig) []reviseResult
	resetForTable(*catalog.TableEntry, *BasicPolicyConfig)
}

func NewUpdatePolicyReq(c *BasicPolicyConfig) *api.AlterTableReq {
	return &api.AlterTableReq{
		Kind: api.AlterKind_UpdatePolicy,
		Operation: &api.AlterTableReq_UpdatePolicy{
			UpdatePolicy: &api.AlterTablePolicy{
				MinOsizeQuailifed: c.ObjectMinOsize,
				MaxObjOnerun:      uint32(c.MergeMaxOneRun),
				MaxOsizeMergedObj: c.MaxOsizeMergedObj,
				MinCnMergeSize:    c.MinCNMergeSize,
				Hints:             c.MergeHints,
			},
		},
	}
}
