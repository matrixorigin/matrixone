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
	"cmp"
	"context"
	"iter"
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type taskHostKind int

const (
	taskHostDN taskHostKind = iota
	taskHostCN

	constMaxMemCap = 12 * common.Const1GBytes // max original memory for an object
)

func removeOversize(objs []*catalog.ObjectEntry) []*catalog.ObjectEntry {
	if len(objs) < 2 {
		return objs
	}
	slices.SortFunc(objs, func(a, b *catalog.ObjectEntry) int {
		return cmp.Compare(a.OriginSize(), b.OriginSize())
	})

	accSize := int(objs[0].OriginSize()) + int(objs[1].OriginSize())
	i := 2
	for i < len(objs) {
		size := int(objs[i].OriginSize())
		if size > accSize {
			break
		}
		accSize += size
		i++
	}
	for j := i; j < len(objs); j++ {
		objs[j] = nil
	}
	if i == 2 {
		if objs[1].OriginSize() < 3*objs[0].OriginSize() || len(objs) > 20 /* do not let the first 2 objects block more merging tasks */ {
			return objs[:2]
		}
		return nil
	}
	return objs[:i]
}

func resourceAvailable(
	estMem int64,
	rsc rscthrottler.RSCThrottler,
) bool {

	mem := rsc.Available()
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}
	return estMem <= 2*mem/3
}

func IterEntryAsStats(objs []*catalog.ObjectEntry) iter.Seq[*objectio.ObjectStats] {
	return func(yield func(*objectio.ObjectStats) bool) {
		for _, obj := range objs {
			if !yield(obj.GetObjectStats()) {
				return
			}
		}
	}
}

func IterStats(objs []*objectio.ObjectStats) iter.Seq[*objectio.ObjectStats] {
	return func(yield func(*objectio.ObjectStats) bool) {
		for _, obj := range objs {
			if !yield(obj) {
				return
			}
		}
	}
}

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
