// Copyright 2026 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
)

func logPKFilterInMemSummary(
	ctx context.Context,
	source string,
	filter *readutil.MemPKFilter,
	iterKind string,
	scan int,
	emitted int,
	skippedByBF int,
	skippedByDeletes int,
	outRows int,
) {
	label := engine.CollectChangesDebugLabelFromContext(ctx)
	if label == "" {
		return
	}

	var (
		filterValid bool
		filterOp    int
		keyCnt      int
		exact       bool
		exactHit    bool
	)
	if filter != nil {
		filterValid = filter.Valid()
		filterOp = filter.Op()
		keyCnt = len(filter.Keys())
		exact, exactHit = filter.Exact()
	}

	logutil.Info(
		"ChangesHandle-PKFilterInMemSummary",
		zap.String("debug-label", label),
		zap.String("source", source),
		zap.String("iter-kind", iterKind),
		zap.Bool("has-mem-pk-filter", filter != nil),
		zap.Bool("mem-pk-filter-valid", filterValid),
		zap.Int("filter-op", filterOp),
		zap.Int("key-count", keyCnt),
		zap.Bool("exact-filter", exact),
		zap.Bool("exact-hit", exactHit),
		zap.Int("scan", scan),
		zap.Int("emitted", emitted),
		zap.Int("skipped-by-bf", skippedByBF),
		zap.Int("skipped-by-deletes", skippedByDeletes),
		zap.Int("out-rows", outRows),
	)
}
