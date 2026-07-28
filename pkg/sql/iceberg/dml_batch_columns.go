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

package iceberg

import (
	"context"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

func dmlReplacementColumnIndexes(
	ctx context.Context,
	bat *batch.Batch,
	attrs []string,
	fallback []int,
) ([]int, error) {
	if bat == nil {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg DML replacement batch is nil")
	}
	if len(bat.Attrs) == len(bat.Vecs) {
		positions := make(map[string]int, len(bat.Attrs))
		for idx, attr := range bat.Attrs {
			key := strings.ToLower(strings.TrimSpace(attr))
			if key == "" {
				continue
			}
			if _, exists := positions[key]; !exists {
				positions[key] = idx
			}
		}
		out := make([]int, len(attrs))
		matched := true
		for idx, attr := range attrs {
			pos, ok := positions[strings.ToLower(strings.TrimSpace(attr))]
			if !ok {
				matched = false
				break
			}
			out[idx] = pos
		}
		if matched {
			return out, nil
		}
	}
	if len(fallback) != len(attrs) {
		return nil, moerr.NewInvalidInputf(ctx,
			"Iceberg DML replacement fallback column count mismatch: attrs=%d columns=%d",
			len(attrs), len(fallback))
	}
	out := append([]int(nil), fallback...)
	for _, col := range out {
		if col < 0 || col >= len(bat.Vecs) {
			return nil, moerr.NewInvalidInputf(ctx,
				"Iceberg DML replacement column index out of range: column=%d columns=%d",
				col, len(bat.Vecs))
		}
	}
	return out, nil
}

func dmlBatchColumnIndexByNameOrError(
	ctx context.Context,
	bat *batch.Batch,
	name string,
	fallback int32,
	purpose string,
) (int, error) {
	idx := int(dmlBatchColumnIndexByName(bat, name, fallback))
	if bat == nil || idx < 0 || idx >= len(bat.Vecs) {
		return 0, moerr.NewInvalidInputf(ctx,
			"Iceberg DML %s column index is out of range: column=%s index=%s columns=%d",
			purpose, strings.TrimSpace(name), strconv.Itoa(idx), dmlBatchVectorCount(bat))
	}
	return idx, nil
}

func dmlBatchVectorCount(bat *batch.Batch) int {
	if bat == nil {
		return 0
	}
	return len(bat.Vecs)
}
