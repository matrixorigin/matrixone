// Copyright 2021-2025 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func getArrayPrunePKFunc(
	relations []engine.Relation,
) prunePKFunc {
	return func(
		bat *batch.Batch,
		partitionIndex int32,
	) ([]engine.Relation, error) {
		return relations, nil
	}
}

func getArrayTableFunc(
	relations []engine.Relation,
) tablesFunc {
	return func() ([]engine.Relation, error) {
		return relations, nil
	}
}

func getArrayPruneTablesFunc(
	relations []engine.Relation,
) pruneFunc {
	return func(
		ctx context.Context,
		param engine.RangesParam,
	) ([]engine.Relation, error) {
		return relations, nil
	}
}
