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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// JSONInputRequester declares demand before a producer reads its next batch.
// Column ordinals describe that producer's actual output schema.
type JSONInputRequester interface {
	RequestJSONInputs([]int)
}

// JSONInputPublisher is optional at a producer's synchronous delivery boundary.
// The batch must stay stable while publishing. The consuming caller owns and
// closes all returned handles; the producer keeps no publication state.
type JSONInputPublisher interface {
	PublishJSONInputs(*process.Process, *batch.Batch, []int) (map[int]vector.JSONReader, error)
}

// PublishJSONInputSnapshots admits delivered mutable columns, including wire
// receive buffers with unknown aliases. It neither seals those buffers nor
// serializes qualification. A partial publication is released on error.
func PublishJSONInputSnapshots(proc *process.Process, input *batch.Batch, columns []int) (readers map[int]vector.JSONReader, err error) {
	return publishJSONInputSnapshots(proc, input, columns, false)
}

// PublishJSONInputSnapshotsForEvaluation defers only malformed-input admission
// failures to JSON_VALUE's ordinary evaluator, preserving its row and path
// short-circuits. A present nil value means no reader was published; it is not
// an authority to query the original bytes without full validation.
func PublishJSONInputSnapshotsForEvaluation(proc *process.Process, input *batch.Batch, columns []int) (map[int]vector.JSONReader, error) {
	return publishJSONInputSnapshots(proc, input, columns, true)
}

func publishJSONInputSnapshots(proc *process.Process, input *batch.Batch, columns []int, deferInvalid bool) (readers map[int]vector.JSONReader, err error) {
	if len(columns) == 0 || input == nil || input.IsEmpty() || input.Last() {
		return nil, nil
	}
	readers = make(map[int]vector.JSONReader, len(columns))
	published := readers
	defer func() {
		if err != nil {
			CloseJSONInputVersions(published)
			readers = nil
		}
	}()
	for _, column := range columns {
		if err = proc.Ctx.Err(); err != nil {
			return nil, err
		}
		if _, exists := readers[column]; exists {
			continue
		}
		if column < 0 || column >= len(input.Vecs) || input.Vecs[column] == nil || input.Vecs[column].GetType().Oid != types.T_json {
			return nil, moerr.NewInvalidInputNoCtx("JSON publication input column mismatch")
		}
		source := input.Vecs[column]
		var invalid bool
		readers[column], invalid, err = vector.TryNewJSONReaderSnapshotForRows(source, input.RowCount(), proc.Mp(), source.AllocationAccountSelection())
		if err != nil {
			if deferInvalid && invalid {
				err = nil
				continue
			}
			return nil, err
		}
	}
	return readers, nil
}

func CloseJSONInputVersions(readers map[int]vector.JSONReader) {
	for _, reader := range readers {
		if reader != nil {
			reader.Close()
		}
	}
}
