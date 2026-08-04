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

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ResultSink owns attempt-scoped result rows. DML RETURNING uses it to discard
// failed RC generations and to defer every client-visible byte until commit.
type ResultSink interface {
	BeginAttempt(context.Context, uint64, *process.Process) error
	Write(uint64, *batch.Batch, *perfcounter.CounterSet) error
	SealAttempt(uint64) error
	AbortAttempt(uint64, error) error
}

func (c *Compile) SetResultSink(sink ResultSink) {
	c.resultSink = sink
}

func (c *Compile) resultWriter() func(*batch.Batch, *perfcounter.CounterSet) error {
	// The sink is installed by the frontend after Compile, but the generation
	// belongs to this compiled scope and must never follow the mutable parent
	// Compile across retries. A late old-scope callback therefore keeps its old
	// generation and is rejected by the attempt-owned sink.
	generation := c.executionGeneration
	return func(bat *batch.Batch, crs *perfcounter.CounterSet) error {
		if c.resultSink != nil {
			return c.resultSink.Write(generation, bat, crs)
		}
		return c.fill(bat, crs)
	}
}
