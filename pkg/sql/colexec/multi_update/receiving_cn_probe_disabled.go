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

//go:build !receiving_cn_acceptance

package multi_update

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// The ordinary build has no registration API, shared state, or instrumentation.
type receivingCNProbeTag struct{}

// ReceivingCNProbeEnabled is a build-time constant, so ordinary builds do not
// retain the acceptance probe's writer statistics or evaluate its arguments.
const ReceivingCNProbeEnabled = false

func markReceivingCNPartitionWriter(w *s3WriterDelegate)        { _ = w.receivingCNProbeTag }
func receivingCNPartitionCall(*process.Process, UpdateAction)   {}
func ObserveReceivingCNCompile(*process.Process, float64, bool) {}
func ReceivingCNForceMultiCN(*process.Process) bool             { return false }
func ObserveReceivingCNReceiver(*process.Process) func()        { return nil }
func ReceivingCNForcePostingShuffle(*process.Process, *plan.Node, *plan.Node, *plan.Node, *plan.Query) {
}
func receivingCNProgress(*process.Process, *s3WriterDelegate, int, []objectio.ObjectStats) error {
	return nil
}
func receivingCNProduced(*process.Process, *batch.Batch)     {}
func receivingCNApplied(*process.Process, *batch.Batch, int) {}
func receivingCNCleanup(*process.Process, int, int, int)     {}
