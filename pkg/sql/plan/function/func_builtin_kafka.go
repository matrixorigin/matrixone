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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// builtInLastKafkaMessageID returns the offset of the last message a
// completed Kafka external-table scan returned in this session, or NULL when
// no scan has completed (including on a non-interactive session that has no
// Kafka state at all). The reader records the value in Close on a clean
// stream end; see reader_kafka.go.
func builtInLastKafkaMessageID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)
	var id int64
	var has bool
	if ses, ok := proc.GetSession().(process.KafkaSessionState); ok {
		id, has = ses.LastKafkaMessageID()
	}
	for i := 0; i < length; i++ {
		if err := rs.Append(id, !has); err != nil {
			return err
		}
	}
	return nil
}
