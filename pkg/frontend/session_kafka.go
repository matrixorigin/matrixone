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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Session implements process.KafkaSessionState: the Kafka external-table
// reader records the offset of the last message a completed scan returned,
// and LAST_KAFKA_MESSAGE_ID() reads it back so a consumer can chain reads
// exactly-once (last id -> next __mo_read_start_id). The value is plain
// session state: it does not survive a proxy connection migration (the
// builtin then returns NULL until the next scan).
var _ process.KafkaSessionState = (*Session)(nil)

func (ses *Session) SetLastKafkaMessageID(id int64) {
	ses.lastKafkaMessageMu.Lock()
	defer ses.lastKafkaMessageMu.Unlock()
	ses.lastKafkaMessageID = id
	ses.lastKafkaMessageSet = true
}

func (ses *Session) LastKafkaMessageID() (int64, bool) {
	ses.lastKafkaMessageMu.Lock()
	defer ses.lastKafkaMessageMu.Unlock()
	return ses.lastKafkaMessageID, ses.lastKafkaMessageSet
}

// EnqueueKafkaProgress queues a drained Kafka scan's progress finalizer for
// the statement terminal (see process.KafkaSessionState).
func (ses *Session) EnqueueKafkaProgress(finalize func(publish bool)) {
	ses.lastKafkaMessageMu.Lock()
	defer ses.lastKafkaMessageMu.Unlock()
	ses.kafkaProgressQueue = append(ses.kafkaProgressQueue, finalize)
}

// sessionFinalizeKafkaProgress finalizes deferred Kafka progress from a
// transaction terminal; sessions without the capability have nothing queued.
func sessionFinalizeKafkaProgress(execCtx *ExecCtx, publish bool) {
	if execCtx == nil || execCtx.ses == nil {
		return
	}
	if ses, ok := execCtx.ses.(*Session); ok {
		ses.FinalizeKafkaProgress(publish)
	}
}

// FinalizeKafkaProgress runs every queued Kafka progress finalizer with the
// statement's outcome and clears the queue. Called from the statement
// terminal (executeStmtWithResponse) with publish = statement succeeded, and
// from Session.Close with publish=false (an interrupted statement must not
// advance the exactly-once chain).
func (ses *Session) FinalizeKafkaProgress(publish bool) {
	ses.lastKafkaMessageMu.Lock()
	queue := ses.kafkaProgressQueue
	ses.kafkaProgressQueue = nil
	ses.lastKafkaMessageMu.Unlock()
	for _, finalize := range queue {
		finalize(publish)
	}
}
