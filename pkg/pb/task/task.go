// Copyright 2022 Matrix Origin
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

package task

import (
	"fmt"
	"time"
)

type Task interface {
	GetID() uint64
	GetMetadata() TaskMetadata
}

// IsDelayTask returns true if the task is a delay task
func (m AsyncTask) IsDelayTask() bool {
	return m.Metadata.Options.DelayDuration > 0
}

// GetDelayDuration returns delay duration
func (m AsyncTask) GetDelayDuration() time.Duration {
	return time.Duration(m.Metadata.Options.DelayDuration)
}

// DebugString returns the debug string
func (m AsyncTask) DebugString() string {
	return fmt.Sprintf("%s/%d", m.Metadata.ID, m.Metadata.Executor)
}

// DebugString returns the debug string
func (m CronTask) DebugString() string {
	return fmt.Sprintf("%s/%d/%s",
		m.Metadata.ID,
		m.TriggerTimes,
		m.CronExpr)
}

// Type returns the task's type, or TypeUnknown when Details is absent or from
// a newer binary that this one does not understand.
func (t *Details) Type() TaskType {
	if t == nil {
		return TaskType_TypeUnknown
	}
	switch t.Details.(type) {
	case *Details_CreateCdc:
		return TaskType_CreateCdc
	case *Details_ISCP:
		return TaskType_ISCP
	case *Details_Publication:
		return TaskType_Publication
	default:
		return TaskType_TypeUnknown
	}
}

func (t *Details) Scan(src any) error {
	var data []byte
	if b, ok := src.([]byte); ok {
		data = b
	} else if s, ok := src.(string); ok {
		data = []byte(s)
	}
	return t.Unmarshal(data)
}
