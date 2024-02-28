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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type Task interface {
	GetID() uint64
	GetMetadata() TaskMetadata
}

// IsDelayTask returns true if the task is a delay task
func (m AsyncTask) IsDelayTask() bool {
	return m.Metadata.Options.DelayDuration > 0
}

// IsInitTask returns true if the task is an init task
//func (m AsyncTask) IsInitTask() bool {
//	return m.Metadata.Executor == TaskCode_SystemInit
//}

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

// Type returns the task's type. It panics if the type is invalid.
func (t *Details) Type() TaskType {
	typ, err := detailsType(t.Details)
	if err != nil {
		panic(err)
	}
	return typ
}

func detailsType(d isDetails_Details) (TaskType, error) {
	switch d := d.(type) {
	case *Details_Connector:
		return TaskType_TypeKafkaSinkConnector, nil
	default:
		return TaskType_TypeUnknown, moerr.NewInternalErrorNoCtx("Unknown details type: %T", d)
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
