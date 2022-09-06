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

import "fmt"

// IsDelayTask returns true if the task is a delay task
func (m Task) IsDelayTask() bool {
	return m.ScheduleAfter > 0
}

// DebugString returns the debug string
func (m Task) DebugString() string {
	return fmt.Sprintf("%s/%d", m.Metadata.ID, m.Metadata.Executor)
}
