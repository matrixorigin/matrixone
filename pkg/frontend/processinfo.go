// Copyright 2021 Matrix Origin
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

import "time"

// ProcessInfo is a struct used for show processlist statement.
type ProcessInfo struct {
	ID        uint32
	AccountID uint32
	User      string
	Host      string
	DB        string
	Command   byte
	Time      time.Time
	State     uint16
	Info      string
}

// ProcessManager is an interface for show processlist.
// type ProcessManager interface {
// 	ShowProcessList() []ProcessInfo
// }
