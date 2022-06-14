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

package base

import "time"

type Observer interface {
	OnExecDone(any)
}

type IOpInternal interface {
	PreExecute() error
	Execute() error
	PostExecute() error
}

type IOp interface {
	OnExec() error
	SetError(err error)
	GetError() error
	WaitDone() error
	Waitable() bool
	GetCreateTime() time.Time
	GetStartTime() time.Time
	GetEndTime() time.Time
	GetExecutTime() int64
	AddObserver(Observer)
}
