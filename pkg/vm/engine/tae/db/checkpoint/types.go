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

package checkpoint

import "github.com/matrixorigin/matrixone/pkg/container/types"

type State int8

const (
	ST_Running State = iota
	ST_Finished
)

type Runner interface {
	Start()
	Stop()
	EnqueueWait(any) error
}

type Observer interface {
	OnNewCheckpoint(ts types.TS)
}

type observers struct {
	os []Observer
}

func (os *observers) add(o Observer) {
	os.os = append(os.os, o)
}

func (os *observers) OnNewCheckpoint(ts types.TS) {
	for _, o := range os.os {
		o.OnNewCheckpoint(ts)
	}
}
