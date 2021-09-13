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

package sched

type ResourceType uint16

const (
	ResT_Invalid ResourceType = iota
	ResT_IO
	ResT_CPU
	ResT_Meta
	ResT_Data
)

type BaseResource struct {
	EventHandler
	t    ResourceType
	name string
}

func NewBaseResource(name string, t ResourceType, handler EventHandler) *BaseResource {
	r := &BaseResource{
		t:            t,
		name:         name,
		EventHandler: handler,
	}
	return r
}

func (r *BaseResource) Type() ResourceType { return r.t }
func (r *BaseResource) Name() string       { return r.name }
func (r *BaseResource) Start() {
	if r.EventHandler != nil {
		r.EventHandler.Start()
	}
}
func (r *BaseResource) Close() error {
	if r.EventHandler != nil {
		r.EventHandler.Close()
	}
	return nil
}
