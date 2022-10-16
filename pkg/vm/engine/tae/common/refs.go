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

package common

import (
	"sync/atomic"
)

// IRef is the general representation of the resources
// that should be managed with a reference count.
// Once the reference count reached 0, the OnZeroCB
// would be called.
type IRef interface {
	RefCount() int64
	Ref()
	Unref()
}

type OnZeroCB func()

type RefHelper struct {
	Refs     atomic.Int64
	OnZeroCB OnZeroCB
}

func (helper *RefHelper) RefCount() int64 {
	return helper.Refs.Load()
}

func (helper *RefHelper) Ref() {
	helper.Refs.Add(1)
}

func (helper *RefHelper) Unref() {
	v := helper.Refs.Add(-1)
	if v == 0 {
		if helper.OnZeroCB != nil {
			helper.OnZeroCB()
		}
	} else if v < 0 {
		panic("logic error")
	}
}
