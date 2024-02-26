// Copyright 2024 Matrix Origin
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

package trace

import (
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

var (
	buffSize = 1024 * 32
)

func init() {
	reuse.CreatePool[EntryData](
		func() *EntryData { return &EntryData{} },
		func(l *EntryData) {
			l.reset()
		},
		reuse.DefaultOptions[EntryData]().
			WithEnableChecker())

	reuse.CreatePool[buffer](
		func() *buffer {
			return &buffer{buf: buf.NewByteBuf(buffSize, buf.WithDisableCompactAfterGrow(true))}
		},
		func(l *buffer) {
			l.reset()
		},
		reuse.DefaultOptions[buffer]().
			WithEnableChecker())
}
