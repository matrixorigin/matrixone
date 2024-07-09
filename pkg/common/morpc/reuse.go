// Copyright 2021-2024 Matrix Origin
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

package morpc

import (
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

var (
	buffSize = 1024 * 64
)

func init() {
	reuse.CreatePool(
		func() *Buffer {
			return &Buffer{
				buf: buf.NewByteBuf(buffSize),
			}
		},
		func(l *Buffer) {
			l.reset()
		},
		reuse.DefaultOptions[Buffer]().
			WithReleaseFunc(
				func(l *Buffer) {
					l.buf.Close()
				}).
			WithEnableChecker(),
	)
}
