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

package malloc

import "unsafe"

type Handle struct {
	ptr   unsafe.Pointer
	class int
}

var dumbHandle = &Handle{
	class: -1,
}

func (h *Handle) Free() {
	if h.class < 0 {
		return
	}
	pid := runtime_procPin()
	runtime_procUnpin()
	shard := pid % numShards
	select {
	case shards[shard].pools[h.class] <- h:
		shards[shard].numFree.Add(1)
	default:
	}
}
