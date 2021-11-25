// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashtable

import (
	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.X86.HasSSE42 {
		BytesHash = Crc32BytesHashAsm
		IntHash = Crc32Int64HashAsm
		IntBatchHash = Crc32Int64BatchHashAsm
		intCellBatchHash = Crc32Int64CellBatchHashAsm
	}

	if cpu.X86.HasAES {
		AltBytesHash = aesBytesHashAsm
	}
}
