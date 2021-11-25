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
	if cpu.ARM64.HasCRC32 {
		BytesHash = crc32BytesHashAsm
		IntHash = crc32Int64HashAsm
		IntBatchHash = crc32Int64BatchHashAsm
		intCellBatchHash = crc32Int64CellBatchHashAsm
	}

	if cpu.ARM64.HasAES {
		AltBytesHash = aesBytesHashAsm
	}
}
