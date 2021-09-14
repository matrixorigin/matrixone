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

type BlockType uint8

const (
	// TRANSIENT_BLK means blk is in memtable
	TRANSIENT_BLK BlockType = iota

	// PERSISTENT_BLK means that blk has created a .blk file
	// TOTO : don't create a .blk file, write it to a .seg file
	PERSISTENT_BLK

	// PERSISTENT_SORTED_BLK means that blk has been
	// written or & merged in the .seg(segment) file
	PERSISTENT_SORTED_BLK
)

func (bt BlockType) String() string {
	switch bt {
	case TRANSIENT_BLK:
		return "TBLK"
	case PERSISTENT_BLK:
		return "PBLK"
	case PERSISTENT_SORTED_BLK:
		return "PSBLK"
	}
	panic("unsupported")
}
