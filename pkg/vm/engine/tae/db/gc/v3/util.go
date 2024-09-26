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

package gc

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

func DedupSortedBatches(
	uniqueIdx int,
	batches []*batch.Batch,
) error {
	if len(batches) == 0 {
		return nil
	}
	var (
		curr, last []byte
	)
	var sels []int64
	for i := 0; i < len(batches); i++ {
		uniqueKey := batches[uniqueIdx].Vecs[uniqueIdx]
		for j := 0; j < uniqueKey.Length(); j++ {
			if i == 0 && j == 0 {
				last = uniqueKey.GetRawBytesAt(j)
			} else {
				curr = uniqueKey.GetRawBytesAt(j)
				if bytes.Compare(curr, last) == 0 {
					sels = append(sels, int64(j))
				} else {
					last = curr
				}
			}
		}
		if len(sels) > 0 {
			batches[i].Shrink(sels, true)
			sels = sels[:0]
		}
	}
	return nil
}
