// Copyright 2025 Matrix Origin
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

package group

func (buf *GroupResultBuffer) Size() int64 {
	var size int64
	for _, b := range buf.ToPopped {
		size += int64(b.Allocated())
	}
	for _, agg := range buf.AggList {
		if agg != nil {
			size += agg.Size()
		}
	}
	return size
}
