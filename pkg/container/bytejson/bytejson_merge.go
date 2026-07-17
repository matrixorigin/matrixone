// Copyright 2026 Matrix Origin
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

package bytejson

const maxJSONMergeNestingDepth = 100

// MergePatch applies an RFC 7396 JSON merge patch to bj.
func (bj ByteJson) MergePatch(patch ByteJson) (ByteJson, error) {
	return mergeDocuments(NewMergePatchBuilder(), bj, patch)
}

// MergePreserve merges bj and other using MySQL JSON_MERGE_PRESERVE rules.
func (bj ByteJson) MergePreserve(other ByteJson) (ByteJson, error) {
	return mergeDocuments(NewMergePreserveBuilder(), bj, other)
}

func mergeDocuments(builder *MergeBuilder, first, second ByteJson) (ByteJson, error) {
	defer builder.Clear()
	if err := builder.BeginRow(); err != nil {
		return Null, err
	}
	if err := builder.Reset(first); err != nil {
		return Null, err
	}
	if err := builder.Merge(second); err != nil {
		return Null, err
	}
	return builder.BuildOwned()
}
